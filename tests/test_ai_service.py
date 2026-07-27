from types import SimpleNamespace

import pytest

from src.core.exception import CustomException, ErrorDesc
from src.modules.ai import schema as ai_schema
from src.modules.ai import service as ai_service


def _chat_body():
  return {
    "model": "client-selected-model",
    "messages": [
      {"role": "system", "content": "PageAgent system prompt"},
      {
        "role": "user",
        "content": (
          "<agent_state><user_request>Manage this page</user_request></agent_state>"
          "<browser_state>Storage Agent</browser_state>"
        ),
      },
    ],
    "tools": [{
      "type": "function",
      "function": {
        "name": "AgentOutput",
        "description": "Return an action",
        "parameters": {"type": "object", "properties": {}},
      },
    }],
    "tool_choice": {
      "type": "function",
      "function": {"name": "AgentOutput"},
    },
    "reasoning_effort": "low",
    "verbosity": "low",
  }


def test_prepare_chat_payload_enforces_server_model_and_guard_prompt():
  config = {
    "default_model": "gpt-5.6-terra",
    "system_prompt": "Only operate Storage Agent and refuse unrelated requests.",
  }
  prepared = ai_service._prepare_chat_payload(_chat_body(), config)
  assert prepared["model"] == "gpt-5.6-terra"
  assert prepared["stream"] is False
  system_content = prepared["messages"][0]["content"]
  assert system_content.startswith("PageAgent system prompt")
  assert ai_service.MANDATORY_SCOPE_PROMPT in system_content
  assert config["system_prompt"] in system_content


def test_proxy_rejects_generic_chat_payload():
  body = _chat_body()
  body["messages"] = [{"role": "user", "content": "Tell me a joke"}]
  with pytest.raises(CustomException) as exc_info:
    ai_service._validate_proxy_payload(body)
  assert exc_info.value.code == ErrorDesc.INVALID_PARAMS.code


def test_chat_to_responses_converts_named_tool_choice():
  body = _chat_body()
  body["model"] = "gpt-5.6-sol"
  converted = ai_service.chat_to_responses_payload(body)
  assert converted["model"] == "gpt-5.6-sol"
  assert converted["tool_choice"] == {"type": "function", "name": "AgentOutput"}
  assert converted["tools"][0]["name"] == "AgentOutput"
  assert converted["reasoning"] == {"effort": "low"}
  assert converted["text"] == {"verbosity": "low"}


def test_responses_to_chat_preserves_function_call_and_usage():
  converted = ai_service.responses_to_chat_payload({
    "id": "resp_123",
    "model": "gpt-5.6-sol",
    "output": [{
      "type": "function_call",
      "call_id": "call_123",
      "name": "AgentOutput",
      "arguments": '{"action":{"done":{"text":"ok"}}}',
    }],
    "usage": {
      "input_tokens": 10,
      "output_tokens": 5,
      "total_tokens": 15,
      "input_tokens_details": {"cached_tokens": 2},
      "output_tokens_details": {"reasoning_tokens": 3},
    },
  })
  choice = converted["choices"][0]
  assert choice["finish_reason"] == "tool_calls"
  assert choice["message"]["tool_calls"][0]["function"]["name"] == "AgentOutput"
  assert converted["usage"]["prompt_tokens"] == 10
  assert converted["usage"]["completion_tokens_details"]["reasoning_tokens"] == 3


def test_ai_config_requires_default_model_in_models():
  with pytest.raises(ValueError):
    ai_schema.AIProviderUpdateRequest(
      base_url="http://ai.example/v1",
      models=["gpt-a"],
      default_model="gpt-b",
      system_prompt="Only operate this system and reject unrelated tasks.",
    )


@pytest.mark.asyncio
async def test_enabling_without_api_key_is_rejected(monkeypatch):
  monkeypatch.setattr(
    "src.modules.ai.service.ai_crud.read_config",
    lambda: _async_value({"api_key_enc": ""}),
  )
  payload = ai_schema.AIProviderUpdateRequest(
    base_url="http://ai.example/v1",
    models=["gpt-a"],
    default_model="gpt-a",
    enabled=True,
    system_prompt="Only operate this system and reject unrelated tasks.",
  )
  with pytest.raises(CustomException) as exc_info:
    await ai_service.update_config(payload, SimpleNamespace(username="admin"))
  assert exc_info.value.code == ErrorDesc.AI_CONFIG_INVALID.code


@pytest.mark.asyncio
async def test_config_is_saved_locally_before_it_is_published(monkeypatch):
  calls = []
  current = {
    "provider_name": "Old provider",
    "base_url": "http://old.example/v1",
    "api_key_enc": "existing-key",
    "protocol": "chat_completions",
    "models": ["old-model"],
    "default_model": "old-model",
    "enabled": False,
    "system_prompt": "Only operate the Storage Agent system.",
    "max_steps": 20,
    "updated_at": None,
    "updated_by": None,
  }

  async def read_config():
    return current

  async def upsert_config(_config):
    calls.append("local")

  async def publish_config(_config):
    calls.append("publish")

  monkeypatch.setattr(ai_service.ai_crud, "read_config", read_config)
  monkeypatch.setattr(ai_service.ai_crud, "upsert_config", upsert_config)
  monkeypatch.setattr("src.core.sync.publish_ai_config", publish_config)
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  payload = ai_schema.AIProviderUpdateRequest(
    base_url="http://ai.example/v1",
    models=["gpt-a"],
    default_model="gpt-a",
    system_prompt="Only operate this system and reject unrelated tasks.",
  )
  await ai_service.update_config(payload, SimpleNamespace(username="admin"))
  assert calls == ["local", "publish"]


@pytest.mark.asyncio
async def test_config_update_encrypts_legacy_plaintext_api_key(monkeypatch):
  saved = []
  current = {
    "provider_name": "Old provider",
    "base_url": "http://old.example/v1",
    "api_key_enc": "legacy-plaintext-key",
    "protocol": "chat_completions",
    "models": ["old-model"],
    "default_model": "old-model",
    "enabled": False,
    "system_prompt": "Only operate the Storage Agent system.",
    "max_steps": 20,
    "updated_at": None,
    "updated_by": None,
  }

  async def read_config():
    return current

  async def save_config(config):
    saved.append(config)

  async def publish_config(_config):
    return None

  monkeypatch.setattr(ai_service.ai_crud, "read_config", read_config)
  monkeypatch.setattr(ai_service.ai_crud, "upsert_config", save_config)
  monkeypatch.setattr("src.core.sync.publish_ai_config", publish_config)
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  payload = ai_schema.AIProviderUpdateRequest(
    base_url="http://ai.example/v1",
    models=["gpt-a"],
    default_model="gpt-a",
    system_prompt="Only operate this system and reject unrelated tasks.",
  )
  await ai_service.update_config(payload, SimpleNamespace(username="admin"))

  assert saved[0]["api_key_enc"].startswith("enc:v1:")
  assert saved[0]["api_key_enc"] != current["api_key_enc"]


async def _async_value(value):
  return value
