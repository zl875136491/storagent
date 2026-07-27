from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator


AIProtocol = Literal["chat_completions", "responses"]


class AIProviderUpdateRequest(BaseModel):
  provider_name: str = Field(default="OpenAI Compatible", min_length=1, max_length=80)
  base_url: str = Field(..., min_length=8, max_length=500)
  api_key: str | None = Field(default=None, max_length=1000)
  clear_api_key: bool = False
  protocol: AIProtocol = "chat_completions"
  models: list[str] = Field(..., min_length=1, max_length=20)
  default_model: str = Field(..., min_length=1, max_length=160)
  enabled: bool = False
  system_prompt: str = Field(..., min_length=20, max_length=8000)
  max_steps: int = Field(default=20, ge=3, le=40)

  @field_validator("provider_name", "base_url", "default_model", "system_prompt")
  @classmethod
  def strip_text(cls, value: str) -> str:
    return value.strip()

  @field_validator("api_key")
  @classmethod
  def normalize_api_key(cls, value: str | None) -> str | None:
    if value is None:
      return None
    return value.strip() or None

  @field_validator("models")
  @classmethod
  def normalize_models(cls, value: list[str]) -> list[str]:
    models = list(dict.fromkeys(item.strip() for item in value if item.strip()))
    if not models:
      raise ValueError("至少配置一个模型")
    return models

  @model_validator(mode="after")
  def validate_default_model(self):
    if self.default_model not in self.models:
      raise ValueError("默认模型必须包含在模型列表中")
    if self.api_key and self.clear_api_key:
      raise ValueError("不能同时更新并清除 API Key")
    return self


class AIProviderAdminResponse(BaseModel):
  provider_name: str
  base_url: str
  api_key_configured: bool
  api_key_hint: str | None = None
  protocol: AIProtocol
  models: list[str]
  default_model: str
  enabled: bool
  system_prompt: str
  max_steps: int
  updated_at: str | None = None
  updated_by: str | None = None


class AIRuntimeConfigResponse(BaseModel):
  enabled: bool
  configured: bool
  provider_name: str
  protocol: AIProtocol
  model: str
  models: list[str]
  max_steps: int


class AIProviderTestResponse(BaseModel):
  ok: bool
  model: str
  protocol: AIProtocol
  latency_ms: int
