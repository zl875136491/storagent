import json
from typing import Any

from src.modules.public.model import SystemConfig


AI_CONFIG_KEY = "ai.provider.openai"
DEFAULT_SYSTEM_PROMPT = """你是 Storage Agent 管理控制台内的操作助手。
只处理当前 Storage Agent 系统中的区域、应用、API Key、MinIO、存储桶、文件组件和文档相关任务。
对于闲聊、通用知识问答、外部网站操作、与本系统无关的编程或内容生成请求，简短说明职责范围并拒绝执行。
不要展示、猜测或索取任何密码、API Key、访问令牌或页面中已遮蔽的敏感值。
涉及创建、授权、吊销、删除、覆盖或复制关系变更时，必须先明确说明影响并让用户确认；未经确认不要执行。
仅使用页面中可见且属于当前系统的控件，遇到权限不足或状态不确定时停止并说明原因。"""


def default_config() -> dict[str, Any]:
  return {
    "provider_name": "OpenAI Compatible",
    "base_url": "http://10.32.129.1:8317/v1",
    "api_key_enc": "",
    "protocol": "chat_completions",
    "models": ["gpt-5.6-sol", "gpt-5.6-terra"],
    "default_model": "gpt-5.6-terra",
    "enabled": False,
    "system_prompt": DEFAULT_SYSTEM_PROMPT,
    "max_steps": 20,
    "updated_at": None,
    "updated_by": None,
  }


async def read_config() -> dict[str, Any]:
  config = default_config()
  item = await SystemConfig.find_one(SystemConfig.key == AI_CONFIG_KEY)
  if not item:
    return config
  try:
    stored = json.loads(item.value)
  except (json.JSONDecodeError, TypeError):
    return config
  if isinstance(stored, dict):
    config.update(stored)
  return config


async def upsert_config(config: dict[str, Any]) -> dict[str, Any]:
  value = json.dumps(config, ensure_ascii=False, separators=(",", ":"))
  item = await SystemConfig.find_one(SystemConfig.key == AI_CONFIG_KEY)
  if not item:
    item = SystemConfig(
      key=AI_CONFIG_KEY,
      value=value,
      name="AI 模型提供商",
      description="Storage Agent 页面助手的上游模型配置",
      value_type="json",
    )
  else:
    item.value = value
  await item.save()
  return config
