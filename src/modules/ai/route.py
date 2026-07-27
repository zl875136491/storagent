from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from src.core.auth import get_current_user, require_admin
from src.core.rate_limit import rate_limit_ai
from src.modules.ai import schema as ai_schema
from src.modules.ai import service as ai_service
from src.modules.auth.model import User


router = APIRouter()


@router.get(
  path="/config",
  response_model=ai_schema.AIRuntimeConfigResponse,
  summary="获取 AI 助手运行配置",
)
async def get_runtime_config(
  current_user: User = Depends(get_current_user),
) -> ai_schema.AIRuntimeConfigResponse:
  return await ai_service.get_runtime_config()


@router.get(
  path="/admin/config",
  response_model=ai_schema.AIProviderAdminResponse,
  summary="获取 AI 模型提供商配置",
)
async def get_admin_config(
  current_user: User = Depends(get_current_user),
) -> ai_schema.AIProviderAdminResponse:
  await require_admin(current_user)
  return await ai_service.get_admin_config()


@router.put(
  path="/admin/config",
  response_model=ai_schema.AIProviderAdminResponse,
  summary="更新 AI 模型提供商配置",
)
async def update_admin_config(
  payload: ai_schema.AIProviderUpdateRequest,
  current_user: User = Depends(get_current_user),
) -> ai_schema.AIProviderAdminResponse:
  await require_admin(current_user)
  return await ai_service.update_config(payload, current_user)


@router.post(
  path="/admin/test",
  response_model=ai_schema.AIProviderTestResponse,
  summary="测试 AI 模型提供商连接",
)
async def test_admin_config(
  current_user: User = Depends(get_current_user),
) -> ai_schema.AIProviderTestResponse:
  await require_admin(current_user)
  return await ai_service.test_config()


@router.post(
  path="/openai/v1/chat/completions",
  response_model=None,
  summary="PageAgent OpenAI 兼容代理",
)
async def proxy_chat_completions(
  payload: dict[str, Any],
  request: Request,
  current_user: User = Depends(get_current_user),
) -> JSONResponse:
  rate_limit_ai(request, current_user.username)
  status_code, data = await ai_service.proxy_chat_completions(payload)
  return JSONResponse(status_code=status_code, content=data)
