from datetime import datetime
from typing import Literal

from fastapi import APIRouter, Depends, Query

from src.core.auth import require_admin
from src.modules.auth.model import User
from src.modules.usage import schema as usage_schema
from src.modules.usage import service as usage_service


router = APIRouter()


@router.get(
  path="/options",
  response_model=usage_schema.UsageOptionsResponse,
  summary="管理员：获取用量统计筛选项",
)
async def get_usage_options(
  current_user: User = Depends(require_admin),
) -> usage_schema.UsageOptionsResponse:
  return await usage_service.get_options()


@router.get(
  path="",
  response_model=usage_schema.UsageQueryResponse,
  summary="管理员：查询本区域 API 用量",
)
async def get_usage(
  start_at: datetime | None = Query(None),
  end_at: datetime | None = Query(None),
  interval: Literal["hour", "day"] = Query("hour"),
  app_name: str | None = Query(None, min_length=1, max_length=128),
  api_key_id: str | None = Query(None, min_length=1, max_length=128),
  current_user: User = Depends(require_admin),
) -> usage_schema.UsageQueryResponse:
  return await usage_service.query_usage(
    start_at=start_at,
    end_at=end_at,
    interval=interval,
    app_name=app_name,
    api_key_id=api_key_id,
  )
