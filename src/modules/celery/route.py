"""Read-only Celery operations endpoints."""
from fastapi import APIRouter, Depends, Query

from src.core.auth import check_permissions, get_current_user
from src.modules.auth.model import User
from src.modules.celery import schema, service

router = APIRouter()


@router.get(
  "/overview",
  response_model=schema.CeleryOverviewResponse,
  summary="获取 Celery worker、队列与实时执行状态",
)
async def get_celery_overview(current_user: User = Depends(get_current_user)):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await service.get_overview()


@router.get(
  "/history",
  response_model=schema.CeleryHistoryResponse,
  summary="获取 Celery 任务执行历史",
)
async def get_celery_history(
  limit: int = Query(50, ge=1, le=200),
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await service.get_history(limit=limit)
