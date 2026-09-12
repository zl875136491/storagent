"""Celery operations endpoints."""
from fastapi import APIRouter, Depends, Query, Request

from src.core.auth import check_permissions, get_current_user
from src.core.rate_limit import (
  rate_limit_celery_history,
  rate_limit_celery_overview,
  rate_limit_celery_run,
)
from src.modules.auth.model import User
from src.modules.celery import schema, service

router = APIRouter()


@router.get(
  "/overview",
  response_model=schema.CeleryOverviewResponse,
  summary="获取 Celery worker、队列与实时执行状态",
)
async def get_celery_overview(
  request: Request,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  rate_limit_celery_overview(request, current_user.username)
  return await service.get_overview()


@router.get(
  "/history",
  response_model=schema.CeleryHistoryResponse,
  summary="获取 Celery 任务执行历史",
)
async def get_celery_history(
  request: Request,
  limit: int = Query(50, ge=1, le=200),
  offset: int = Query(0, ge=0),
  failed_only: bool = Query(False),
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  rate_limit_celery_history(request, current_user.username)
  return await service.get_history(limit=limit, offset=offset, failed_only=failed_only)


@router.post(
  "/tasks/run",
  response_model=schema.CeleryTaskRunResponse,
  summary="手动发起已注册的白名单任务",
)
async def run_celery_task(
  request: Request,
  payload: schema.CeleryTaskRunRequest,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  rate_limit_celery_run(request, current_user.username)
  return await service.run_registered_task(payload.name, current_user.username)
