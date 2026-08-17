"""Etcd operations endpoints."""
from fastapi import APIRouter, Depends, Query

from src.core.auth import check_permissions, get_current_user
from src.modules.auth.model import User
from src.modules.etcd import schema, service

router = APIRouter()


@router.get(
  "/operations/etcd",
  response_model=schema.EtcdClusterStatusResponse,
  summary="获取 Etcd 控制面健康状态",
)
async def get_etcd_operations(
  refresh: bool = Query(False),
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await service.get_status(force_refresh=refresh)
