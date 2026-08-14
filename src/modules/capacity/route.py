from fastapi import APIRouter, Depends

from src.core.auth import check_permissions, get_current_user
from src.modules.auth.model import User
from src.modules.capacity import schema, service

router = APIRouter()


@router.get("", response_model=schema.CapacityPlanningResponse, summary="获取区域容量规划视图")
async def get_capacity_planning(current_user: User = Depends(get_current_user)):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await service.get_planning()
