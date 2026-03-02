from fastapi import APIRouter
from src.modules.public import service as public_service
from src.modules.public import schema as public_schema

router = APIRouter()

@router.post(
  path="/region",
  response_model=public_schema.RegionResponse,
  summary="创建区域")
async def create_region(
  payload: public_schema.RegionCreateRequest) -> public_schema.RegionResponse:
  """
  创建区域

  Args:
    name: 区域名称

  Returns:
    RegionResponse: 区域
  """
  name = payload.name.strip()
  return await public_service.create_region(name)

@router.get(
  path="/region",
  response_model=public_schema.RegionListResponse,
  summary="获取区域列表")
async def get_region_list() -> public_schema.RegionListResponse:
  """
  获取区域列表

  Returns:
    RegionListResponse: 区域列表
  """
  return await public_service.get_region_list()