from fastapi import APIRouter, Depends
from src.core.auth import get_current_user, check_permissions
from src.modules.auth.model import User
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
  nickname = payload.nickname.strip()
  return await public_service.create_region(name, nickname)

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

@router.post(
  path="/application",
  response_model=public_schema.ApplicationResponse,
  summary="创建应用")
async def create_application(
  payload: public_schema.ApplicationCreateRequest,
  current_user: User = Depends(get_current_user)) -> public_schema.ApplicationResponse:
  """
  创建应用
  """
  name = payload.name.strip()
  nickname = payload.nickname.strip().lower()
  description = payload.description.strip()
  regions = payload.regions
  return await public_service.create_application(
    name=name,
    nickname=nickname,
    description=description,
    regions=regions,
    current_user=current_user
  )

@router.get(
  path="/application",
  response_model=public_schema.ApplicationListResponse,
  summary="获取应用列表")
async def get_application_list() -> public_schema.ApplicationListResponse:
  """
  获取应用列表

  Returns:
    ApplicationListResponse: 应用列表
  """
  return await public_service.get_application_list()

@router.post(
  path="/application/{application_id}/approval",
  response_model=public_schema.SimpleMessageResponse,
  summary="授权应用")
async def approval_application(
  application_id: public_schema.PydanticObjectId,
  current_user: User = Depends(get_current_user)) -> public_schema.SimpleMessageResponse:
  """
  授权应用
  """
  await check_permissions(current_user, ["application_manage"])
  return await public_service.enable_application(application_id, current_user)