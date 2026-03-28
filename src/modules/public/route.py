from fastapi import APIRouter, Depends
from src.core.auth import get_current_user, check_permissions
from src.modules.auth.model import User
from src.modules.public import service as public_service
from src.modules.public import schema as public_schema

router = APIRouter()

@router.get(
  path="/endpoints",
  response_model=public_schema.EndpointsResponse,
  summary="获取端点列表")
async def get_endpoints() -> public_schema.EndpointsResponse:
  """
  获取端点列表
  """
  return await public_service.get_endpoints()
    
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
  shown_name = payload.shown_name.strip()
  return await public_service.create_region(name, shown_name)

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
  shown_name = payload.shown_name.strip().lower()
  description = payload.description.strip()
  return await public_service.create_application(
    name=name,
    shown_name=shown_name,
    description=description,
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

@router.get(
  path="/application/enabled",
  response_model=public_schema.SimpleApplicationListResponse,
  summary="用户启用的应用列表")
async def get_users_enabled_application_list(
  current_user: User = Depends(get_current_user)) -> public_schema.SimpleApplicationListResponse:
  """
  用户启用的应用列表
  """
  return await public_service.get_users_enabled_application_list(current_user)

@router.post(
  path="/api-key",
  response_model=public_schema.APIKeyResponse,
  summary="创建API密钥")
async def create_api_key(
  payload: public_schema.APIKeyCreateRequest,
  current_user: User = Depends(get_current_user)) -> public_schema.APIKeyResponse:
  """
  创建API密钥
  """
  application_id = payload.application_id
  expired_at = payload.expired_at
  return await public_service.create_api_key(
    application_id=application_id,
    expired_at=expired_at,
    current_user=current_user
  )

@router.get(
  path="/api-key",
  response_model=public_schema.APIKeyListResponse,
  summary="获取API密钥列表")
async def get_api_key_list(
  current_user: User = Depends(get_current_user)) -> public_schema.APIKeyListResponse:
  """
  获取API密钥列表
  """
  return await public_service.get_api_key_list(current_user)
