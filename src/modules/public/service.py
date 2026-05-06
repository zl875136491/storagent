from typing import List
from bson import ObjectId
from datetime import datetime, timedelta

from src.modules.auth.model import User
from src.modules.public.model import Region
from src.core import minio_op
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.core.exception import CustomException, ErrorDesc
from src.utils.helpers import (
  utc_now,
  generate_api_key
)
from src.modules.public.model import (
  APIKey,
  Region,
  Application
)
from src.core.redis_op import RedisOp

async def get_endpoints() -> dict[str, List[str]]:
  """
  获取端点列表
  """
  from src.modules.storage import service as storage_service
  minio_server_objs = await storage_service.get_minio_server_list()
  data = []
  for minio_server_obj in minio_server_objs["data"]:
    data.append({
      "region_id": minio_server_obj.region.id,
      "server_id": minio_server_obj.id,
      "name": minio_server_obj.region.name,
      "shown_name": minio_server_obj.region.shown_name,
      "master": minio_server_obj.master,
      "endpoint": f"http://{minio_server_obj.host}:{minio_server_obj.server_port}"
    })
  return dict[str, List[dict]](data=data)

async def _validate_application_name(name: str) -> None:
  """
  验证应用显示名称
  """

  if len(name) < 3 or len(name) > 32:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "应用别名长度不能小于3或大于32")
  # 符号仅允许连字符, 其他 deny
  for char in name:
    if char.isalnum() or char == "-":
      continue
    else:
      raise CustomException(ErrorDesc.INVALID_PARAMS, "应用别名只能包含连字符和字母数字")

async def create_region(
  name: str,
  shown_name: str) -> Region:
  """
  创建区域

  Args:
    name: 区域名称

  Returns:
    Region: 区域
  """
  return await public_crud.create_region(name, shown_name)

async def get_region_list() -> dict[str, List[Region]]:
  """
  获取区域列表

  Returns:
    List[Region]: 区域列表
  """
  region_objs = await public_crud.read_region_list()
  return dict[str, List[Region]](data=region_objs)

async def create_application(
  name: str,
  shown_name: str,
  description: str,
  current_user: User) -> Application:
  """
  创建应用
  """
  # 因为应用名用于存储桶创建, 所以需要一定的约束条件
  await _validate_application_name(name)
  # region_objs = await public_crud.read_many_region_by_ids(regions)
  # if len(region_objs) != len(regions):
  #   raise CustomException(ErrorDesc.RES_NOT_FOUND, "Region.id")
  existed_name = await public_crud.read_application_by_name(name)
  if existed_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Application.name")
  existed_shown_name = await public_crud.read_application_by_shown_name(shown_name)
  if existed_shown_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Application.shown_name")
  # 创建所有桶, 并且开启版本控制
  return await public_crud.create_application(name, shown_name, description, current_user)

async def get_application_list() -> dict[str, List[Application]]:
  """
  获取应用列表
  """
  application_objs = await public_crud.read_application_list()
  return dict[str, List[Application]](data=application_objs)

async def enable_application(
  application_id: ObjectId,
  current_user: User) -> Application:
  """
  启用应用
  """
  application_obj = await public_crud.read_application_by_id(application_id)
  if not application_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "Application.id")
  if application_obj.enabled:
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "Application.enabled")
  application_obj.enabled = True
  application_obj.enabled_at = utc_now()
  application_obj.approver = current_user
  # 批量创建 Minio 存储桶数据
  server_names = await storage_crud.read_minio_server_names()
  for server_name in server_names:
    success, err = await minio_op.create_bucket(server_name, application_obj.name)
    if not success:
      raise CustomException(ErrorDesc.MINIO_CREATE_BUCKET_FAILED, err)
  # 批量开启桶的版本控制
  for server_name in server_names:
    success, err = await minio_op.enable_bucket_versioning(server_name, application_obj.name)
    if not success:
      raise CustomException(ErrorDesc.MINIO_ENABLE_VERSIONING_FAILED, err)
  # await storage_crud.bulk_create_minio_bucket(application_obj)
  await application_obj.save()
  return dict[str, str](message="启用授权成功")

async def get_users_enabled_application_list(
  current_user: User) -> List[Application]:
  """
  获取用户启用的应用列表
  """
  app_objs = await public_crud.read_users_enabled_application_list(current_user)
  return dict[str, List[Application]](data=app_objs)
  
async def create_api_key(
  application_id: ObjectId,
  expired_at: datetime | None,
  current_user: User) -> APIKey:
  """
  创建API密钥
  """
  application_obj = await public_crud.read_application_by_id(application_id)
  if not application_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  if not application_obj.enabled:
    raise CustomException(ErrorDesc.STATUS_ERR, "应用未启用")
  if application_obj.author != current_user:
    raise CustomException(ErrorDesc.RES_NOT_BELONG_TO_USER, "应用不属于当前用户")
  if expired_at:
    expired_at = expired_at.replace(tzinfo=utc_now().tzinfo)
    if expired_at < utc_now():
      raise CustomException(ErrorDesc.INVALID_PARAMS, "过期时间不能小于当前时间")
  else:
    expired_at = utc_now() + timedelta(days=36500) # 100年, 设置一个特别大的时间, 视同为永久有效
    # 只精确到秒, 与用户设置时间保持一致
    expired_at = expired_at.replace(second=0, microsecond=0)
  key = generate_api_key()
  # 生成一个唯一的API密钥
  while await public_crud.read_api_key_by_key(key):
    key = generate_api_key()
  api_key_obj = await public_crud.create_api_key(application_obj, key, expired_at)
  # 将 API Key 数据同步到 Agent 中
  async with RedisOp() as redis_op:
    await redis_op.publish_api_key_create_patch(
      api_key=api_key_obj.key,
      app_name=application_obj.name,
      expired_at=expired_at,
    )
  return api_key_obj

async def get_api_key_list(
  current_user: User) -> List[APIKey]:
  """
  获取API密钥列表
  """
  users_app_objs = await public_crud.read_users_enabled_application_list(current_user)
  api_key_objs = await public_crud.read_api_key_by_app(users_app_objs)
  data = []
  for api_key_obj in api_key_objs:
    data.append({
      "id": api_key_obj.id,
      "key": f"{api_key_obj.key[:7]}************{api_key_obj.key[-4:]}",
      "application": {
        "id": api_key_obj.application.id,
        "name": api_key_obj.application.name,
        "shown_name": api_key_obj.application.shown_name
      },
      "expired_at": api_key_obj.expired_at
    })
  return dict[str, List[APIKey]](data=data)