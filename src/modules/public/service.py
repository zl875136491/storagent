from typing import List
from bson import ObjectId
from datetime import datetime, timedelta

from src.modules.auth.model import User
from src.modules.public.model import Region
from src.core.minio_op import create_bucket
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

async def _validate_application_nickname(nickname: str) -> None:
  """
  验证应用别名
  """

  if len(nickname) < 3 or len(nickname) > 32:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "应用别名长度不能小于3或大于32")
  # 符号仅允许连字符, 其他 deny
  for char in nickname:
    if char.isalnum() or char == "-":
      continue
    else:
      raise CustomException(ErrorDesc.INVALID_PARAMS, "应用别名只能包含连字符和字母数字")

async def create_region(
  name: str,
  nickname: str) -> Region:
  """
  创建区域

  Args:
    name: 区域名称

  Returns:
    Region: 区域
  """
  return await public_crud.create_region(name, nickname)

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
  nickname: str,
  description: str,
  regions: List[str | ObjectId],
  current_user: User) -> Application:
  """
  创建应用
  """
  # 因为应用别名用于存储桶创建, 所以需要一定的约束条件
  await _validate_application_nickname(nickname)
  region_objs = await public_crud.read_many_region_by_ids(regions)
  if len(region_objs) != len(regions):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "Region.id")
  existed_name = await public_crud.read_application_by_name(name)
  if existed_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Application.name")
  existed_nickname = await public_crud.read_application_by_nickname(nickname)
  if existed_nickname:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Application.nickname")
  return await public_crud.create_application(name, nickname, description, region_objs, current_user)

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
  # 将应用别名相应的桶添加到 master minio server 中
  master_minio_server_obj = await storage_crud.read_master_minio_server()
  master_region : Region = master_minio_server_obj.region
  await create_bucket(master_region.nickname, application_obj.nickname)
  # 批量创建 Minio 存储桶数据
  await storage_crud.bulk_create_minio_bucket(application_obj)
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
    if expired_at.replace(tzinfo=utc_now().tzinfo) < utc_now():
      raise CustomException(ErrorDesc.INVALID_PARAMS, "过期时间不能小于当前时间")
  else:
    expired_at = utc_now() + timedelta(days=36500) # 100年, 设置一个特别大的时间, 视同为永久有效
  key = generate_api_key()
  # 生成一个唯一的API密钥
  while await public_crud.read_api_key_by_key(key):
    key = generate_api_key()
  api_key_obj = await public_crud.create_api_key(application_obj, key, expired_at)
  # 将 API Key 数据同步到 Agent 中
  async with RedisOp() as redis_op:
    await redis_op.publish_api_key(api_key_obj.key)
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
        "nickname": api_key_obj.application.nickname
      },
      "expired_at": api_key_obj.expired_at
    })
  return dict[str, List[APIKey]](data=data)