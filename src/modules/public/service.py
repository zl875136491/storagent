from typing import List
from beanie import Link
from bson import ObjectId

from src.utils.helpers import utc_now
from src.modules.auth.model import User
from src.modules.public.model import Region
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.modules.public.model import Region, Application
from src.core.exception import CustomException, ErrorDesc
from src.core.minio_op import create_bucket

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
  await application_obj.save()
  return dict[str, str](message="启用授权成功")
  