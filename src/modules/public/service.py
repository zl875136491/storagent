from src.modules.public.model import Region, Application
from src.modules.auth.model import User
from bson import ObjectId
from typing import List
from src.modules.public.model import Region
from src.modules.public import crud as public_crud
from src.core.exception import CustomException, ErrorDesc

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
  description: str,
  regions: List[str | ObjectId],
  current_user: User) -> Application:
  """
  创建应用
  """
  region_objs = await public_crud.read_many_region_by_ids(regions)
  if len(region_objs) != len(regions):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "Region.id")
  return await public_crud.create_application(name, description, region_objs, current_user)

async def get_application_list() -> dict[str, List[Application]]:
  """
  获取应用列表
  """
  application_objs = await public_crud.read_application_list()
  return dict[str, List[Application]](data=application_objs)