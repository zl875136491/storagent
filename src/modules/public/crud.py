from bson import ObjectId
from src.modules.public.model import Region
from typing import List
from src.core.exception import CustomException, ErrorDesc

async def create_region(name: str) -> Region:
  """
  创建区域

  Args:
    name: 区域名称

  Returns:
    Region: 区域
  """
  exist_name = await read_region_by_name(name)
  if exist_name:
    raise CustomException(ErrorDesc.NAME_EXISTED)
  region = Region(name=name)
  await region.save()
  return region

async def read_region_list() -> List[Region]:
  """
  获取区域列表

  Returns:
    List[Region]: 区域列表
  """
  return await Region.find_all().to_list()

async def read_region_by_id(region_id: str | ObjectId) -> Region | None:
  """
  获取区域
 
  Returns:
    Region | None: 区域
  """
  return await Region.find_one(Region.id == region_id)

async def read_region_by_name(name: str) -> Region | None:
  """
  获取区域

  Returns:
    Region | None: 区域
  """
  return await Region.find_one(Region.name == name)

async def delete_region_by_id(region_id: str | ObjectId) -> bool:
  """
  删除区域

  Args:
    region_id: 区域ID

  Returns:
    bool: 是否删除成功
  """
  region_obj = await read_region_by_id(region_id)
  if not region_obj:
    return False
  await region_obj.delete()
  return True