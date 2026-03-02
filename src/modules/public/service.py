from src.modules.public.model import Region


from typing import List
from src.modules.public.model import Region
from src.modules.public import crud as public_crud

async def create_region(name: str) -> Region:
  """
  创建区域

  Args:
    name: 区域名称

  Returns:
    Region: 区域
  """
  return await public_crud.create_region(name)

async def get_region_list() -> dict[str, List[Region]]:
  """
  获取区域列表

  Returns:
    List[Region]: 区域列表
  """
  region_objs = await public_crud.read_region_list()
  return dict[str, List[Region]](data=region_objs)
