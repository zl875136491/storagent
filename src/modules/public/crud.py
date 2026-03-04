import uuid
from bson import ObjectId
from beanie import Document
from typing import List, Type
from datetime import datetime
from beanie.operators import Set, In

from src.modules.auth.model import User
from src.utils.helpers import try_to_obj_id, utc_now
from src.core.exception import CustomException, ErrorDesc
from src.modules.public.model import Region, Application, APIKey, APIKeyUsage

async def get_document_fields(model: Type[Document]) -> list[str]:
  """
  获取文档的字段
  
  Args:
    model: 文档类型

  Returns:
    list[str]: 文档的字段
  """
  fields = list[str](model.model_fields.keys())
  if "id" in fields:
    fields.remove("id")
  if "created_at" in fields:
    fields.remove("created_at")
  if "updated_at" in fields:
    fields.remove("updated_at")
  return fields

# async def name_exist_doc(obj: Document, doc: Type[Document], name: str) -> bool:
#   """
#   检查名称除当前对象外是否存在
#   """
#   return await doc.find_one(doc.name == name, doc.id != obj.id)

async def update_document(
  obj: Document,
  doc: Type[Document],
  alterations: dict = {}) -> Document:
  """
  更新文档
  
  Args:
    obj: 文档对象
    doc: 文档类型
    alterations: 修改的字段

  Returns:
    Document: 文档对象
  """
  null_keys = []
  doc_fields = await get_document_fields(doc)
  for key, value in alterations.items():
    if key not in doc_fields:
      raise CustomException(ErrorDesc.INVALID_PARAMS, f"字段 {key} 不存在")
    if value is None:
      null_keys.append(key)
  for key in null_keys:
    alterations.pop(key)
  # if "name" in alterations:
  #   if await name_exist_doc(obj, doc, alterations["name"]):
  #     raise CustomException(ErrorDesc.NAME_EXISTED, "名称已存在")
  try:
    await obj.update(Set(alterations))
  except Exception as e:
    raise CustomException(ErrorDesc.DB_UPDATE_FAILED, str(e))
  return obj

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
  existed_name = await read_region_by_name(name)
  if existed_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Region.name")
  existed_nickname = await read_region_by_nickname(nickname)
  if existed_nickname:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Region.nickname")
  region = Region(name=name, nickname=nickname)
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

async def read_region_by_nickname(nickname: str) -> Region | None:
  """
  获取区域

  Returns:
    Region | None: 区域
  """
  return await Region.find_one(Region.nickname == nickname)

async def read_many_region_by_ids(region_ids: List[str | ObjectId]) -> List[Region]:
  """
  获取多个区域
  """
  return await Region.find(In(Region.id, region_ids)).to_list()

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

async def read_application_by_name(name: str) -> Application | None:
  """
  获取应用

  Returns:
    Application | None: 应用
  """
  return await Application.find_one(Application.name == name)

async def read_application_by_nickname(nickname: str) -> Application | None:
  """
  获取应用
  """
  return await Application.find_one(Application.nickname == nickname)

async def read_users_enabled_application_list(current_user: User) -> List[Application]:
  """
  获取用户启用的应用列表
  """
  return await Application.find(
    Application.enabled == True,
    Application.author.id == current_user.id
  ).to_list()

async def create_application(
  name: str,
  nickname: str,
  description: str,
  regions: List[Region],
  author: User) -> Application:
  """
  创建应用

  Args:
    name: 应用名称
    description: 应用描述
    author: 作者

  Returns:
    Application: 应用
  """

  application = Application(
    name=name,
    nickname=nickname,
    description=description,
    regions=regions,
    author=author
  )
  await application.save()
  return application

async def read_application_list() -> List[Application]:
  """
  获取应用列表

  Returns:
    List[Application]: 应用列表
  """
  return await Application.find_all(fetch_links=True).to_list()

async def read_application_by_id(application_id: str | ObjectId) -> Application | None:
  """
  获取应用

  Returns:
    Application | None: 应用
  """
  return await Application.find_one(Application.id == application_id, fetch_links=True)

async def create_api_key(
  application: Application,
  key: str,
  expired_at: datetime) -> APIKey:
  """
  创建API密钥
  """
  api_key = APIKey(
    application=application,
    key=key,
    expired_at=expired_at
  )
  await api_key.save()
  return api_key

async def read_api_key_by_app(applications: List[Application]) -> List[APIKey]:
  """
  获取API密钥
  """
  application_ids = [app.id for app in applications]
  return await APIKey.find(
    In(APIKey.application.id, application_ids),
    APIKey.deleted == False
  ).to_list()

async def read_api_key_by_id(api_key_id: str | ObjectId) -> APIKey | None:
  """
  获取API密钥
  """
  api_key_id = try_to_obj_id(api_key_id)
  return await APIKey.find_one(APIKey.id == api_key_id)

async def read_api_key_by_key(key: str) -> APIKey | None:
  """
  获取API密钥
  """
  return await APIKey.find_one(APIKey.key == key)

async def delete_api_key_by_id(api_key_id: str | ObjectId) -> bool:
  """
  删除API密钥
  """
  api_key = await read_api_key_by_id(api_key_id)
  if not api_key:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "API密钥不存在")
  api_key.deleted = True
  api_key.deleted_at = utc_now()
  await api_key.save()
  return True