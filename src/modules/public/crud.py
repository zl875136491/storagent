import uuid
from bson import ObjectId
from beanie import Document
from typing import List, Type
from datetime import datetime
from beanie.operators import Set, In

from src.modules.auth.model import User
from src.utils.helpers import try_to_obj_id, utc_now
from src.core.exception import CustomException, ErrorDesc
from src.modules.public.model import Region, Application, APIKey, APIKeyUsage, SystemConfig, ShellCommandLog

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
  shown_name: str) -> Region:
  """
  创建区域

  Args:
    name: 区域名称
    shown_name: 区域显示名称

  Returns:
    Region: 区域
  """
  existed_name = await read_region_by_name(name)
  if existed_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Region.name")
  existed_shown_name = await read_region_by_shown_name(shown_name)
  if existed_shown_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Region.shown_name")
  region = Region(name=name, shown_name=shown_name)
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

async def read_region_by_shown_name(shown_name: str) -> Region | None:
  """
  获取区域

  Returns:
    Region | None: 区域
  """
  return await Region.find_one(Region.shown_name == shown_name)

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

async def read_application_by_shown_name(shown_name: str) -> Application | None:
  """
  获取应用
  """
  return await Application.find_one(Application.shown_name == shown_name)

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
  shown_name: str,
  description: str,
  author: User,
  domains: list[str] | None = None) -> Application:
  """
  创建应用

  Args:
    name: 应用名称
    shown_name: 应用显示名称
    description: 应用描述
    author: 作者
    domains: 浏览器来源白名单

  Returns:
    Application: 应用
  """

  application = Application(
    name=name,
    shown_name=shown_name,
    description=description,
    author=author,
    domains=list(domains or []),
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


async def delete_application_by_id(application_id: str | ObjectId) -> bool:
  """
  删除本地应用投影。跨节点权威数据由 Etcd applications map 收敛。
  """
  application = await read_application_by_id(application_id)
  if not application:
    return False
  await application.delete()
  return True

async def create_api_key(
  application: Application,
  key: str,
  expired_at: datetime) -> APIKey:
  """
  创建API密钥：Mongo 仅存哈希 + 提示 + 密文
  """
  from src.core.crypto import api_key_etcd_map_key, api_key_hint, encrypt_secret

  api_key = APIKey(
    application=application,
    key=api_key_etcd_map_key(key),
    key_hint=api_key_hint(key),
    key_enc=encrypt_secret(key),
    expired_at=expired_at
  )
  await api_key.save()
  return api_key

async def read_api_key_by_app(
  applications: List[Application],
  *,
  include_admin_destroyed: bool = False,
) -> List[APIKey]:
  """
  获取应用下的 API 密钥。
  include_admin_destroyed=True 时额外返回被管理员吊销的密钥（供所有者展示状态）。
  """
  application_ids = [app.id for app in applications]
  if not application_ids:
    return []
  if include_admin_destroyed:
    return await APIKey.find(
      In(APIKey.application.id, application_ids),
      {
        "$or": [
          {"deleted": False},
          {"destory_by_admin": True},
        ]
      },
      fetch_links=True,
    ).to_list()
  return await APIKey.find(
    In(APIKey.application.id, application_ids),
    APIKey.deleted == False,
    fetch_links=True
  ).to_list()


async def read_all_api_keys(*, include_admin_destroyed: bool = False) -> List[APIKey]:
  """管理员视角：全部有效密钥，可选含管理员吊销记录。"""
  if include_admin_destroyed:
    return await APIKey.find(
      {
        "$or": [
          {"deleted": False},
          {"destory_by_admin": True},
        ]
      },
      fetch_links=True,
    ).to_list()
  return await APIKey.find(APIKey.deleted == False, fetch_links=True).to_list()

async def read_api_key_by_id(api_key_id: str | ObjectId) -> APIKey | None:
  """
  获取API密钥
  """
  api_key_id = try_to_obj_id(api_key_id)
  return await APIKey.find_one(APIKey.id == api_key_id, fetch_links=True)

async def _migrate_legacy_api_key_plaintext(api_key_obj: APIKey, plain_key: str) -> APIKey:
  from src.core.crypto import api_key_etcd_map_key, api_key_hint, encrypt_secret, is_sha256_hex

  if is_sha256_hex(api_key_obj.key) and api_key_obj.key_enc:
    return api_key_obj
  api_key_obj.key = api_key_etcd_map_key(plain_key)
  api_key_obj.key_hint = api_key_obj.key_hint or api_key_hint(plain_key)
  api_key_obj.key_enc = encrypt_secret(plain_key)
  await api_key_obj.save()
  return api_key_obj

async def read_api_key_by_key(key: str) -> APIKey | None:
  """
  获取API密钥（不含已吊销）。按哈希查询，兼容历史明文行并就地迁移。
  """
  from src.core.crypto import api_key_etcd_map_key

  hashed = api_key_etcd_map_key(key)
  found = await APIKey.find_one(APIKey.key == hashed, APIKey.deleted == False, fetch_links=True)
  if found:
    return found
  legacy = await APIKey.find_one(APIKey.key == key, APIKey.deleted == False, fetch_links=True)
  if legacy:
    return await _migrate_legacy_api_key_plaintext(legacy, key)
  return None

async def read_api_key_by_hash(key_hash: str) -> APIKey | None:
  """
  按哈希直接查询有效 APIKey（不含已吊销）。

  用于 v1 数据面能力令牌校验：令牌只携带 APIKey 的 SHA256 摘要（与本表 `key`
  字段同一算法），Storagent 据此反查记录、解密出明文 Key 后再校验令牌签名，
  全程不需要、也不会接触到明文 x-api-key 之外的任何敏感信息。
  """
  return await APIKey.find_one(APIKey.key == key_hash, APIKey.deleted == False, fetch_links=True)


async def read_api_key_by_key_including_deleted(key: str) -> APIKey | None:
  """
  获取API密钥（含已吊销，用于跨节点同步）
  """
  from src.core.crypto import api_key_etcd_map_key, is_sha256_hex

  if is_sha256_hex(key):
    return await APIKey.find_one(APIKey.key == key, fetch_links=True)
  hashed = api_key_etcd_map_key(key)
  found = await APIKey.find_one(APIKey.key == hashed, fetch_links=True)
  if found:
    return found
  legacy = await APIKey.find_one(APIKey.key == key, fetch_links=True)
  if legacy:
    return await _migrate_legacy_api_key_plaintext(legacy, key)
  return None


async def delete_api_key_by_id(
  api_key_id: str | ObjectId,
  *,
  destory_by_admin: bool = False,
) -> bool:
  """
  删除API密钥（软删除）
  """
  api_key = await read_api_key_by_id(api_key_id)
  if not api_key:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "API密钥不存在")
  api_key.deleted = True
  api_key.deleted_at = utc_now()
  api_key.destory_by_admin = bool(destory_by_admin)
  await api_key.save()
  return True

async def create_system_config(
  key: str,
  value: str,
  name: str,
  description: str,
  value_type: str):
  """
  创建系统配置信息
  """
  value_str = str(value)
  system_config = SystemConfig(
    key=key,
    value=value_str,
    name=name,
    description=description,
    value_type=value_type
  )
  await system_config.save()
  return system_config

async def read_system_config_by_key(key: str) -> SystemConfig | None:
  """
  获取系统配置信息
  """
  return await SystemConfig.find_one(SystemConfig.key == key)

async def update_system_config_by_key(key: str, value: str | int | float | bool) -> bool:
  """
  更新系统配置信息
  """
  system_config = await read_system_config_by_key(key)
  if not system_config:
    return False
  system_config.value = str(value)
  await system_config.save()
  return True

async def create_shell_command_log(
  command: str,
  stdout: str = "",
  stderr: str = "") -> ShellCommandLog:
  """
  创建Shell命令日志（命令中的凭证已脱敏）
  """
  from src.core.crypto import redact_shell_command

  if command == "mc alias list --json":
    return None
  shell_command_log = ShellCommandLog(
    command=redact_shell_command(command),
    stdout=stdout,
    stderr=stderr
  )
  await shell_command_log.save()
  return shell_command_log