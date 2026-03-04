import secrets
from typing import Type
from loguru import logger
from beanie import Document
from zoneinfo import ZoneInfo
from bson.objectid import ObjectId
from pydantic import NonNegativeFloat
from decimal import Decimal, ROUND_UP
from datetime import datetime, timezone
from requests import get as requests_get
from requests import post as post_requests
from src.configs.configs import settings


LOCAL_TIMEZONE = ZoneInfo(settings.TIMEZONE)

def local_utc_now() -> datetime:
  """
  获取本地时区的当前时间
  """
  return datetime.now(LOCAL_TIMEZONE)

def utc_now() -> datetime:
  """
  获取 UTC 时区的当前时间
  """
  return datetime.now(timezone.utc)

def mongo_date_to_utc(dt: datetime) -> datetime:
  """
  将 MongoDB 日期时间对象转换为 UTC 时区的日期时间对象
  """
  return dt.replace(tzinfo=timezone.utc)

async def import_user_from_springboard(username: str) -> dict | None:
  """
  从 springboard 中导入用户
  """
  api_url = settings.USER_INFO_URL + "?itcode=" + username
  response = requests_get(api_url, timeout=3)
  if response.status_code != 200:
    logger.error(f"Import User from Springboard Error: {response.text}")
    return None
  return response.json()

def convert_utc_to_local_str(dt: datetime) -> str:
  """
  将 UTC datetime 对象转换为本地时区的字符串格式。
  """
  # 如果 datetime 对象是 naive (无时区信息)，假定它是 UTC 并为其添加时区信息
  if isinstance(dt, datetime) and dt.tzinfo is None:
    utc_dt = dt.replace(tzinfo=timezone.utc)
  else:
    # 如果已经有时区信息，确保其是 UTC
    utc_dt = dt.astimezone(timezone.utc)

  local_dt = utc_dt.astimezone(LOCAL_TIMEZONE)

  return local_dt.strftime("%Y-%m-%d %H:%M:%S")

def get_full_permissions(selected_permissions: list) -> list:
  """
  获取所有选中的权限及其子权限（包含去重处理）
  """
  from src.configs.consts import preset_permissions
  full_set = set[str]()

  def discover(perm_key):
    if perm_key not in preset_permissions:
      return
    
    # 如果已经处理过该权限，跳过以防止循环引用（虽然在权限树中较少见）
    if perm_key in full_set:
      return
        
    full_set.add(perm_key)
    
    # 递归获取子权限
    children = preset_permissions[perm_key].get("children", [])
    for child in children:
      discover(child)

  # 遍历输入的初始权限列表
  for pmt in selected_permissions:
    discover(pmt)
      
  return list[str](full_set)

def try_to_obj_id(obj_id: ObjectId | str | Document) -> ObjectId:
  """
  尝试将对象ID转换为ObjectId
  """
  from src.core.exception import CustomException, ErrorDesc
  if isinstance(obj_id, Document):
    obj_id = obj_id.id
  if not isinstance(obj_id, ObjectId):
    try:
      obj_id = ObjectId(obj_id)
    except Exception as e:
      logger.error(f"🔍 [Try to Object ID] Error: {e}")
      raise CustomException(ErrorDesc.OBJECT_ID_NOT_VALID, "尝试将对象转换为ObjectId失败")
  return obj_id

def file_tree_post_process(node_dict):
  """
  递归处理：将字典转为列表，并计算 size 和最新的 last_modified
  """
  result_list = []
  for key, node in node_dict.items():
    if not node.get("is_file", False):
      # 递归处理子节点
      children_list = file_tree_post_process(node.pop("children"))
      node["children"] = children_list
      
      # 计算文件夹属性
      total_size = sum(child["size"] for child in children_list)
      # 获取子节点中最晚的时间
      valid_times = [child["last_modified"] for child in children_list if child["last_modified"]]
      latest_time = max(valid_times) if valid_times else node["last_modified"]
      
      node["size"] = total_size
      node["last_modified"] = latest_time
    
    # 移除辅助标记并添加到结果
    node.pop("is_file", None)
    result_list.append(node)
  
  # 按照名称排序（可选）
  return sorted(result_list, key=lambda x: x['name'])

def build_file_tree(files):
  """
  构建文件树
  """
  root_nodes = {}

  for file in files:
    path_parts = file['name'].split('/')
    current_level = root_nodes
    
    # 逐层构建/查找目录
    for i, part in enumerate(path_parts):
      is_file = (i == len(path_parts) - 1)
      
      if part not in current_level:
        if is_file:
          # 如果是具体文件
          current_level[part] = {
            "name": part,
            "size": file.get('size', 0),
            "last_modified": file.get('last_modified', ""),
            "is_file": True
          }
        else:
          # 如果是文件夹
          current_level[part] = {
            "name": part,
            "size": 0,
            "last_modified": "0001-01-01T00:00:00Z", # 初始占位时间
            "children": {},
            "is_file": False
          }
      
      if not is_file:
        current_level = current_level[part]["children"]

  return file_tree_post_process(root_nodes)

def generate_api_key():
  """
  生成 API 密钥
  """
  prefix="sk"
  # 生成 36 字节的安全随机数，并转换为 Base64 风格字符串
  # token_urlsafe 会生成包含 A-Z, a-z, 0-9, -, _ 的字符
  random_str = secrets.token_urlsafe(36).replace('-', '').replace('_', '').lower()
  return f"{prefix}-{random_str}"