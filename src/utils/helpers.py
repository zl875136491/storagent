import time
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