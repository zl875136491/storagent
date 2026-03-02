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