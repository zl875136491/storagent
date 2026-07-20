from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
from typing import List
from pydantic import Field
from src.utils.helpers import utc_now
  
class Role(Document):
  name: str
  is_admin: bool = False
  permissions: List[str] = Field(default=[])
  
  class Settings:
    name = "role"
    indexes = [
      IndexModel(["name"], unique=True),
    ]
  
class User(Document):
  username: str
  name: str
  hashed_password: str
  roles: List[Link[Role]] = Field(default=[])
  permissions: List[str] = Field(default=[])
  """跨节点同步创建的占位用户，禁止登录"""
  is_sync: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  
  class Settings:
    name = "user"
    indexes = [
      IndexModel(["username"], unique=True),
    ]
  
class TempCode(Document):
  username: str
  code: str
  expired_at: datetime
  
  class Settings:
    name = "temp_code"
    indexes = [
      IndexModel(["expired_at"]),
    ]
  
class DestoryedToken(Document):
  token: str
  expired_at: datetime
  
  class Settings:
    name = "destoryed_token"
    indexes = [
      IndexModel(["token"], unique=True),
      IndexModel(["expired_at"]),
    ]