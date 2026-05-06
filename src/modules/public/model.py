from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
from pydantic import Field
from src.utils.helpers import utc_now
from src.modules.auth.model import User
from typing import List
from pydantic import BaseModel

class Region(Document):
  name: str
  shown_name: str
  
  class Settings:
    name = "region"
    indexes = [
      IndexModel(["name"], unique=True),
      IndexModel(["shown_name"], unique=True),
    ]

class Application(Document):
  name: str
  shown_name: str = Field(default="")
  description: str = Field(default="")
  enabled: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  enabled_at: datetime | None = Field(default=None)
  # regions: List[Link[Region]] = Field(default=[])
  author: Link[User]
  approver: Link[User] | None = Field(default=None)
  
  class Settings:
    name = "application"
    indexes = [
      IndexModel(["name"], unique=True),
      IndexModel(["shown_name"], unique=True),
    ]

class APIKey(Document):
  application: Link[Application]
  key: str
  expired_at: datetime # 过期时间
  deleted: bool = Field(default=False)
  deleted_at: datetime | None = Field(default=None)
  
  class Settings:
    name = "api_key"
    indexes = [
      IndexModel(["key"], unique=True),
      IndexModel(["app"]),
      IndexModel(["expired_at"]),
    ]

class APIKeyUsageData(BaseModel):
  date: datetime
  server: str
  type: str
  size: int

class APIKeyUsage(Document):
  api_key: Link[APIKey]
  is_full: bool = Field(default=False)
  data: List[APIKeyUsageData] = Field(default=[]) # 最大3000条数据
  full_at: datetime | None = Field(default=None) # 满3000条数据的时间

class SystemConfig(Document):
  key: str
  value: str
  name: str
  description: str
  value_type: str
  
  class Settings:
    name = "system_config"
    indexes = [
      IndexModel(["key"], unique=True),
    ]

class ShellCommandLog(Document):
  command: str
  date: datetime = Field(default_factory=utc_now)
  stdout: str = Field(default="")
  stderr: str = Field(default="")
  
  class Settings:
    name = "shell_command"
    indexes = [
      IndexModel(["date"]),
    ]