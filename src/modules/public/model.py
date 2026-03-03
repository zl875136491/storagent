from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
from pydantic import Field
from src.utils.helpers import utc_now
from src.modules.auth.model import User
from typing import List

class Region(Document):
  name: str
  nickname: str
  
  class Settings:
    name = "region"
    indexes = [
      IndexModel(["name"], unique=True),
      IndexModel(["nickname"], unique=True),
    ]

class Application(Document):
  name: str
  nickname: str
  description: str = Field(default="")
  enabled: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  enabled_at: datetime | None = Field(default=None)
  regions: List[Link[Region]] = Field(default=[])
  author: Link[User]
  approver: Link[User] | None = Field(default=None)
  
  class Settings:
    name = "application"
    indexes = [
      IndexModel(["name"], unique=True),
      IndexModel(["nickname"], unique=True),
    ]

class APIKey(Document):
  key: str
  app: Link[Application]
  expired_at: datetime
  
  class Settings:
    name = "api_key"
    indexes = [
      IndexModel(["key"], unique=True),
      IndexModel(["app"]),
      IndexModel(["expired_at"]),
    ]

