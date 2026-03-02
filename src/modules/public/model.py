from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel

class Region(Document):
  name: str
  
  class Settings:
    name = "region"
    indexes = [
      IndexModel(["name"], unique=True),
    ]

class Application(Document):
  name: str
  description: str
  created_at: datetime
  updated_at: datetime
  
  class Settings:
    name = "application"
    indexes = [
      IndexModel(["name"], unique=True),
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

