from beanie import Document
from beanie.odm.fields import Link
from pydantic import Field
from pymongo import IndexModel
from src.modules.public.model import Region
from datetime import datetime

class MinioServer(Document):
  """
  Minio 服务器
  """
  region: Link[Region]
  name: str = Field(..., default="")
  host: str
  port: int
  access_key: str = Field(..., default="")
  secret_key: str = Field(..., default="")
  
  class Settings:
    name = "minio_server"
    indexes = [
      IndexModel(["region"]),
      IndexModel(["host", "port"], unique=True),
    ]

class MinioBucket(Document):
  """
  Minio 存储桶
  """
  region: Link[Region]
  server: Link[MinioServer]
  name: str = Field(..., default="")
  
  class Settings:
    name = "minio_bucket"
    indexes = [
      IndexModel(["region", "name"], unique=True),
      IndexModel(["name"]),
      IndexModel(["server"])
    ]

class MinioEvent(Document):
  """
  Minio 事件
  """
  region: Link[Region]
  server: Link[MinioServer]
  bucket: Link[MinioBucket]
  event: str
  object: str
  size: int
  etag: str
  last_modified: datetime
  
  class Settings:
    name = "minio_event"
    indexes = [
      IndexModel(["region"]),
      IndexModel(["server"]),
      IndexModel(["bucket"]),
      IndexModel(["last_modified"]),
    ]
    
  
  