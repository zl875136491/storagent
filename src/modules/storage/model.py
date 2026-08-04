from beanie import Document
from beanie.odm.fields import Link
from pydantic import Field
from pymongo import IndexModel
from src.modules.public.model import Region, Application
from src.utils.helpers import utc_now
from datetime import datetime, timedelta
from typing import Any, Literal

class MinioServer(Document):
  """
  Minio 服务器
  """
  region: Link[Region]
  name: str = Field(default="")
  host: str
  server_port: int
  minio_port: int
  access_key: str = Field(default="")
  secret_key: str = Field(default="")
  master: bool = Field(default=False)
  replicate_weight: int = Field(default=0, description="复制集权重")
  
  class Settings:
    name = "minio_server"
    indexes = [
      IndexModel(["region"], unique=True),
      IndexModel(["host", "minio_port"], unique=True),
    ]

class MinioBucket(Document):
  """
  Minio 存储桶
  """
  region: Link[Region]
  app: Link[Application]
  server: Link[MinioServer]
  name: str = Field(default="")
  
  class Settings:
    name = "minio_bucket"
    indexes = [
      IndexModel(["region", "name"], unique=True),
      IndexModel(["name"]),
      IndexModel(["server"]),
      IndexModel(["app"]),
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


class ServerFileDetailsCache(Document):
  """Cached recursive bucket inventory for one configured MinIO server."""
  server_id: str
  data: list[dict[str, Any]] = Field(default_factory=list)
  fetched_at: datetime = Field(default_factory=utc_now)
  expires_at: datetime

  class Settings:
    name = "server_file_details_cache"
    indexes = [
      IndexModel(["server_id"], unique=True),
      IndexModel(["expires_at"], expireAfterSeconds=0),
    ]


class StorageOperation(Document):
  """Persistent state for long-running MinIO maintenance operations."""
  kind: Literal["cluster_heal"]
  status: Literal["queued", "running", "succeeded", "failed"] = "queued"
  server: str
  bucket: str = ""
  actor: str = "-"
  message: str = ""
  result: dict[str, Any] = Field(default_factory=dict)
  created_at: datetime = Field(default_factory=utc_now)
  started_at: datetime | None = None
  finished_at: datetime | None = None
  expires_at: datetime = Field(default_factory=lambda: utc_now() + timedelta(days=30))

  class Settings:
    name = "storage_operation"
    indexes = [
      IndexModel([("created_at", -1)]),
      IndexModel([("kind", 1), ("server", 1), ("status", 1)]),
      IndexModel(["expires_at"], expireAfterSeconds=0),
    ]
  
