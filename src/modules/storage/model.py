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
  # 对外 Storagent 网关域名；host 仅用于 MinIO 内网连接。
  domain: str = Field(default="")
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
  """One compressed chunk of a cached recursive MinIO inventory."""
  server_id: str
  generation: str
  chunk_index: int = Field(ge=0)
  payload: bytes
  fetched_at: datetime = Field(default_factory=utc_now)
  expires_at: datetime

  class Settings:
    # A new collection avoids inheriting the old one-document-per-server index.
    name = "server_file_details_cache_v2"
    indexes = [
      IndexModel(
        [("server_id", 1), ("generation", 1), ("chunk_index", 1)],
        unique=True,
      ),
      IndexModel([("server_id", 1), ("fetched_at", -1)]),
      IndexModel(["expires_at"], expireAfterSeconds=0),
    ]


class StorageOperation(Document):
  """Persistent state for long-running MinIO maintenance operations."""
  kind: Literal["cluster_heal", "replication_reconcile", "replication_resync"]
  status: Literal["queued", "running", "succeeded", "failed"] = "queued"
  server: str
  bucket: str = ""
  target: str = ""
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
      IndexModel([("kind", 1), ("bucket", 1), ("server", 1), ("target", 1), ("status", 1)]),
      IndexModel(["expires_at"], expireAfterSeconds=0),
    ]


class RegionCapacitySnapshot(Document):
  """Daily regional capacity sample used by planning views and risk forecasts."""
  region: str
  shown_name: str
  raw_capacity_bytes: int = Field(ge=0)
  raw_used_bytes: int = Field(ge=0)
  logical_usage_bytes: int = Field(ge=0)
  object_count: int = Field(ge=0)
  archive_bytes: int = Field(default=0, ge=0)
  archived_object_count: int = Field(default=0, ge=0)
  expected_replica_count: int = Field(default=0, ge=0)
  actual_replica_count: int = Field(default=0, ge=0)
  captured_at: datetime = Field(default_factory=utc_now)
  sample_day: str

  class Settings:
    name = "region_capacity_snapshot"
    indexes = [
      IndexModel([("region", 1), ("sample_day", 1)], unique=True),
      IndexModel([("captured_at", -1)]),
    ]
  


class EtcdOperationEvent(Document):
  """Audited Etcd maintenance action and its bounded result."""
  kind: Literal["status", "snapshot", "restore", "compact", "defrag", "keyspace", "alarm_disarm"]
  status: Literal["started", "succeeded", "failed", "staged"]
  actor: str = "-"
  endpoint: str = ""
  revision: int = 0
  detail: dict[str, Any] = Field(default_factory=dict)
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "etcd_operation_event"
    indexes = [IndexModel(["created_at"], unique=False)]


class EtcdOperationTask(Document):
  """Persistent state for an Etcd maintenance task shown in the UI."""
  kind: Literal["keyspace", "compact", "defrag", "alarm-disarm"]
  status: Literal["queued", "running", "succeeded", "failed"] = "queued"
  actor: str = "-"
  message: str = ""
  result: dict[str, Any] = Field(default_factory=dict)
  error: str = ""
  created_at: datetime = Field(default_factory=utc_now)
  started_at: datetime | None = None
  finished_at: datetime | None = None

  class Settings:
    name = "etcd_operation_task"
    indexes = [IndexModel(["created_at"], unique=False), IndexModel(["status", "created_at"])]
