"""File-level catalog data shared by versioned file Services."""
from datetime import datetime
from typing import Literal

from beanie import Document
from pydantic import Field
from pymongo import IndexModel

from src.utils.helpers import utc_now


ObjectState = Literal[
  "active",
  "soft_deleted",
  "archive_pending",
  "archived",
  "purge_pending",
  "purged",
  "archive_failed",
  "purge_failed",
]


class ObjectCatalog(Document):
  """Application-visible state for one object key and active version."""

  object_id: str
  app_name: str
  bucket: str
  object_key: str
  storage_key: str
  minio_version_id: str | None = None
  size_bytes: int = Field(ge=0)
  etag: str = ""
  content_type: str = "application/octet-stream"
  source_region: str = ""
  state: ObjectState = "active"
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  deleted_at: datetime | None = None
  restore_until: datetime | None = None
  archive_after: datetime | None = None
  purge_after: datetime | None = None
  archive_id: str = ""
  archive_checksum: str = ""
  deletion_generation: int = Field(default=0, ge=0)
  last_operation_id: str = ""

  class Settings:
    name = "object_catalog"
    indexes = [
      IndexModel(["object_id"], unique=True),
      IndexModel([("app_name", 1), ("object_key", 1)], unique=True),
      IndexModel([("app_name", 1), ("state", 1), ("object_key", 1), ("object_id", 1)]),
      IndexModel([("state", 1), ("archive_after", 1)]),
      IndexModel([("state", 1), ("purge_after", 1)]),
    ]
