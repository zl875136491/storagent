from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ObjectItem(BaseModel):
  object_id: str
  object_key: str
  size_bytes: int
  etag: str
  content_type: str
  state: str
  created_at: datetime
  updated_at: datetime
  deleted_at: datetime | None = None
  restore_until: datetime | None = None


class ObjectListData(BaseModel):
  items: list[ObjectItem]
  next_cursor: str | None = None
  has_more: bool = False


class ObjectListResponse(BaseModel):
  data: ObjectListData
  request_id: str


class ObjectMutationData(BaseModel):
  object_id: str
  object_key: str
  state: str
  deleted_at: datetime | None = None
  restore_until: datetime | None = None


class ObjectMutationResponse(BaseModel):
  data: ObjectMutationData
  request_id: str


class ShareCreateRequest(BaseModel):
  expires_in_seconds: int = Field(default=600, ge=60, le=900)
  download_name: str | None = Field(default=None, max_length=1024)


class ShareData(BaseModel):
  share_id: str
  download_url: str
  expires_at: datetime
  expires_in_seconds: int
  single_use: Literal[True] = True
  filename: str


class ShareCreateResponse(BaseModel):
  data: ShareData
  request_id: str
