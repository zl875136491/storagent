from datetime import datetime

from pydantic import BaseModel, Field


class AuditEventItem(BaseModel):
  """A bounded, display-safe projection of one persisted audit event."""

  id: str
  action: str
  action_label: str = ""
  actor: str
  resource: str
  success: bool
  detail: str
  region: str
  created_at: datetime


class AuditEventListResponse(BaseModel):
  data: list[AuditEventItem]
  total: int = Field(ge=0)
  page: int = Field(ge=1)
  page_size: int = Field(ge=1)
  has_more: bool


class AuditActionOption(BaseModel):
  code: str
  label: str


class AuditEventOptionsResponse(BaseModel):
  actions: list[AuditActionOption]
  actors: list[str]
  regions: list[str]
