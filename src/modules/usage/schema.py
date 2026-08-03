from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class UsageApplicationOption(BaseModel):
  name: str
  shown_name: str


class UsageAPIKeyOption(BaseModel):
  id: str
  hint: str
  app_name: str
  app_shown_name: str


class UsageOptionsResponse(BaseModel):
  applications: list[UsageApplicationOption]
  api_keys: list[UsageAPIKeyOption]


class UsageTotals(BaseModel):
  upload_requests: int = 0
  upload_bytes: int = 0
  download_requests: int = 0
  download_bytes: int = 0


class UsagePoint(UsageTotals):
  period_start: datetime
  app_name: str
  app_shown_name: str
  api_key_id: str
  api_key_hint: str
  region: str
  first_at: datetime
  last_at: datetime


class UsageEventItem(BaseModel):
  id: str
  occurred_at: datetime
  app_name: str
  app_shown_name: str
  api_key_id: str
  api_key_hint: str
  operation: Literal["upload", "download"]
  bytes_transferred: int = Field(ge=0)
  region: str


class UsageQueryResponse(BaseModel):
  region: str
  start_at: datetime
  end_at: datetime
  interval: Literal["hour", "day"]
  totals: UsageTotals
  points: list[UsagePoint]
  events: list[UsageEventItem]
  truncated: bool = False
