from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class DiagnosticCheck(BaseModel):
  name: str = Field(min_length=1, max_length=80)
  status: Literal["passed", "failed", "skipped"]
  detail: str = Field(default="", max_length=4000)
  latency_ms: int = Field(default=0, ge=0)


class DiagnosticReportCreate(BaseModel):
  run_id: str = Field(min_length=8, max_length=128)
  network_only: bool = False
  overall_status: Literal["passed", "failed", "partial"]
  checks: list[DiagnosticCheck] = Field(default_factory=list, max_length=32)
  raw_log: str = Field(default="", max_length=200_000)


class DiagnosticRunItem(BaseModel):
  id: str
  run_id: str
  api_version: Literal["v1", "v2"]
  app_name: str
  source_host: str
  network_only: bool
  overall_status: Literal["passed", "failed", "partial"]
  checks: list[DiagnosticCheck]
  raw_log: str
  created_at: datetime


class DiagnosticRunListResponse(BaseModel):
  data: list[DiagnosticRunItem]
