from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class CapacityTrendPoint(BaseModel):
  captured_at: datetime
  raw_capacity_bytes: int
  raw_used_bytes: int
  logical_usage_bytes: int
  object_count: int
  archive_bytes: int


class RegionCapacityItem(BaseModel):
  region: str
  shown_name: str
  raw_capacity_bytes: int
  raw_used_bytes: int
  logical_usage_bytes: int
  object_count: int
  archive_bytes: int
  archived_object_count: int
  expected_replica_count: int
  actual_replica_count: int
  waterline_percent: float
  daily_growth_bytes: float
  estimated_days_to_70: int | None = None
  estimated_days_to_85: int | None = None
  estimated_days_to_95: int | None = None
  risks: list[str] = Field(default_factory=list)
  trend: list[CapacityTrendPoint] = Field(default_factory=list)
  # These fields are used by caller diagnostics on non-authority Regions. They
  # are additive for the planning page and keep the authoritative sample's
  # confidence separate from capacity admission.
  captured_at: datetime | None = None
  health_status: str = "unknown"
  reachable: bool = False
  health_reasons: list[str] = Field(default_factory=list)


class CapacityPlanningResponse(BaseModel):
  generated_at: datetime
  data: list[RegionCapacityItem]
