"""Public contracts for the read-only Celery operations module."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class CeleryBrokerStatus(BaseModel):
  enabled: bool
  reachable: bool
  transport: str = "mongodb"
  database: str = ""
  message: str = ""


class CeleryWorkerStatus(BaseModel):
  name: str
  hostname: str = ""
  region: str = ""
  status: str
  last_seen: datetime | None = None
  heartbeat_age_seconds: int | None = None
  active_count: int = 0
  reserved_count: int = 0
  scheduled_count: int = 0
  processed_count: int = 0
  concurrency: int | None = None
  registered_task_count: int = 0
  source: str = "inspect"


class CeleryQueueStatus(BaseModel):
  name: str
  pending_count: int = 0
  exchange: str = "celery"
  routing_keys: list[str] = Field(default_factory=list)
  worker_count: int = 0


class CeleryTaskExecution(BaseModel):
  id: str
  name: str
  status: str
  worker: str = ""
  region: str = ""
  queue: str = "celery"
  retries: int = 0
  received_at: datetime | None = None
  started_at: datetime | None = None
  finished_at: datetime | None = None
  eta: datetime | None = None
  duration_ms: int | None = None
  result_summary: str = ""
  error: str = ""
  source: str = "runtime"


class CeleryTaskCatalogItem(BaseModel):
  name: str
  display_name: str
  trigger: str
  schedule_seconds: int | None = None
  execution_scope: str
  description: str


class CeleryOverviewResponse(BaseModel):
  generated_at: datetime
  broker: CeleryBrokerStatus
  workers: list[CeleryWorkerStatus] = Field(default_factory=list)
  queues: list[CeleryQueueStatus] = Field(default_factory=list)
  active_tasks: list[CeleryTaskExecution] = Field(default_factory=list)
  reserved_tasks: list[CeleryTaskExecution] = Field(default_factory=list)
  scheduled_tasks: list[CeleryTaskExecution] = Field(default_factory=list)
  task_catalog: list[CeleryTaskCatalogItem] = Field(default_factory=list)
  inspection_message: str = ""


class CeleryHistoryResponse(BaseModel):
  generated_at: datetime
  available: bool
  data: list[CeleryTaskExecution] = Field(default_factory=list)
  legacy_record_count: int = 0
  message: str = ""
