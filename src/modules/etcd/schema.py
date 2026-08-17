"""Schemas for the read-only etcd operations view."""
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


EtcdStatus = Literal["healthy", "warning", "critical", "unknown"]


class EtcdEndpointStatus(BaseModel):
  """Health information for one configured etcd client endpoint."""

  name: str
  endpoint: str
  status: EtcdStatus
  reachable: bool = False
  is_leader: bool = False
  latency_ms: float = 0
  version: str = ""
  member_id: str = ""
  leader_id: str = ""
  raft_term: int = 0
  raft_index: int = 0
  raft_applied_index: int = 0
  raft_lag: int = 0
  db_size_bytes: int = 0
  alarms: list[str] = Field(default_factory=list)
  error: str = ""
  reasons: list[str] = Field(default_factory=list)


class EtcdSyncStatus(BaseModel):
  """Storagent's own watch/reconcile health, separate from etcd health."""

  watch_status: EtcdStatus = "unknown"
  watch_reconnects: int = 0
  reconcile_runs: int = 0
  reconcile_failures: int = 0
  last_reconcile_success_at: datetime | None = None
  last_reconcile_failure_at: datetime | None = None


class EtcdClusterStatusResponse(BaseModel):
  status: EtcdStatus
  checked_at: datetime
  configured_endpoint_count: int = 0
  reachable_endpoint_count: int = 0
  quorum: bool = False
  leader_id: str = ""
  leader_endpoint: str = ""
  versions: list[str] = Field(default_factory=list)
  database_size_bytes: int = 0
  alarms: list[str] = Field(default_factory=list)
  members: list[EtcdEndpointStatus] = Field(default_factory=list)
  sync: EtcdSyncStatus
  reasons: list[str] = Field(default_factory=list)
  metadata: dict[str, Any] = Field(default_factory=dict)
