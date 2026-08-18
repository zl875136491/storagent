"""Collect a safe, read-only etcd health snapshot.

The application already uses src.core.etcd_op for control-plane operations.
This module reuses the same client configuration and only calls the native
status endpoint; it never exposes etcd key/value data or changes membership.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import aetcd

from src.configs.configs import DEFAULT_ETCD_ENDPOINTS, settings
from src.core import metrics
from src.modules.etcd import schema
from src.utils.logger import logger


_cache_lock = asyncio.Lock()
_cached_snapshot: schema.EtcdClusterStatusResponse | None = None
_cached_at = 0.0


def _value(source: Any, *names: str, default: Any = None) -> Any:
  """Read a field from dict-like and attribute-style aetcd responses."""
  for name in names:
    if isinstance(source, dict) and name in source:
      return source[name]
    value = getattr(source, name, None)
    if value is not None:
      return value
  return default


def _as_int(value: Any) -> int:
  try:
    return max(int(value or 0), 0)
  except (TypeError, ValueError):
    return 0


def _endpoint_list() -> list[tuple[str, str, int]]:
  """Return configured endpoints, preserving the complete default cluster."""
  raw = str(getattr(settings, "ETCD_ENDPOINTS", "") or "")
  values = [item.strip() for item in raw.split(",") if item.strip()]
  if not values:
    # Some production env files contain ETCD_ENDPOINTS= explicitly. Treat
    # that as "use the standard cluster", not as a request to check only the
    # legacy local endpoint; custom non-empty values still take precedence.
    values = list(DEFAULT_ETCD_ENDPOINTS)

  result: list[tuple[str, str, int]] = []
  for index, value in enumerate(values, start=1):
    candidate = value if "://" in value else f"http://{value}"
    parsed = urlparse(candidate)
    if parsed.hostname:
      result.append((f"etcd-{index}", parsed.hostname, parsed.port or 2379))
  return result


def _status_fields(status: Any) -> dict[str, Any]:
  """Normalize common etcd v3 status response field names."""
  header = _value(status, "header", default=None)
  member_id = _value(status, "member_id", "memberId", default=None)
  if not member_id:
    member_id = _value(header, "member_id", "memberId", default="")
  leader = _value(status, "leader", "leader_id", "leaderId", default=None)
  leader_id = _value(leader, "id", "member_id", "memberId", default=leader)
  return {
    "version": str(_value(status, "version", "server_version", default="") or ""),
    "member_id": str(member_id or ""),
    "leader_id": str(leader_id or ""),
    "raft_term": _as_int(_value(status, "raft_term", "raftTerm", default=0)),
    "raft_index": _as_int(_value(status, "raft_index", "raftIndex", default=0)),
    "raft_applied_index": _as_int(_value(status, "raft_applied_index", "raftAppliedIndex", default=0)),
    "db_size_bytes": _as_int(_value(status, "db_size", "dbSize", default=0)),
  }


async def _check_endpoint(name: str, host: str, port: int) -> schema.EtcdEndpointStatus:
  endpoint = f"http://{host}:{port}"
  started = time.perf_counter()
  client = None
  try:
    client = aetcd.Client(
      host=host,
      port=port,
      username=settings.ETCD_USERNAME,
      password=settings.ETCD_PASSWORD,
    )
    status = await asyncio.wait_for(client.status(), timeout=max(float(settings.ETCD_HEALTH_TIMEOUT_SECONDS), 0.5))
    fields = _status_fields(status)
    applied_index = _value(status, "raft_applied_index", "raftAppliedIndex", default=None)
    members = []
    async for member in client.members():
      members.append(member)
    endpoint_marker = f"{host}:{port}"
    own_member = next(
      (
        member
        for member in members
        if any(endpoint_marker in str(url) for url in (_value(member, "client_urls", "clientUrls", default=[]) or []))
      ),
      None,
    )
    if own_member is not None:
      fields["member_id"] = str(_value(own_member, "id", "member_id", "memberId", default="") or "")
    alarms = []
    async for alarm in client.list_alarms():
      alarm_name = _value(alarm, "alarm", "alarm_type", "alarmType", default=alarm)
      alarms.append(str(alarm_name))
    # aetcd 1.0.0rc3 does not expose applied index. Do not turn an
    # unavailable optional field into a false critical lag alarm.
    lag = max(fields["raft_index"] - _as_int(applied_index), 0) if applied_index is not None else 0
    reasons: list[str] = []
    result: schema.EtcdStatus = "healthy"
    if alarms:
      result = "critical"
      reasons.append("etcd 存在活动告警")
    elif not fields["leader_id"] or fields["leader_id"] in {"0", "None"}:
      result = "critical"
      reasons.append("当前端点未报告 Leader")
    elif applied_index is not None and lag >= settings.ETCD_RAFT_LAG_CRITICAL:
      result = "critical"
      reasons.append(f"Raft applied index 落后 {lag}")
    elif applied_index is not None and lag >= settings.ETCD_RAFT_LAG_WARNING:
      result = "warning"
      reasons.append(f"Raft applied index 落后 {lag}")
    latency = round((time.perf_counter() - started) * 1000, 2)
    return schema.EtcdEndpointStatus(
      name=name,
      endpoint=endpoint,
      status=result,
      reachable=True,
      is_leader=fields["member_id"] != "" and fields["member_id"] == fields["leader_id"],
      latency_ms=latency,
      **fields,
      raft_lag=lag,
      alarms=alarms,
      reasons=reasons,
    )
  except Exception as error:
    latency = round((time.perf_counter() - started) * 1000, 2)
    logger.warning(f"etcd 状态检查失败 {endpoint}: {error}")
    return schema.EtcdEndpointStatus(
      name=name,
      endpoint=endpoint,
      status="critical",
      latency_ms=latency,
      error=str(error),
      reasons=["端点不可达或认证失败"],
    )
  finally:
    if client is not None:
      try:
        await client.close()
      except Exception:
        pass


def _sync_status() -> schema.EtcdSyncStatus:
  data = metrics.snapshot()
  counters = data.get("counters", {})
  gauges = data.get("gauges", {})
  reconnects = int(counters.get("etcd_watch_reconnects_total", 0))
  failures = int(counters.get("sync_reconcile_failures_total", 0))
  success_timestamp = gauges.get("sync_last_success_timestamp_seconds")
  failure_timestamp = gauges.get("sync_last_failure_timestamp_seconds")
  return schema.EtcdSyncStatus(
    watch_status="warning" if reconnects else "healthy",
    watch_reconnects=reconnects,
    reconcile_runs=int(counters.get("sync_reconcile_runs_total", 0)),
    reconcile_failures=failures,
    last_reconcile_success_at=datetime.fromtimestamp(success_timestamp, timezone.utc) if success_timestamp else None,
    last_reconcile_failure_at=datetime.fromtimestamp(failure_timestamp, timezone.utc) if failure_timestamp else None,
  )


async def get_status(*, force_refresh: bool = False) -> schema.EtcdClusterStatusResponse:
  """Return a short-lived snapshot so page refreshes do not fan out to etcd."""
  global _cached_snapshot, _cached_at
  ttl = max(float(settings.ETCD_HEALTH_CACHE_TTL_SECONDS), 0)
  if not force_refresh and _cached_snapshot is not None and time.monotonic() - _cached_at < ttl:
    return _cached_snapshot

  async with _cache_lock:
    if not force_refresh and _cached_snapshot is not None and time.monotonic() - _cached_at < ttl:
      return _cached_snapshot
    endpoints = _endpoint_list()
    members = list(await asyncio.gather(*[_check_endpoint(*item) for item in endpoints]))
    reachable = [item for item in members if item.reachable]
    configured = len(members)
    quorum = bool(reachable) and len(reachable) >= (configured // 2 + 1)
    leaders = [item for item in reachable if item.leader_id]
    leader_id = next((item.leader_id for item in leaders if item.is_leader), leaders[0].leader_id if leaders else "")
    leader_endpoint = next((item.endpoint for item in members if item.is_leader), "")
    reasons: list[str] = []
    status: schema.EtcdStatus = "healthy"
    if not reachable:
      status = "critical"
      reasons.append("没有可达的 etcd 端点")
    elif not quorum:
      status = "critical"
      reasons.append(f"可达端点 {len(reachable)}/{configured}，不满足 quorum")
    elif not leader_id:
      status = "critical"
      reasons.append("当前 etcd 集群没有 Leader")
    elif len(reachable) != configured or any(item.status == "critical" for item in reachable):
      status = "warning"
      reasons.append("部分 etcd 端点需要关注")
    elif any(item.status == "warning" for item in reachable):
      status = "warning"
      reasons.extend(reason for item in reachable for reason in item.reasons)

    sync = _sync_status()
    if sync.watch_status == "warning" and status == "healthy":
      status = "warning"
      reasons.append("Storagent Etcd Watch 曾发生重连")
    versions = sorted({item.version for item in reachable if item.version})
    snapshot = schema.EtcdClusterStatusResponse(
      status=status,
      checked_at=datetime.now(timezone.utc),
      configured_endpoint_count=configured,
      reachable_endpoint_count=len(reachable),
      quorum=quorum,
      leader_id=leader_id,
      leader_endpoint=leader_endpoint,
      versions=versions,
      database_size_bytes=sum(item.db_size_bytes for item in reachable),
      alarms=list(dict.fromkeys(alarm for item in reachable for alarm in item.alarms)),
      members=members,
      sync=sync,
      reasons=list(dict.fromkeys(reasons)),
      metadata={"source": "configured_endpoints", "multi_endpoint": configured > 1},
    )
    _cached_snapshot = snapshot
    _cached_at = time.monotonic()
    return snapshot


def clear_cache() -> None:
  """Test and operational helper; no external endpoint is exposed."""
  global _cached_snapshot, _cached_at
  _cached_snapshot = None
  _cached_at = 0.0
