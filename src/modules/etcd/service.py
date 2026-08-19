"""Collect a safe, read-only etcd health snapshot.

The application already uses src.core.etcd_op for control-plane operations.
This module reuses the same client configuration and only calls the native
status endpoint; it never exposes etcd key/value data or changes membership.
"""
from __future__ import annotations

import asyncio
import hashlib
import io
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import aetcd

from src.configs.configs import DEFAULT_ETCD_ENDPOINTS, settings
from src.core import metrics
from src.modules.etcd import schema
from src.utils.logger import logger
from src.utils.helpers import utc_now
from src.modules.storage.model import EtcdOperationEvent, EtcdOperationTask


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
    # Some deployments contain ETCD_ENDPOINTS= explicitly. Treat that as a
    # request for the standard cluster rather than silently checking only a
    # legacy local endpoint; non-empty custom values still take precedence.
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
  revision = _as_int(_value(header, "revision", "Revision", default=0))
  if not revision:
    revision = _as_int(_value(status, "revision", "Revision", default=0))
  return {
    "version": str(_value(status, "version", "server_version", default="") or ""),
    "member_id": str(member_id or ""),
    "leader_id": str(leader_id or ""),
    "raft_term": _as_int(_value(status, "raft_term", "raftTerm", default=0)),
    "raft_index": _as_int(_value(status, "raft_index", "raftIndex", default=0)),
    "raft_applied_index": _as_int(_value(status, "raft_applied_index", "raftAppliedIndex", default=0)),
    "db_size_bytes": _as_int(_value(status, "db_size", "dbSize", default=0)),
    "revision": revision,
  }


async def _store_revision(client: Any) -> int:
  """Read the MVCC revision from a range header.

  aetcd's Status object exposes raft fields but not the v3 response header;
  the range header is the authoritative revision used by compaction choices.
  """
  result = await client.get_range(b"/storagent/", b"/storagent/" + b"\xff")
  header = _value(result, "header", default=None)
  return _as_int(_value(header, "revision", "Revision", default=0))


async def _check_endpoint(name: str, host: str, port: int) -> schema.EtcdEndpointStatus:
  endpoint = f"http://{host}:{port}"
  started = time.perf_counter()
  client = None
  try:
    client = _make_client(host, port)
    status = await asyncio.wait_for(client.status(), timeout=max(float(settings.ETCD_HEALTH_TIMEOUT_SECONDS), 0.5))
    fields = _status_fields(status)
    if not fields["revision"]:
      fields["revision"] = await _store_revision(client)
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
      revision=max((item.revision for item in reachable), default=0),
      alarms=list(dict.fromkeys(alarm for item in reachable for alarm in item.alarms)),
      members=members,
      sync=sync,
      reasons=list(dict.fromkeys(reasons)),
      metadata={"source": "configured_endpoints", "multi_endpoint": configured > 1},
    )
    _cached_snapshot = snapshot
    _cached_at = time.monotonic()
    await _record(
      "status",
      "succeeded",
      "system",
      detail={
        "status": snapshot.status,
        "checked_at": snapshot.checked_at,
        "reachable_endpoint_count": snapshot.reachable_endpoint_count,
        "configured_endpoint_count": snapshot.configured_endpoint_count,
        "database_size_bytes": snapshot.database_size_bytes,
        "quorum": snapshot.quorum,
        "alarms": snapshot.alarms,
        "revision": snapshot.revision,
        "leader_endpoint": snapshot.leader_endpoint,
        "average_latency_ms": round(sum(item.latency_ms for item in reachable) / len(reachable), 2) if reachable else 0,
        "max_raft_lag": max((item.raft_lag for item in reachable), default=0),
      },
    )
    return snapshot


async def _record(kind: str, status: str, actor: str, *, endpoint: str = "", revision: int = 0, detail: dict | None = None):
  created_at = utc_now()
  try:
    event = EtcdOperationEvent(
      kind=kind,
      status=status,
      actor=actor,
      endpoint=endpoint,
      revision=revision,
      detail=detail or {},
      created_at=created_at,
    )
    await event.insert()
  except Exception as error:
    logger.warning("Etcd 运维事件落库失败: %s", error)
    # Status checks and unit tests may run before Beanie collections are
    # initialized. Keep the API response usable even when persistence is not.
    from types import SimpleNamespace
    return SimpleNamespace(created_at=created_at)
  return event


async def _client(host: str, port: int):
  return _make_client(host, port)


def _make_client(host: str, port: int):
  """Create an authenticated client only when credentials are configured.

  The isolated test Etcd cluster intentionally has authentication disabled;
  passing empty credentials through aetcd can still trigger an auth request.
  """
  kwargs: dict[str, Any] = {"host": host, "port": port}
  username = str(getattr(settings, "ETCD_USERNAME", "") or "")
  password = str(getattr(settings, "ETCD_PASSWORD", "") or "")
  if username or password:
    kwargs.update(username=username, password=password)
  return aetcd.Client(**kwargs)


async def _first_endpoint():
  endpoints = _endpoint_list()
  if not endpoints:
    raise RuntimeError("没有配置 Etcd 端点")
  name, host, port = endpoints[0]
  return name, host, port


async def keyspace(actor: str = "system") -> schema.EtcdOperationResponse:
  name, host, port = await _first_endpoint()
  client = await _client(host, port)
  created = utc_now()
  try:
    prefix = b"/storagent/"
    end = prefix + b"\xff"
    result = await client.get_range(prefix, end)
    sizes = [len(item.key) + len(item.value) for item in result.kvs]
    detail = {"endpoint": f"{host}:{port}", "key_count": len(result.kvs), "bytes": sum(sizes), "revision": int(getattr(result.header, "revision", 0) or 0)}
    await _record("keyspace", "succeeded", actor, endpoint=name, revision=detail["revision"], detail=detail)
    return schema.EtcdOperationResponse(kind="keyspace", status="succeeded", message="Key 空间检查完成", detail=detail, created_at=created)
  finally:
    await client.close()


async def compact(revision: int, actor: str, physical: bool = True) -> schema.EtcdOperationResponse:
  name, host, port = await _first_endpoint()
  created = utc_now()
  client = await _client(host, port)
  try:
    await client.compact(revision, physical=physical)
    detail = {"revision": revision, "physical": physical}
    await _record("compact", "succeeded", actor, endpoint=name, revision=revision, detail=detail)
    from src.core import audit
    audit.audit("etcd.compact", actor=actor, resource=name, detail=detail)
    return schema.EtcdOperationResponse(kind="compact", status="succeeded", message="Etcd 压缩完成", detail=detail, created_at=created)
  except Exception as error:
    await _record("compact", "failed", actor, endpoint=name, revision=revision, detail={"error": str(error)})
    raise
  finally:
    await client.close()


async def defrag(actor: str) -> schema.EtcdOperationResponse:
  created = utc_now()
  results = []
  for name, host, port in _endpoint_list():
    client = await _client(host, port)
    try:
      await client.defragment()
      results.append({"endpoint": name, "status": "succeeded"})
    except Exception as error:
      results.append({"endpoint": name, "status": "failed", "error": str(error)})
    finally:
      await client.close()
  failed = [item for item in results if item["status"] == "failed"]
  status = "failed" if len(failed) == len(results) else "succeeded"
  detail = {"members": results}
  await _record("defrag", status, actor, detail=detail)
  from src.core import audit
  audit.audit("etcd.defrag", actor=actor, resource="cluster", detail=detail, success=not failed)
  return schema.EtcdOperationResponse(kind="defrag", status=status, message="Etcd 碎片整理完成" if not failed else "部分 Etcd 节点碎片整理失败", detail=detail, created_at=created)


async def disarm_alarm(actor: str) -> schema.EtcdOperationResponse:
  created = utc_now(); results = []
  for name, host, port in _endpoint_list():
    client = await _client(host, port)
    try:
      alarms = await client.disarm_alarm()
      results.append({"endpoint": name, "cleared": len(alarms)})
    except Exception as error:
      results.append({"endpoint": name, "error": str(error)})
    finally:
      await client.close()
  detail = {"members": results}
  await _record("alarm_disarm", "succeeded", actor, detail=detail)
  from src.core import audit
  audit.audit("etcd.alarm_disarm", actor=actor, resource="cluster", detail=detail)
  return schema.EtcdOperationResponse(kind="alarm_disarm", status="succeeded", message="Etcd 活动告警解除请求已完成", detail=detail, created_at=created)


async def snapshot(actor: str) -> tuple[bytes, dict]:
  """Export one consistent Etcd snapshot without exposing credentials."""
  name, host, port = await _first_endpoint()
  client = await _client(host, port)
  created = utc_now()
  buffer = io.BytesIO()
  try:
    await client.snapshot(buffer)
    payload = buffer.getvalue()
    digest = hashlib.sha256(payload).hexdigest()
    detail = {"endpoint": name, "size_bytes": len(payload), "sha256": digest}
    await _record("snapshot", "succeeded", actor, endpoint=name, detail=detail)
    from src.core import audit
    audit.audit("etcd.snapshot", actor=actor, resource=name, detail=detail)
    return payload, {**detail, "created_at": created}
  except Exception as error:
    await _record("snapshot", "failed", actor, endpoint=name, detail={"error": str(error)})
    raise
  finally:
    await client.close()


async def stage_restore(payload: bytes, filename: str, actor: str) -> schema.EtcdOperationResponse:
  """Stage and verify a snapshot for an offline restore window.

  Replacing a live Etcd data directory online is unsafe. This endpoint stores
  the verified artifact and audit record; an operator must stop Etcd and use
  the recorded file during a maintenance window.
  """
  max_bytes = max(int(getattr(settings, "ETCD_SNAPSHOT_MAX_BYTES", 1024 ** 3)), 1)
  if not payload or len(payload) > max_bytes:
    raise ValueError("快照文件为空或超过允许大小")
  root = str(getattr(settings, "ETCD_SNAPSHOT_DIR", "/var/lib/storagent/etcd-snapshots"))
  os.makedirs(root, exist_ok=True)
  digest = hashlib.sha256(payload).hexdigest()
  safe_name = f"restore-{utc_now().strftime('%Y%m%d%H%M%S')}-{digest[:16]}.db"
  path = os.path.join(root, safe_name)
  with open(path, "wb") as handle:
    handle.write(payload)
  detail = {"filename": filename[:200], "path": path, "size_bytes": len(payload), "sha256": digest, "mode": "staged_offline_restore"}
  event = await _record("restore", "staged", actor, detail=detail)
  from src.core import audit
  audit.audit("etcd.restore.stage", actor=actor, resource=safe_name, detail=detail)
  return schema.EtcdOperationResponse(kind="restore", status="staged", message="快照已校验并登记，需在 Etcd 停机维护窗口执行恢复", detail=detail, created_at=event.created_at)


async def trend(limit: int = 100) -> schema.EtcdTrendResponse:
  rows = await EtcdOperationEvent.find(EtcdOperationEvent.kind == "status").sort(-EtcdOperationEvent.created_at).limit(limit).to_list()
  return schema.EtcdTrendResponse(data=[schema.EtcdTrendPoint(**row.detail) for row in reversed(rows) if isinstance(row.detail, dict) and "status" in row.detail])


async def events(limit: int = 50) -> schema.EtcdEventListResponse:
  rows = await EtcdOperationEvent.find().sort(-EtcdOperationEvent.created_at).limit(limit).to_list()
  return schema.EtcdEventListResponse(data=[schema.EtcdEventItem(
    kind=row.kind, status=row.status, actor=row.actor, endpoint=row.endpoint,
    revision=row.revision, detail=row.detail, created_at=row.created_at,
  ) for row in rows])


async def revision_options() -> schema.EtcdRevisionOptionsResponse:
  """Read the current revision and offer conservative retention points."""
  _, host, port = await _first_endpoint()
  client = await _client(host, port)
  try:
    status = await client.status()
    header = _value(status, "header", default=None)
    current = _as_int(_value(header, "revision", "Revision", default=0))
    if not current:
      current = _as_int(_value(status, "revision", "Revision", default=0))
    if not current:
      current = await _store_revision(client)
    candidates = sorted({max(current - offset, 1) for offset in (0, 100, 500, 1000, 5000) if current}, reverse=True)
    return schema.EtcdRevisionOptionsResponse(
      current_revision=current,
      options=[schema.EtcdRevisionOption(revision=item, label=("当前 revision" if item == current else "保留至 revision " + str(item))) for item in candidates],
    )
  finally:
    await client.close()


def _task_response(task: EtcdOperationTask) -> schema.EtcdTaskResponse:
  return schema.EtcdTaskResponse(
    id=str(task.id), kind=task.kind, status=task.status, actor=task.actor, message=task.message,
    result=task.result, error=task.error, created_at=task.created_at,
    started_at=task.started_at, finished_at=task.finished_at,
  )


async def _execute_task(task_id: str, actor: str, revision: int | None) -> None:
  task = await EtcdOperationTask.get(task_id)
  if task is None:
    return
  task.status = "running"
  task.started_at = utc_now()
  await task.save()
  try:
    if task.kind == "keyspace":
      result = await keyspace(actor)
    elif task.kind == "compact":
      if revision is None:
        raise ValueError("压缩任务缺少 revision")
      result = await compact(revision, actor)
    elif task.kind == "defrag":
      result = await defrag(actor)
    else:
      result = await disarm_alarm(actor)
    task.status = "succeeded"
    task.message = result.message
    task.result = {"kind": result.kind, "status": result.status, "detail": result.detail}
  except Exception as error:
    task.status = "failed"
    task.error = str(error)
    task.message = "Etcd 运维任务执行失败"
  task.finished_at = utc_now()
  await task.save()


async def create_task(kind: str, actor: str, revision: int | None = None) -> schema.EtcdTaskResponse:
  task = EtcdOperationTask(kind=kind, actor=actor, message="任务已排队")
  await task.insert()
  asyncio.create_task(_execute_task(str(task.id), actor, revision))
  return _task_response(task)


async def get_task(task_id: str) -> schema.EtcdTaskResponse | None:
  task = await EtcdOperationTask.get(task_id)
  return _task_response(task) if task else None


async def tasks(limit: int = 50) -> schema.EtcdTaskListResponse:
  """Return recent persisted maintenance tasks for the operations history."""
  rows = await EtcdOperationTask.find().sort(-EtcdOperationTask.created_at).limit(limit).to_list()
  return schema.EtcdTaskListResponse(data=[_task_response(row) for row in rows])


def clear_cache() -> None:
  """Test and operational helper; no external endpoint is exposed."""
  global _cached_snapshot, _cached_at
  _cached_snapshot = None
  _cached_at = 0.0
