"""Read Celery runtime state without exposing broker credentials or task arguments."""
from __future__ import annotations

import asyncio
import json
import re
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlparse

from motor.motor_asyncio import AsyncIOMotorClient

from src.configs.configs import settings
from src.core.celery_client import broker_url, celery_app, result_backend
from src.core.celery_routing import task_queue_name
from src.modules.celery import schema
from src.utils.helpers import utc_now
from src.utils.logger import logger


TASK_CATALOG = (
  {
    "name": "storagent.auth.cleanup_expired_tokens",
    "display_name": "认证过期数据清理",
    "trigger": "周期调度",
    "schedule_setting": "AUTH_CLEANUP_INTERVAL_SECONDS",
    "execution_scope": "本区元数据",
    "description": "清理本区过期 JWT 吊销记录和 OA 认证挑战。",
  },
  {
    "name": "storagent.files.archive_expired_objects",
    "display_name": "恢复期超时文件归档",
    "trigger": "周期调度",
    "schedule_setting": "OBJECT_ARCHIVE_INTERVAL_SECONDS",
    "execution_scope": "本区对象目录与归档桶",
    "description": "扫描本区恢复期已结束的对象，归档后删除源对象。",
  },
  {
    "name": "storagent.etcd.reconcile",
    "display_name": "Etcd 全量校准",
    "trigger": "周期调度",
    "schedule_setting": "SYNC_RECONCILE_INTERVAL_SECONDS",
    "execution_scope": "本区控制面与共享 Etcd",
    "description": "将本区 Mongo 元数据与 Etcd 控制面进行一次有界校准。",
  },
  {
    "name": "storagent.replication.reconcile_policies",
    "display_name": "复制策略校准",
    "trigger": "周期调度",
    "schedule_setting": "REPLICATION_RECONCILE_INTERVAL_SECONDS",
    "execution_scope": "权威区域",
    "description": "仅权威区域补齐已启用应用的复制规则和桶配额。",
  },
  {
    "name": "storagent.public.refresh_quota_aggregates",
    "display_name": "应用配额聚合刷新",
    "trigger": "周期调度",
    "schedule_setting": "APPLICATION_QUOTA_AGGREGATE_INTERVAL_SECONDS",
    "execution_scope": "权威区域",
    "description": "仅权威区域刷新应用用量聚合并回写 Etcd 配额数据。",
  },
  {
    "name": "storagent.capacity.snapshot",
    "display_name": "容量快照采集",
    "trigger": "周期调度",
    "schedule_setting": "CAPACITY_SNAPSHOT_INTERVAL_SECONDS",
    "execution_scope": "权威区域",
    "description": "仅权威区域采集跨区域容量与复制冗余快照。",
  },
  {
    "name": "storagent.storage.monitor_cluster_health",
    "display_name": "MinIO 集群自愈巡检",
    "trigger": "周期调度",
    "schedule_setting": "CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS",
    "execution_scope": "权威区域",
    "description": "仅权威区域且启用自动自愈时检查 MinIO 原生 healing 状态。",
  },
  {
    "name": "storagent.etcd.execute",
    "display_name": "Etcd 运维任务执行",
    "trigger": "Etcd 运维页面创建任务",
    "execution_scope": "创建任务的本区 Mongo",
    "description": "执行已持久化的 Etcd 检查、压缩、碎片整理或告警解除任务。",
  },
  {
    "name": "storagent.storage.execute_operation",
    "display_name": "存储运维任务执行",
    "trigger": "存储运维页面创建任务",
    "execution_scope": "创建任务的本区 Mongo 与 MinIO",
    "description": "执行复制校准、resync、集群自愈或未受管桶删除等持久化任务。",
  },
  {
    "name": "storagent.audit.persist",
    "display_name": "审计事件落库",
    "trigger": "管理操作与业务审计调用",
    "execution_scope": "产生事件的本区 Mongo",
    "description": "异步写入本区审计事件；Celery 不可用时调用方会回退为同步落库。",
  },
  {
    "name": "storagent.maintenance.recover_queued_tasks",
    "display_name": "运维任务状态看门狗",
    "trigger": "周期调度",
    "schedule_setting": "CELERY_OPERATION_WATCHDOG_INTERVAL_SECONDS",
    "execution_scope": "本区持久化运维任务",
    "description": "将长期未启动或运行超时的存储、Etcd 运维任务标记为需要人工复核。",
  },
)

_overview_cache_lock = asyncio.Lock()
_overview_cache_value: schema.CeleryOverviewResponse | None = None
_overview_cache_at = 0.0

_SENSITIVE_VALUE_RE = re.compile(
  r"(?i)(api[_-]?key|access[_-]?key|secret|token|password|authorization)"
  r"([=:]\s*)([^\s,;&]+)",
)
_CREDENTIAL_URL_RE = re.compile(r"(?i)((?:mongodb(?:\+srv)?|https?)://)[^/@\s]+@")
_QUERY_SECRET_RE = re.compile(
  r"(?i)([?&](?:api[_-]?key|access[_-]?key|secret|token|password)=)[^&#\s]+",
)


def _database_name(url: str, fallback: str) -> str:
  try:
    path = unquote(urlparse(url).path or "").strip("/")
  except Exception:
    path = ""
  return path.split("/", 1)[0] or fallback


def _as_datetime(value: Any) -> datetime | None:
  if value is None:
    return None
  if isinstance(value, datetime):
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
  if isinstance(value, (int, float)):
    try:
      return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
      return None
  if isinstance(value, str):
    try:
      parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
      return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
      return None
  return None


def _as_int(value: Any, default: int = 0) -> int:
  try:
    return int(value)
  except (TypeError, ValueError):
    return default


def _expected_queue() -> str:
  try:
    return task_queue_name(
      settings.REGION,
      queue_prefix=settings.CELERY_TASK_QUEUE_PREFIX,
      protocol_version=settings.CELERY_TASK_PROTOCOL_VERSION,
    )
  except ValueError:
    return ""


def _redact(value: Any, limit: int = 600) -> str:
  if value is None:
    return ""
  if isinstance(value, bytes):
    value = value.decode("utf-8", errors="replace")
  text = " ".join(str(value).split())
  text = _CREDENTIAL_URL_RE.sub(r"\1***@", text)
  text = _QUERY_SECRET_RE.sub(r"\1***", text)
  text = _SENSITIVE_VALUE_RE.sub(r"\1\2***", text)
  return text[:limit] + ("..." if len(text) > limit else "")


def _summary(value: Any, limit: int = 500) -> str:
  if value is None:
    return ""
  if isinstance(value, (dict, list, tuple)):
    try:
      text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
      text = str(value)
  else:
    text = str(value)
  return _redact(text, limit=limit)


def _error_summary(value: Any) -> str:
  text = _summary(value, limit=600)
  if not text:
    return ""
  lines = [line.strip() for line in text.splitlines() if line.strip()]
  return lines[-1] if lines else text


def clear_overview_cache() -> None:
  """Clear the short runtime-inspection cache for tests and diagnostics."""
  global _overview_cache_value, _overview_cache_at
  _overview_cache_value = None
  _overview_cache_at = 0.0


def task_catalog() -> list[schema.CeleryTaskCatalogItem]:
  rows = []
  for item in TASK_CATALOG:
    setting = item.get("schedule_setting")
    schedule_value = getattr(settings, setting, None) if setting else None
    rows.append(schema.CeleryTaskCatalogItem(
      name=str(item["name"]),
      display_name=str(item["display_name"]),
      trigger=str(item["trigger"]),
      schedule_seconds=_as_int(schedule_value, 0) if schedule_value is not None else None,
      execution_scope=str(item["execution_scope"]),
      description=str(item["description"]),
    ))
  return rows


def _inspect_runtime_sync() -> tuple[dict[str, Any], list[str]]:
  """Celery inspect uses blocking broker control calls; keep it off the event loop."""
  timeout = max(float(settings.CELERY_RUNTIME_TIMEOUT_SECONDS), 0.5)
  values: dict[str, Any] = {}
  errors: list[str] = []

  def inspect_one(key: str) -> tuple[str, Any, str]:
    try:
      inspector = celery_app.control.inspect(timeout=timeout)
      return key, getattr(inspector, key)() or {}, ""
    except Exception as error:
      return key, {}, f"{key}: {type(error).__name__}"

  # MongoDB transport completes each inspect broadcast independently. Running
  # these read-only calls in parallel avoids a page load waiting once per
  # control command when a worker is slow or temporarily unreachable.
  keys = ("ping", "stats", "active", "reserved", "scheduled", "registered")
  with ThreadPoolExecutor(max_workers=len(keys)) as executor:
    futures = [executor.submit(inspect_one, key) for key in keys]
    for future in as_completed(futures):
      key, value, error = future.result()
      values[key] = value
      if error:
        errors.append(error)
  return values, errors


async def _inspect_runtime() -> tuple[dict[str, Any], list[str]]:
  timeout = max(float(settings.CELERY_RUNTIME_TIMEOUT_SECONDS), 0.5)
  try:
    return await asyncio.wait_for(
      asyncio.to_thread(_inspect_runtime_sync),
      timeout=timeout * 3 + 2,
    )
  except asyncio.TimeoutError:
    return {}, ["inspect: timeout"]


def _task_from_runtime(
  raw: Any,
  *,
  worker: str,
  status: str,
  scheduled: bool = False,
) -> schema.CeleryTaskExecution | None:
  request = raw.get("request", {}) if scheduled and isinstance(raw, dict) else raw
  if not isinstance(request, dict):
    return None
  task_id = str(request.get("id") or "")
  name = str(request.get("name") or "")
  if not task_id and not name:
    return None
  delivery = request.get("delivery_info") if isinstance(request.get("delivery_info"), dict) else {}
  headers = request.get("headers") if isinstance(request.get("headers"), dict) else {}
  return schema.CeleryTaskExecution(
    id=task_id or "-",
    name=name or "未知任务",
    status=status,
    worker=worker,
    queue=str(delivery.get("routing_key") or "celery"),
    origin_region=str(headers.get("storagent-origin-region") or ""),
    task_protocol=str(headers.get("storagent-task-protocol") or ""),
    retries=_as_int(request.get("retries")),
    received_at=_as_datetime(request.get("time_start")),
    started_at=_as_datetime(request.get("time_start")),
    eta=_as_datetime(raw.get("eta")) if scheduled and isinstance(raw, dict) else None,
    source="runtime",
  )


async def _load_persistence() -> dict[str, Any]:
  """Read queue depth, heartbeat records, and result history directly from MongoDB."""
  broker = broker_url()
  result = result_backend()
  broker_db_name = _database_name(broker, settings.CELERY_MONGODB_DATABASE)
  result_db_name = (
    settings.CELERY_MONGODB_RESULT_DATABASE.strip()
    or _database_name(result, settings.CELERY_MONGODB_DATABASE)
  )
  timeout_ms = max(int(float(settings.CELERY_RUNTIME_TIMEOUT_SECONDS) * 1000), 500)
  broker_client = AsyncIOMotorClient(
    broker,
    serverSelectionTimeoutMS=timeout_ms,
    connectTimeoutMS=timeout_ms,
  )
  result_client = broker_client
  if result != broker:
    result_client = AsyncIOMotorClient(
      result,
      serverSelectionTimeoutMS=timeout_ms,
      connectTimeoutMS=timeout_ms,
    )
  try:
    await broker_client.admin.command("ping")
    if result_client is not broker_client:
      await result_client.admin.command("ping")
    broker_db = broker_client[broker_db_name]
    result_db = result_client[result_db_name]
    pending_rows = await broker_db[settings.CELERY_MONGODB_MESSAGES_COLLECTION].aggregate([
      {"$group": {"_id": "$queue", "count": {"$sum": 1}}},
    ]).to_list(length=None)
    routing_rows = await broker_db[settings.CELERY_MONGODB_ROUTING_COLLECTION].find(
      {}, {"_id": 0, "queue": 1, "exchange": 1, "routing_key": 1},
    ).to_list(length=None)
    queue_rows = await broker_db[settings.CELERY_MONGODB_QUEUES_COLLECTION].find(
      {}, {"_id": 1},
    ).to_list(length=None)
    heartbeat_rows = await result_db[settings.CELERY_WORKER_HEARTBEAT_COLLECTION].find(
      {},
      {
        "_id": 0,
        "worker": 1,
        "hostname": 1,
        "region": 1,
        "status": 1,
        "last_seen": 1,
        "started_at": 1,
        "concurrency": 1,
        "queue": 1,
        "task_protocol": 1,
        "beat_enabled": 1,
      },
    ).to_list(length=500)
    beat_lock_rows = await broker_db[settings.CELERY_BEAT_LOCK_COLLECTION].find(
      {},
      {"_id": 0, "key": 1, "owner": 1, "expires_at": 1, "updated_at": 1},
    ).to_list(length=100)
    return {
      "broker_database": broker_db_name,
      "pending_rows": pending_rows,
      "routing_rows": routing_rows,
      "queue_rows": queue_rows,
      "heartbeat_rows": heartbeat_rows,
      "beat_lock_rows": beat_lock_rows,
    }
  finally:
    broker_client.close()
    if result_client is not broker_client:
      result_client.close()


def _queue_statuses(
  pending_rows: list[dict[str, Any]],
  routing_rows: list[dict[str, Any]],
  queue_rows: list[dict[str, Any]],
  workers: list[schema.CeleryWorkerStatus],
) -> list[schema.CeleryQueueStatus]:
  pending = {str(row.get("_id") or "celery"): _as_int(row.get("count")) for row in pending_rows}
  routing: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for row in routing_rows:
    queue = str(row.get("queue") or "")
    if queue and not queue.endswith(".pidbox"):
      routing[queue].append(row)
  known = {*pending.keys(), *routing.keys()}
  if _expected_queue():
    known.add(_expected_queue())
  known.update(str(row.get("_id") or "") for row in queue_rows if str(row.get("_id") or "") and not str(row.get("_id")).endswith(".pidbox"))
  rows = []
  for name in sorted(known):
    if name.endswith(".pidbox"):
      continue
    routes = routing.get(name, [])
    rows.append(schema.CeleryQueueStatus(
      name=name,
      pending_count=pending.get(name, 0),
      exchange=str(routes[0].get("exchange") or "celery") if routes else "celery",
      routing_keys=sorted({str(item.get("routing_key") or name) for item in routes}) or [name],
      worker_count=sum(
        1
        for worker in workers
        if worker.status == "online" and worker.queue == name
      ),
    ))
  return rows


def _compatible_heartbeat_rows(
  rows: list[dict[str, Any]],
  *,
  online_workers: set[str] | None = None,
) -> list[dict[str, Any]]:
  """Keep only routable heartbeats and collapse replaced Worker instances.

  Heartbeats from the legacy shared queue predate the Region/protocol contract
  and cannot be attributed safely.  A normal container restart also changes
  the Worker hostname, leaving a stale record behind until its TTL expires.
  If a fresh compatible heartbeat exists for the same Region queue, it is the
  authoritative instance; otherwise retain the most recent record so a real
  outage remains visible to operators.
  """
  now = utc_now()
  stale_after = max(_as_int(settings.CELERY_WORKER_STALE_AFTER_SECONDS), 1)
  grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
  for original in rows:
    worker = str(original.get("worker") or "").strip()
    region = str(original.get("region") or "").strip().lower()
    queue = str(original.get("queue") or "").strip()
    protocol = str(original.get("task_protocol") or "").strip()
    if not worker or not region or not queue or not protocol:
      continue
    try:
      expected_queue = task_queue_name(
        region,
        queue_prefix=settings.CELERY_TASK_QUEUE_PREFIX,
        protocol_version=protocol,
      )
    except ValueError:
      continue
    if queue != expected_queue:
      continue
    grouped[(region, queue, protocol)].append({
      **original,
      "worker": worker,
      "region": region,
      "queue": queue,
      "task_protocol": protocol,
    })

  filtered: list[dict[str, Any]] = []
  for key in sorted(grouped):
    group = grouped[key]

    # A successful control inspect is stronger evidence than a heartbeat. It
    # identifies the exact current process after a fast container restart,
    # while the prior process can still look fresh for one heartbeat interval.
    inspected = [
      row for row in group
      if str(row.get("worker") or "") in (online_workers or set())
    ]
    if inspected:
      filtered.extend(inspected)
      continue

    def last_seen(row: dict[str, Any]) -> datetime:
      return _as_datetime(row.get("last_seen")) or datetime.min.replace(tzinfo=timezone.utc)

    current = [
      row for row in group
      if str(row.get("status") or "").lower() != "offline"
      and (now - last_seen(row)).total_seconds() <= stale_after
    ]
    if current:
      filtered.extend(current)
    else:
      filtered.append(max(group, key=last_seen))
  return sorted(filtered, key=lambda row: str(row.get("worker") or ""))


def _worker_statuses(
  runtime: dict[str, Any],
  heartbeat_rows: list[dict[str, Any]],
) -> tuple[list[schema.CeleryWorkerStatus], list[schema.CeleryTaskExecution], list[schema.CeleryTaskExecution], list[schema.CeleryTaskExecution]]:
  ping = runtime.get("ping") or {}
  stats = runtime.get("stats") or {}
  active = runtime.get("active") or {}
  reserved = runtime.get("reserved") or {}
  scheduled = runtime.get("scheduled") or {}
  registered = runtime.get("registered") or {}
  online_runtime_workers = {str(name) for name in set(ping) | set(stats)}
  heartbeat_by_worker = {
    str(row.get("worker") or ""): row
    for row in _compatible_heartbeat_rows(
      heartbeat_rows,
      online_workers=online_runtime_workers,
    )
    if str(row.get("worker") or "")
  }
  names = set(ping) | set(stats) | set(active) | set(reserved) | set(scheduled) | set(registered) | set(heartbeat_by_worker)
  now = utc_now()
  stale_after = max(_as_int(settings.CELERY_WORKER_STALE_AFTER_SECONDS), 1)
  workers = []
  active_tasks = []
  reserved_tasks = []
  scheduled_tasks = []
  for name in sorted(str(value) for value in names):
    heartbeat = heartbeat_by_worker.get(name, {})
    last_seen = _as_datetime(heartbeat.get("last_seen"))
    age = int(max((now - last_seen).total_seconds(), 0)) if last_seen else None
    online_from_inspect = name in ping or name in stats
    if online_from_inspect:
      status = "online"
    elif heartbeat.get("status") == "offline":
      status = "offline"
    elif age is not None and age <= stale_after:
      status = "online"
    elif age is not None:
      status = "stale"
    else:
      status = "unknown"
    worker_stats = stats.get(name) if isinstance(stats.get(name), dict) else {}
    pool = worker_stats.get("pool") if isinstance(worker_stats.get("pool"), dict) else {}
    total = worker_stats.get("total") if isinstance(worker_stats.get("total"), dict) else {}
    source = "both" if online_from_inspect and heartbeat else "inspect" if online_from_inspect else "heartbeat"
    workers.append(schema.CeleryWorkerStatus(
      name=name,
      hostname=str(heartbeat.get("hostname") or name.split("@", 1)[-1]),
      region=str(heartbeat.get("region") or ""),
      status=status,
      last_seen=last_seen,
      heartbeat_age_seconds=age,
      active_count=len(active.get(name) or []),
      reserved_count=len(reserved.get(name) or []),
      scheduled_count=len(scheduled.get(name) or []),
      processed_count=sum(_as_int(value) for value in total.values()),
      concurrency=_as_int(pool.get("max-concurrency")) or _as_int(heartbeat.get("concurrency")) or None,
      registered_task_count=len(registered.get(name) or []),
      queue=str(heartbeat.get("queue") or ""),
      task_protocol=str(heartbeat.get("task_protocol") or ""),
      beat_enabled=bool(heartbeat.get("beat_enabled", False)),
      source=source,
    ))
    for item in active.get(name) or []:
      task = _task_from_runtime(item, worker=name, status="STARTED")
      if task:
        active_tasks.append(task)
    for item in reserved.get(name) or []:
      task = _task_from_runtime(item, worker=name, status="RESERVED")
      if task:
        reserved_tasks.append(task)
    for item in scheduled.get(name) or []:
      task = _task_from_runtime(item, worker=name, status="SCHEDULED", scheduled=True)
      if task:
        scheduled_tasks.append(task)
  return workers, active_tasks, reserved_tasks, scheduled_tasks


def _beat_leaders(rows: list[dict[str, Any]]) -> list[schema.CeleryBeatLeader]:
  now = utc_now()
  result = []
  for row in rows:
    key = str(row.get("key") or "")
    if not key.startswith("storagent-beat:"):
      continue
    expires_at = _as_datetime(row.get("expires_at"))
    result.append(schema.CeleryBeatLeader(
      key=key,
      owner=str(row.get("owner") or ""),
      expires_at=expires_at,
      updated_at=_as_datetime(row.get("updated_at")),
      active=bool(expires_at and expires_at > now),
    ))
  return sorted(result, key=lambda item: item.key)


async def _build_overview() -> schema.CeleryOverviewResponse:
  generated_at = utc_now()
  catalog = task_catalog()
  expected_queue = _expected_queue()
  if not settings.CELERY_ENABLED:
    return schema.CeleryOverviewResponse(
      generated_at=generated_at,
      broker=schema.CeleryBrokerStatus(
        enabled=False,
        reachable=False,
        message="当前节点未启用 Celery",
        region=settings.REGION,
        expected_queue=expected_queue,
        task_protocol=str(settings.CELERY_TASK_PROTOCOL_VERSION),
      ),
      task_catalog=catalog,
      inspection_message="当前节点未启用 Celery，未执行运行时探测。",
    )

  persistence: dict[str, Any] = {}
  persistence_error = ""
  try:
    persistence = await _load_persistence()
  except Exception as error:
    logger.warning("Celery 运维读取 MongoDB 失败: %s", type(error).__name__)
    persistence_error = f"MongoDB 读取失败: {type(error).__name__}"
  runtime, inspect_errors = await _inspect_runtime()
  workers, active_tasks, reserved_tasks, scheduled_tasks = _worker_statuses(
    runtime,
    list(persistence.get("heartbeat_rows") or []),
  )
  queues = _queue_statuses(
    list(persistence.get("pending_rows") or []),
    list(persistence.get("routing_rows") or []),
    list(persistence.get("queue_rows") or []),
    workers,
  )
  messages = [item for item in (persistence_error, *inspect_errors) if item]
  return schema.CeleryOverviewResponse(
    generated_at=generated_at,
    broker=schema.CeleryBrokerStatus(
      enabled=True,
      reachable=not bool(persistence_error),
      database=str(persistence.get("broker_database") or ""),
      message=persistence_error,
      region=settings.REGION,
      expected_queue=expected_queue,
      task_protocol=str(settings.CELERY_TASK_PROTOCOL_VERSION),
    ),
    workers=workers,
    queues=queues,
    active_tasks=active_tasks,
    reserved_tasks=reserved_tasks,
    scheduled_tasks=scheduled_tasks,
    task_catalog=catalog,
    beat_leaders=_beat_leaders(list(persistence.get("beat_lock_rows") or [])),
    inspection_message="；".join(messages),
  )


async def get_overview() -> schema.CeleryOverviewResponse:
  """Return a short-lived shared runtime snapshot for the operations page."""
  global _overview_cache_value, _overview_cache_at
  if not settings.CELERY_ENABLED:
    return await _build_overview()
  ttl = max(float(settings.CELERY_OVERVIEW_CACHE_SECONDS), 0.0)
  now = time.monotonic()
  if _overview_cache_value is not None and now - _overview_cache_at < ttl:
    return _overview_cache_value
  async with _overview_cache_lock:
    now = time.monotonic()
    if _overview_cache_value is not None and now - _overview_cache_at < ttl:
      return _overview_cache_value
    overview = await _build_overview()
    _overview_cache_value = overview
    _overview_cache_at = now
    return overview


def _history_item(row: dict[str, Any]) -> schema.CeleryTaskExecution:
  has_safe_summary = int(row.get("result_summary_version") or 0) >= 2
  has_safe_error = int(row.get("error_summary_version") or 0) >= 2
  return schema.CeleryTaskExecution(
    id=str(row.get("task_id") or row.get("_id") or "-"),
    name=str(row.get("task_name") or "未记录任务名"),
    status=str(row.get("status") or "UNKNOWN"),
    worker=str(row.get("worker") or ""),
    region=str(row.get("region") or ""),
    queue=str(row.get("queue") or "celery"),
    origin_region=str(row.get("origin_region") or ""),
    task_protocol=str(row.get("task_protocol") or ""),
    retries=_as_int(row.get("retries")),
    received_at=_as_datetime(row.get("received_at")),
    started_at=_as_datetime(row.get("started_at")),
    finished_at=_as_datetime(row.get("finished_at") or row.get("date_done")),
    duration_ms=_as_int(row.get("duration_ms")) or None,
    result_summary=(
      _summary(row.get("result_summary"))
      if has_safe_summary else (
        "历史记录未暴露任务返回内容"
        if row.get("result") is not None or row.get("result_summary") is not None else ""
      )
    ),
    error=(
      _error_summary(row.get("error"))
      if has_safe_error else (
        "历史记录已隐藏未脱敏的失败详情"
        if row.get("error") is not None or row.get("traceback") is not None else ""
      )
    ),
    source=str(row.get("source") or "history"),
  )


async def get_history(limit: int = 50) -> schema.CeleryHistoryResponse:
  generated_at = utc_now()
  if not settings.CELERY_ENABLED:
    return schema.CeleryHistoryResponse(
      generated_at=generated_at,
      available=False,
      message="当前节点未启用 Celery。",
    )
  result = result_backend()
  result_db_name = (
    settings.CELERY_MONGODB_RESULT_DATABASE.strip()
    or _database_name(result, settings.CELERY_MONGODB_DATABASE)
  )
  timeout_ms = max(int(float(settings.CELERY_RUNTIME_TIMEOUT_SECONDS) * 1000), 500)
  client = AsyncIOMotorClient(result, serverSelectionTimeoutMS=timeout_ms, connectTimeoutMS=timeout_ms)
  try:
    await client.admin.command("ping")
    database = client[result_db_name]
    history_rows = await database[settings.CELERY_TASK_HISTORY_COLLECTION].find(
      {},
      {
        "task_id": 1, "task_name": 1, "status": 1, "worker": 1,
        "region": 1, "queue": 1, "retries": 1, "received_at": 1,
        "started_at": 1, "finished_at": 1, "duration_ms": 1,
        "result_summary": 1, "result_summary_version": 1,
        "error": 1, "error_summary_version": 1,
        "origin_region": 1, "task_protocol": 1,
      },
    ).sort("updated_at", -1).limit(limit).to_list(length=limit)
    rows = [_history_item({**row, "source": "history"}) for row in history_rows]
    seen_ids = {item.id for item in rows}
    remaining = max(limit - len(rows), 0)
    legacy_count = 0
    if remaining:
      legacy_rows = await database[settings.CELERY_MONGODB_RESULT_COLLECTION].find(
        {}, {"status": 1, "result": 1, "traceback": 1, "date_done": 1},
      ).sort("date_done", -1).limit(limit + len(seen_ids)).to_list(length=limit + len(seen_ids))
      for row in legacy_rows:
        task_id = str(row.get("_id") or "")
        if task_id in seen_ids:
          continue
        rows.append(_history_item({**row, "source": "legacy"}))
        legacy_count += 1
        if len(rows) >= limit:
          break
    rows.sort(
      key=lambda item: item.finished_at or item.started_at or item.received_at or datetime.min.replace(tzinfo=timezone.utc),
      reverse=True,
    )
    return schema.CeleryHistoryResponse(
      generated_at=generated_at,
      available=True,
      data=rows[:limit],
      legacy_record_count=legacy_count,
      message=(
        "旧版 Celery 结果仅保存状态和结果，不保存任务名、区域或 worker；"
        "新任务会在独立历史记录中提供完整执行信息。"
        if legacy_count else ""
      ),
    )
  except Exception as error:
    logger.warning("Celery 历史读取失败: %s", type(error).__name__)
    return schema.CeleryHistoryResponse(
      generated_at=generated_at,
      available=False,
      message=f"历史记录读取失败: {type(error).__name__}",
    )
  finally:
    client.close()
