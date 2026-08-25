"""Read Celery runtime state without exposing broker credentials or task arguments."""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote, urlparse

from motor.motor_asyncio import AsyncIOMotorClient

from src.configs.configs import settings
from src.core.celery_client import broker_url, celery_app, result_backend
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


def _summary(value: Any, limit: int = 500) -> str:
  if value is None:
    return ""
  if isinstance(value, bytes):
    value = value.decode("utf-8", errors="replace")
  if isinstance(value, (dict, list, tuple)):
    try:
      text = json.dumps(value, ensure_ascii=False, default=str, separators=(",", ":"))
    except (TypeError, ValueError):
      text = str(value)
  else:
    text = str(value)
  text = " ".join(text.split())
  return text[:limit] + ("..." if len(text) > limit else "")


def _error_summary(value: Any) -> str:
  text = _summary(value, limit=600)
  if not text:
    return ""
  lines = [line.strip() for line in text.splitlines() if line.strip()]
  return lines[-1] if lines else text


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
  return schema.CeleryTaskExecution(
    id=task_id or "-",
    name=name or "未知任务",
    status=status,
    worker=worker,
    queue=str(delivery.get("routing_key") or "celery"),
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
      },
    ).to_list(length=500)
    return {
      "broker_database": broker_db_name,
      "pending_rows": pending_rows,
      "routing_rows": routing_rows,
      "queue_rows": queue_rows,
      "heartbeat_rows": heartbeat_rows,
    }
  finally:
    broker_client.close()
    if result_client is not broker_client:
      result_client.close()


def _queue_statuses(
  pending_rows: list[dict[str, Any]],
  routing_rows: list[dict[str, Any]],
  queue_rows: list[dict[str, Any]],
  worker_count: int,
) -> list[schema.CeleryQueueStatus]:
  pending = {str(row.get("_id") or "celery"): _as_int(row.get("count")) for row in pending_rows}
  routing: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for row in routing_rows:
    queue = str(row.get("queue") or "")
    if queue and not queue.endswith(".pidbox"):
      routing[queue].append(row)
  known = {"celery", *pending.keys(), *routing.keys()}
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
      worker_count=worker_count,
    ))
  return rows


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
  heartbeat_by_worker = {
    str(row.get("worker") or ""): row
    for row in heartbeat_rows
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


async def get_overview() -> schema.CeleryOverviewResponse:
  generated_at = utc_now()
  catalog = task_catalog()
  if not settings.CELERY_ENABLED:
    return schema.CeleryOverviewResponse(
      generated_at=generated_at,
      broker=schema.CeleryBrokerStatus(enabled=False, reachable=False, message="当前节点未启用 Celery"),
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
    sum(1 for worker in workers if worker.status == "online"),
  )
  messages = [item for item in (persistence_error, *inspect_errors) if item]
  return schema.CeleryOverviewResponse(
    generated_at=generated_at,
    broker=schema.CeleryBrokerStatus(
      enabled=True,
      reachable=not bool(persistence_error),
      database=str(persistence.get("broker_database") or ""),
      message=persistence_error,
    ),
    workers=workers,
    queues=queues,
    active_tasks=active_tasks,
    reserved_tasks=reserved_tasks,
    scheduled_tasks=scheduled_tasks,
    task_catalog=catalog,
    inspection_message="；".join(messages),
  )


def _history_item(row: dict[str, Any]) -> schema.CeleryTaskExecution:
  return schema.CeleryTaskExecution(
    id=str(row.get("task_id") or row.get("_id") or "-"),
    name=str(row.get("task_name") or "未记录任务名"),
    status=str(row.get("status") or "UNKNOWN"),
    worker=str(row.get("worker") or ""),
    region=str(row.get("region") or ""),
    queue=str(row.get("queue") or "celery"),
    retries=_as_int(row.get("retries")),
    received_at=_as_datetime(row.get("received_at")),
    started_at=_as_datetime(row.get("started_at")),
    finished_at=_as_datetime(row.get("finished_at") or row.get("date_done")),
    duration_ms=_as_int(row.get("duration_ms")) or None,
    result_summary=_summary(row.get("result_summary") if "result_summary" in row else row.get("result")),
    error=_error_summary(row.get("error") if "error" in row else row.get("traceback")),
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
        "result_summary": 1, "error": 1,
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
