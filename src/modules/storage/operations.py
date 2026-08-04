"""Operational views and safe maintenance actions for managed MinIO sites."""
from __future__ import annotations

import asyncio
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from src.configs.configs import settings
from src.core import audit, metrics as metrics_mod, minio_op
from src.core.exception import CustomException, ErrorDesc
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.modules.storage.model import StorageOperation
from src.utils.helpers import utc_now
from src.utils.logger import logger


_ALIAS_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DURATION_RE = re.compile(r"^(?:[0-9]+(?:ms|s|m|h|d|w))+$")
_background_tasks: set[asyncio.Task] = set()


class StorageOperationLockBusy(RuntimeError):
  pass


@asynccontextmanager
async def _distributed_operation_lock(name: str):
  """Prevent the five regional backends from starting the same repair."""
  from src.core import etcd_op

  client = await etcd_op.get_etcd_client()
  ttl = max(int(settings.REPLICATION_LOCK_TTL_SECONDS), 30)
  lock = client.lock(f"/storagent/locks/storage-operation/{name}".encode(), ttl=ttl)
  acquired = False
  refresh_job = None

  async def refresh_lock():
    while True:
      await asyncio.sleep(max(ttl / 3, 5))
      await lock.refresh()

  try:
    acquired = await lock.acquire(timeout=0)
    if not acquired:
      raise StorageOperationLockBusy(f"{name} 已由其他区域执行")
    refresh_job = asyncio.create_task(refresh_lock())
    yield
  finally:
    if refresh_job is not None:
      refresh_job.cancel()
      try:
        await refresh_job
      except asyncio.CancelledError:
        pass
      except Exception as e:
        logger.warning(f"存储运维锁续租失败 {name}: {e}")
    if acquired:
      try:
        await lock.release()
      except Exception as e:
        logger.warning(f"存储运维锁释放失败 {name}: {e}")
    try:
      await client.close()
    except Exception:
      pass


def _as_int(value: Any) -> int:
  try:
    return int(value or 0)
  except (TypeError, ValueError):
    return 0


def _as_float(value: Any) -> float:
  try:
    return float(value or 0)
  except (TypeError, ValueError):
    return 0.0


def _ns_to_ms(value: Any) -> float:
  return _as_float(value) / 1_000_000


def _ns_to_seconds(value: Any) -> float:
  return _as_float(value) / 1_000_000_000


def _safe_datetime(value: Any) -> Any:
  if not value or str(value).startswith("0001-"):
    return None
  return value


def _validate_alias(value: str, field: str = "server") -> str:
  normalized = value.strip()
  if not _ALIAS_RE.fullmatch(normalized):
    raise CustomException(ErrorDesc.INVALID_PARAMS, f"{field} 格式不合法")
  return normalized


def _validate_bucket(value: str) -> str:
  # Keep the exact same bucket constraints used by topology management.
  from src.modules.storage.service import _validate_bucket_name
  return _validate_bucket_name(value)


def _endpoint_key(value: str) -> str:
  endpoint = str(value or "").strip().rstrip("/")
  endpoint = re.sub(r"^https?://", "", endpoint, flags=re.IGNORECASE)
  return endpoint.lower()


def _operation_dict(operation: StorageOperation | None) -> dict[str, Any] | None:
  if not operation:
    return None
  return {
    "id": str(operation.id),
    "kind": operation.kind,
    "status": operation.status,
    "server": operation.server,
    "bucket": operation.bucket,
    "actor": operation.actor,
    "message": operation.message,
    "result": operation.result,
    "created_at": operation.created_at,
    "started_at": operation.started_at,
    "finished_at": operation.finished_at,
  }


def _pool_totals(pools: Any) -> tuple[int, int, int]:
  capacity = 0
  used = 0
  healing = 0
  if not isinstance(pools, dict):
    return capacity, used, healing
  for pool in pools.values():
    if not isinstance(pool, dict):
      continue
    for item in pool.values():
      if not isinstance(item, dict):
        continue
      capacity += _as_int(item.get("rawCapacity"))
      used += _as_int(item.get("rawUsage"))
      healing += _as_int(item.get("healDisks"))
  return capacity, used, healing


def parse_cluster_admin_info(
  server: Any,
  payload: dict[str, Any],
  *,
  elapsed_ms: float,
  checked_at: datetime,
) -> dict[str, Any]:
  info = payload.get("info") if isinstance(payload.get("info"), dict) else payload
  backend = info.get("backend") if isinstance(info.get("backend"), dict) else {}
  server_entries = info.get("servers") if isinstance(info.get("servers"), list) else []
  drives: list[dict[str, Any]] = []
  versions: list[str] = []
  uptime = 0
  for entry in server_entries:
    if not isinstance(entry, dict):
      continue
    uptime = max(uptime, _as_int(entry.get("uptime")))
    if entry.get("version"):
      versions.append(str(entry["version"]))
    for drive in entry.get("drives") or []:
      if not isinstance(drive, dict):
        continue
      metrics = drive.get("metrics") if isinstance(drive.get("metrics"), dict) else {}
      drives.append({
        "endpoint": str(drive.get("endpoint") or ""),
        "path": str(drive.get("path") or ""),
        "state": str(drive.get("state") or "unknown"),
        "total_bytes": _as_int(drive.get("totalspace")),
        "used_bytes": _as_int(drive.get("usedspace")),
        "available_bytes": _as_int(drive.get("availspace")),
        "waiting_operations": _as_int(metrics.get("totalWaiting")),
      })

  raw_capacity, raw_used, healing_disks = _pool_totals(info.get("pools"))
  if raw_capacity <= 0:
    raw_capacity = sum(item["total_bytes"] for item in drives)
  if raw_used <= 0:
    raw_used = sum(item["used_bytes"] for item in drives)
  online_disks = _as_int(backend.get("onlineDisks"))
  offline_disks = _as_int(backend.get("offlineDisks"))
  if not online_disks and drives:
    online_disks = sum(item["state"].lower() in ("ok", "online") for item in drives)
  if not offline_disks and drives:
    offline_disks = sum(item["state"].lower() not in ("ok", "online") for item in drives)

  mode = str(info.get("mode") or "").lower()
  degraded = (
    mode != "online"
    or offline_disks > 0
    or healing_disks > 0
    or any(item["state"].lower() not in ("ok", "online") for item in drives)
  )
  region = getattr(server, "region", None)
  usage = info.get("usage") if isinstance(info.get("usage"), dict) else {}
  buckets = info.get("buckets") if isinstance(info.get("buckets"), dict) else {}
  objects = info.get("objects") if isinstance(info.get("objects"), dict) else {}
  versions_info = info.get("versions") if isinstance(info.get("versions"), dict) else {}
  markers = info.get("deletemarkers") if isinstance(info.get("deletemarkers"), dict) else {}
  return {
    "id": str(server.id),
    "server": server.name,
    "region": str(getattr(region, "name", server.name)),
    "shown_name": str(getattr(region, "shown_name", server.name)),
    "endpoint": f"{server.host}:{server.minio_port}",
    "status": "degraded" if degraded else "online",
    "reachable": True,
    "error": "",
    "checked_at": checked_at,
    "command_latency_ms": elapsed_ms,
    "version": ", ".join(sorted(set(versions))),
    "uptime_seconds": uptime,
    "bucket_count": _as_int(buckets.get("count")),
    "object_count": _as_int(objects.get("count")),
    "version_count": _as_int(versions_info.get("count")),
    "delete_marker_count": _as_int(markers.get("count")),
    "logical_usage_bytes": _as_int(usage.get("size")),
    "raw_capacity_bytes": raw_capacity,
    "raw_used_bytes": raw_used,
    "online_disks": online_disks,
    "offline_disks": offline_disks,
    "healing_disks": healing_disks,
    "drives": drives,
  }


async def get_cluster_health_overview() -> dict[str, Any]:
  checked_at = utc_now()
  servers = await storage_crud.read_minio_server_list()
  semaphore = asyncio.Semaphore(5)

  async def inspect(server: Any) -> dict[str, Any]:
    async with semaphore:
      success, payload, error, elapsed_ms = await minio_op.get_cluster_admin_info(
        server.name,
        timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
      )
    if success:
      return parse_cluster_admin_info(
        server,
        payload,
        elapsed_ms=elapsed_ms,
        checked_at=checked_at,
      )
    region = getattr(server, "region", None)
    return {
      "id": str(server.id),
      "server": server.name,
      "region": str(getattr(region, "name", server.name)),
      "shown_name": str(getattr(region, "shown_name", server.name)),
      "endpoint": f"{server.host}:{server.minio_port}",
      "status": "offline",
      "reachable": False,
      "error": error,
      "checked_at": checked_at,
      "command_latency_ms": elapsed_ms,
    }

  clusters = await asyncio.gather(*(inspect(server) for server in servers))
  online = sum(item["status"] == "online" for item in clusters)
  degraded = sum(item["status"] == "degraded" for item in clusters)
  offline = sum(item["status"] == "offline" for item in clusters)
  overall = "offline" if not clusters or offline == len(clusters) else (
    "degraded" if offline or degraded else "online"
  )
  summary = {
    "status": overall,
    "cluster_count": len(clusters),
    "online_clusters": online,
    "degraded_clusters": degraded,
    "offline_clusters": offline,
    "online_disks": sum(_as_int(item.get("online_disks")) for item in clusters),
    "offline_disks": sum(_as_int(item.get("offline_disks")) for item in clusters),
    "healing_disks": sum(_as_int(item.get("healing_disks")) for item in clusters),
    "raw_capacity_bytes": sum(_as_int(item.get("raw_capacity_bytes")) for item in clusters),
    "raw_used_bytes": sum(_as_int(item.get("raw_used_bytes")) for item in clusters),
    "logical_usage_bytes": sum(_as_int(item.get("logical_usage_bytes")) for item in clusters),
    "object_count": sum(_as_int(item.get("object_count")) for item in clusters),
  }
  return {
    "generated_at": checked_at,
    "auto_heal_enabled": settings.AUTO_HEAL_ENABLED,
    "auto_heal_authority_region": settings.SYNC_AUTHORITY_REGION,
    "summary": summary,
    "clusters": clusters,
  }


def _rate_by_arn(replication_stats: dict[str, Any]) -> dict[str, float]:
  result: dict[str, float] = {}
  queue_stats = replication_stats.get("queueStats")
  if not isinstance(queue_stats, dict):
    return result
  for node in queue_stats.get("nodes") or []:
    if not isinstance(node, dict):
      continue
    for arn, rate_info in (node.get("tgtTransferStats") or {}).items():
      if not isinstance(rate_info, dict):
        continue
      total = rate_info.get("Total") if isinstance(rate_info.get("Total"), dict) else {}
      result[str(arn)] = result.get(str(arn), 0.0) + _as_float(total.get("currRate"))
  return result


def parse_replication_resync_status(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
  """Normalize mc resync status entries and index them by remote ARN."""
  resync_info = payload.get("resyncInfo")
  if not isinstance(resync_info, dict):
    return {}
  targets = resync_info.get("target")
  if not isinstance(targets, list):
    return {}

  status_map = {
    "ongoing": "running",
    "running": "running",
    "pending": "running",
    "started": "running",
    "starting": "running",
    "completed": "completed",
    "complete": "completed",
    "failed": "failed",
    "cancelled": "failed",
    "canceled": "failed",
  }
  result: dict[str, dict[str, Any]] = {}
  for item in targets:
    if not isinstance(item, dict):
      continue
    arn = str(item.get("arn") or "")
    if not arn:
      continue
    raw_status = str(item.get("resyncStatus") or "").strip().lower()
    result[arn] = {
      "resync_status": status_map.get(raw_status, "unknown"),
      "resync_reset_id": str(item.get("resetid") or item.get("resetID") or ""),
      "resync_started_at": _safe_datetime(item.get("startTime")),
      "resync_updated_at": _safe_datetime(item.get("endTime")),
      "resync_completed_bytes": _as_int(item.get("completedReplicationSize")),
      "resync_object_count": _as_int(item.get("replicationCount")),
      "resync_current_object": str(item.get("object") or ""),
    }
  return result


def parse_replication_source(
  source: str,
  payload: dict[str, Any],
  *,
  server_names: list[str],
  endpoints: dict[str, str],
  elapsed_ms: float,
  resync_by_arn: dict[str, dict[str, Any]] | None = None,
  resync_status_known: bool = True,
) -> dict[str, Any]:
  replication_stats = payload.get("replicationstats")
  if not isinstance(replication_stats, dict):
    replication_stats = {}
  current = replication_stats.get("currStats")
  if not isinstance(current, dict):
    current = {}
  queue = current.get("queued") if isinstance(current.get("queued"), dict) else {}
  queue_current = queue.get("curr") if isinstance(queue.get("curr"), dict) else {}
  failed = current.get("failed") if isinstance(current.get("failed"), dict) else {}
  failed_totals = failed.get("totals") if isinstance(failed.get("totals"), dict) else {}
  stats_by_arn = current.get("Stats") if isinstance(current.get("Stats"), dict) else {}
  rates = _rate_by_arn(replication_stats)
  resync_by_arn = resync_by_arn or {}

  mrf_failed = 0
  retries_total = 0
  queue_stats = replication_stats.get("queueStats")
  if not isinstance(queue_stats, dict):
    queue_stats = {}
  for node in queue_stats.get("nodes") or []:
    if not isinstance(node, dict):
      continue
    mrf = node.get("mrfStats") if isinstance(node.get("mrfStats"), dict) else {}
    retries = node.get("retries") if isinstance(node.get("retries"), dict) else {}
    mrf_failed += _as_int(mrf.get("failedCount_last5min"))
    retries_total += _as_int(retries.get("total"))

  targets: list[dict[str, Any]] = []
  found_targets: set[str] = set()
  for target in payload.get("remoteTargets") or []:
    if not isinstance(target, dict):
      continue
    arn = str(target.get("arn") or "")
    endpoint = _endpoint_key(target.get("endpoint") or "")
    target_name = endpoints.get(endpoint, endpoint or "unknown")
    found_targets.add(target_name)
    target_stats = stats_by_arn.get(arn) if isinstance(stats_by_arn.get(arn), dict) else {}
    target_failed = target_stats.get("failed") if isinstance(target_stats.get("failed"), dict) else {}
    target_failed_totals = (
      target_failed.get("totals") if isinstance(target_failed.get("totals"), dict) else {}
    )
    online = bool(target.get("isOnline"))
    failed_count = _as_int(target_failed_totals.get("count"))
    status = "critical" if not online else (
      "degraded" if failed_count else ("syncing" if rates.get(arn, 0) > 0 else "healthy")
    )
    latency = target.get("latency") if isinstance(target.get("latency"), dict) else {}
    resync = resync_by_arn.get(arn) or {
      "resync_status": "idle" if resync_status_known else "unknown",
    }
    targets.append({
      "source": source,
      "target": target_name,
      "arn": arn,
      "endpoint": endpoint,
      "status": status,
      "online": online,
      "latency_current_ms": _ns_to_ms(latency.get("curr")),
      "latency_average_ms": _ns_to_ms(latency.get("avg")),
      "latency_maximum_ms": _ns_to_ms(latency.get("max")),
      "total_downtime_seconds": _ns_to_seconds(target.get("totalDowntime")),
      "last_online": _safe_datetime(target.get("lastOnline")),
      "replication_count": _as_int(target_stats.get("replicationCount")),
      "completed_bytes": _as_int(target_stats.get("completedReplicationSize")),
      "failed_count": failed_count,
      "failed_bytes": _as_int(target_failed_totals.get("bytes")),
      "current_rate_bps": rates.get(arn, 0.0),
      **resync,
    })

  expected_targets = [name for name in server_names if name != source]
  for missing in expected_targets:
    if missing in found_targets:
      continue
    endpoint = next((key for key, name in endpoints.items() if name == missing), "")
    targets.append({
      "source": source,
      "target": missing,
      "arn": "",
      "endpoint": endpoint,
      "status": "critical",
      "online": False,
    })

  actual_target_count = len(payload.get("remoteTargets") or [])
  queued_count = _as_int(queue_current.get("count"))
  failed_count = _as_int(failed_totals.get("count"))
  current_rate = sum(rates.values())
  if any(item["status"] == "critical" for item in targets):
    status = "critical"
  elif actual_target_count != len(expected_targets) or failed_count or mrf_failed:
    status = "degraded"
  elif queued_count or current_rate > 0:
    status = "syncing"
  else:
    status = "healthy"
  return {
    "server": source,
    "status": status,
    "reachable": True,
    "command_latency_ms": elapsed_ms,
    "error": "",
    "queued_count": queued_count,
    "queued_bytes": _as_int(queue_current.get("bytes")),
    "failed_count": failed_count,
    "failed_bytes": _as_int(failed_totals.get("bytes")),
    "mrf_failed_last_5m": mrf_failed,
    "retries_total": retries_total,
    "current_rate_bps": current_rate,
    "expected_target_count": len(expected_targets),
    "actual_target_count": actual_target_count,
    "targets": sorted(targets, key=lambda item: item["target"]),
  }


def _worst_replication_status(statuses: list[str]) -> str:
  for status in ("unreachable", "critical", "degraded", "syncing", "healthy"):
    if status in statuses:
      return status
  return "healthy"


async def get_replication_overview(bucket: str | None = None) -> dict[str, Any]:
  servers = await storage_crud.read_minio_server_list()
  server_names = sorted(server.name for server in servers)
  endpoints = {
    _endpoint_key(f"{server.host}:{server.minio_port}"): server.name
    for server in servers
  }
  applications = await public_crud.read_application_list()
  app_names = {
    app.name: str(getattr(app, "shown_name", "") or "")
    for app in applications
    if getattr(app, "enabled", False)
  }
  if bucket:
    bucket_names = [_validate_bucket(bucket)]
  else:
    bucket_names = sorted(app_names)

  semaphore = asyncio.Semaphore(5)

  async def inspect(bucket_name: str, source: str) -> dict[str, Any]:
    async with semaphore:
      metrics_result, resync_result = await asyncio.gather(
        minio_op.get_bucket_replication_metrics(
          source,
          bucket_name,
          timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
        ),
        minio_op.get_bucket_replication_resync_status(
          source,
          bucket_name,
          timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
        ),
      )
    success, payload, error, elapsed_ms = metrics_result
    if not success:
      return {
        "server": source,
        "status": "unreachable",
        "reachable": False,
        "command_latency_ms": elapsed_ms,
        "error": error,
        "expected_target_count": max(len(server_names) - 1, 0),
        "actual_target_count": 0,
        "targets": [],
      }
    resync_success, resync_payload, _, _ = resync_result
    return parse_replication_source(
      source,
      payload,
      server_names=server_names,
      endpoints=endpoints,
      elapsed_ms=elapsed_ms,
      resync_by_arn=(
        parse_replication_resync_status(resync_payload)
        if resync_success
        else {}
      ),
      resync_status_known=resync_success,
    )

  tasks = {
    (bucket_name, source): asyncio.create_task(inspect(bucket_name, source))
    for bucket_name in bucket_names
    for source in server_names
  }
  buckets: list[dict[str, Any]] = []
  all_sources: list[dict[str, Any]] = []
  for bucket_name in bucket_names:
    sources = [await tasks[(bucket_name, source)] for source in server_names]
    all_sources.extend(sources)
    buckets.append({
      "bucket": bucket_name,
      "shown_name": app_names.get(bucket_name, ""),
      "status": _worst_replication_status([item["status"] for item in sources]),
      "sources": sources,
    })

  all_targets = [target for source in all_sources for target in source.get("targets", []) if target.get("arn")]
  expected_links = len(bucket_names) * len(server_names) * max(len(server_names) - 1, 0)
  summary = {
    "status": (
      "degraded"
      if not bucket_names or len(server_names) < 2
      else _worst_replication_status([item["status"] for item in all_sources])
    ),
    "bucket_count": len(bucket_names),
    "source_count": len(all_sources),
    "reachable_source_count": sum(item.get("reachable", False) for item in all_sources),
    "expected_link_count": expected_links,
    "actual_link_count": sum(_as_int(item.get("actual_target_count")) for item in all_sources),
    "online_link_count": sum(bool(item.get("online")) for item in all_targets),
    "queued_count": sum(_as_int(item.get("queued_count")) for item in all_sources),
    "queued_bytes": sum(_as_int(item.get("queued_bytes")) for item in all_sources),
    "failed_count": sum(_as_int(item.get("failed_count")) for item in all_sources),
    "failed_bytes": sum(_as_int(item.get("failed_bytes")) for item in all_sources),
    "mrf_failed_last_5m": sum(_as_int(item.get("mrf_failed_last_5m")) for item in all_sources),
    "current_rate_bps": sum(_as_float(item.get("current_rate_bps")) for item in all_sources),
  }
  return {
    "generated_at": utc_now(),
    "servers": server_names,
    "summary": summary,
    "buckets": buckets,
  }


async def reconcile_bucket_replication(bucket: str, actor: str) -> dict[str, Any]:
  from src.core import sync as sync_module

  bucket_name = _validate_bucket(bucket)
  server_names = await storage_crud.read_minio_server_names()
  try:
    async with sync_module.application_replication_lock(bucket_name):
      policy = await sync_module.setup_bucket_replication(bucket_name, server_names)
  except Exception as e:
    audit.audit(
      "replication.reconcile",
      actor=actor,
      resource=bucket_name,
      detail=str(e),
      success=False,
    )
    raise
  audit.audit("replication.reconcile", actor=actor, resource=bucket_name, detail=policy)
  return {
    "message": "复制规则校准完成",
    "bucket": bucket_name,
    "detail": policy,
  }


async def _read_running_resync(
  source: str,
  bucket: str,
  remote_arn: str,
) -> dict[str, Any] | None:
  success, payload, _, _ = await minio_op.get_bucket_replication_resync_status(
    source,
    bucket,
    remote_arn,
    timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
  )
  if not success:
    return None
  detail = parse_replication_resync_status(payload).get(remote_arn)
  if not detail or detail.get("resync_status") != "running":
    return None
  return detail


def _running_resync_response(
  bucket: str,
  source: str,
  target: str,
  detail: dict[str, Any],
) -> dict[str, Any]:
  return {
    "message": "对象补传任务正在运行",
    "bucket": bucket,
    "source_server": source,
    "target_server": target,
    "detail": {**detail, "already_running": True},
  }


async def start_replication_resync(
  bucket: str,
  source_server: str,
  target_server: str,
  older_than: str | None,
  actor: str,
) -> dict[str, Any]:
  bucket_name = _validate_bucket(bucket)
  source = _validate_alias(source_server, "source_server")
  target = _validate_alias(target_server, "target_server")
  if source == target:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "源站点与目标站点不能相同")
  if older_than:
    older_than = older_than.strip()
    if not _DURATION_RE.fullmatch(older_than):
      raise CustomException(ErrorDesc.INVALID_PARAMS, "older_than 应为 7d12h 等时长格式")

  servers = await storage_crud.read_minio_server_list()
  server_names = {server.name for server in servers}
  if source not in server_names or target not in server_names:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "源站点或目标站点不存在")
  endpoint_map = {
    _endpoint_key(f"{server.host}:{server.minio_port}"): server.name
    for server in servers
  }
  success, payload, error, _ = await minio_op.get_bucket_replication_metrics(
    source,
    bucket_name,
    timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
  )
  if not success:
    raise CustomException(ErrorDesc.MINIO_REPLICATE_FAILED, error)
  remote = next((
    item for item in payload.get("remoteTargets") or []
    if endpoint_map.get(_endpoint_key(item.get("endpoint") or "")) == target
  ), None)
  if not remote or not remote.get("arn"):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "该复制链路不存在，无法启动补传")

  remote_arn = str(remote["arn"])
  running = await _read_running_resync(source, bucket_name, remote_arn)
  if running:
    audit.audit(
      "replication.resync",
      actor=actor,
      resource=f"{bucket_name}:{source}->{target}",
      detail={**running, "already_running": True},
    )
    return _running_resync_response(bucket_name, source, target, running)

  success, detail, error, _ = await minio_op.start_bucket_replication_resync(
    source,
    bucket_name,
    remote_arn,
    older_than=older_than,
    timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
  )
  if not success:
    # MinIO rejects a second start while the first request is racing to become
    # visible. Re-read authoritative status and treat that case as idempotent.
    running = await _read_running_resync(source, bucket_name, remote_arn)
    if running:
      audit.audit(
        "replication.resync",
        actor=actor,
        resource=f"{bucket_name}:{source}->{target}",
        detail={**running, "already_running": True},
      )
      return _running_resync_response(bucket_name, source, target, running)
  audit.audit(
    "replication.resync",
    actor=actor,
    resource=f"{bucket_name}:{source}->{target}",
    detail=detail if success else error,
    success=success,
  )
  if not success:
    raise CustomException(ErrorDesc.MINIO_REPLICATE_FAILED, error)
  return {
    "message": "对象补传任务已启动",
    "bucket": bucket_name,
    "source_server": source,
    "target_server": target,
    "detail": detail,
  }


async def get_cluster_heal_status(server_name: str) -> dict[str, Any]:
  server = _validate_alias(server_name)
  if server not in set(await storage_crud.read_minio_server_names()):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  success, payload, error, _ = await minio_op.get_cluster_heal_info(
    server,
    timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
  )
  latest = await storage_crud.read_latest_storage_operation("cluster_heal", server)
  if not success:
    return {
      "server": server,
      "reachable": False,
      "status": "unreachable",
      "error": error,
      "checked_at": utc_now(),
      "latest_operation": _operation_dict(latest),
    }
  info = payload.get("HealInfo") if isinstance(payload.get("HealInfo"), dict) else {}
  heal_disks = info.get("HealDisks") if isinstance(info.get("HealDisks"), list) else []
  offline_nodes = info.get("offline_nodes") if isinstance(info.get("offline_nodes"), list) else []
  sets = []
  for item in info.get("sets") or []:
    if not isinstance(item, dict):
      continue
    sets.append({
      "id": str(item.get("id") or ""),
      "pool_index": _as_int(item.get("pool_index")),
      "set_index": _as_int(item.get("set_index")),
      "heal_status": str(item.get("heal_status") or ""),
      "heal_priority": str(item.get("heal_priority") or ""),
      "total_objects": _as_int(item.get("total_objects")),
    })
  return {
    "server": server,
    "reachable": True,
    "status": "healing" if heal_disks else "idle",
    "scanned_items": _as_int(info.get("ScannedItemsCount")),
    "offline_nodes": [str(item) for item in offline_nodes],
    "heal_disks": heal_disks,
    "sets": sets,
    "error": "",
    "checked_at": utc_now(),
    "latest_operation": _operation_dict(latest),
  }


def _spawn(coro: Any) -> None:
  task = asyncio.create_task(coro)
  _background_tasks.add(task)
  task.add_done_callback(_background_tasks.discard)


def _heal_result(items: list[dict[str, Any]]) -> dict[str, Any]:
  payload = items[-1] if items else {}
  info = payload.get("HealInfo") if isinstance(payload.get("HealInfo"), dict) else {}
  return {
    "scanned_items": _as_int(info.get("ScannedItemsCount")),
    "offline_nodes": info.get("offline_nodes") or [],
    "heal_disk_count": len(info.get("HealDisks") or []),
    "set_count": len(info.get("sets") or []),
  }


async def _run_heal_operation(operation: StorageOperation) -> None:
  try:
    async with _distributed_operation_lock(f"cluster-heal/{operation.server}"):
      operation.status = "running"
      operation.started_at = utc_now()
      operation.message = "正在采集 MinIO 原生自愈状态"
      await operation.save()
      success, items, error, elapsed_ms = await minio_op.inspect_cluster_heal(
        operation.server,
        timeout=settings.MINIO_HEAL_TIMEOUT_SECONDS,
      )
  except StorageOperationLockBusy as e:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = str(e)
    await operation.save()
    return
  except asyncio.CancelledError:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = "后端停止，自愈巡检状态已回收"
    await operation.save()
    raise
  except Exception as e:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = str(e)
    await operation.save()
    metrics_mod.incr("cluster_heal_failures_total")
    audit.audit(
      "cluster.heal",
      actor=operation.actor,
      resource=operation.server,
      detail=str(e),
      success=False,
    )
    return
  operation.finished_at = utc_now()
  operation.result = {**_heal_result(items), "elapsed_ms": elapsed_ms}
  if success:
    operation.status = "succeeded"
    operation.message = "自愈巡检完成"
  else:
    operation.status = "failed"
    operation.message = error
  await operation.save()
  metrics_mod.incr("cluster_heal_runs_total")
  if not success:
    metrics_mod.incr("cluster_heal_failures_total")
  audit.audit(
    "cluster.heal",
    actor=operation.actor,
    resource=operation.server,
    detail=operation.result if success else error,
    success=success,
  )


def _aware(value: datetime) -> datetime:
  return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


async def start_cluster_heal(server_name: str, actor: str) -> dict[str, Any]:
  server = _validate_alias(server_name)
  if server not in set(await storage_crud.read_minio_server_names()):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  active = await storage_crud.read_active_storage_operation("cluster_heal", server)
  if active:
    max_age = settings.MINIO_HEAL_TIMEOUT_SECONDS + 300
    if (utc_now() - _aware(active.created_at)).total_seconds() <= max_age:
      return _operation_dict(active) or {}
    active.status = "failed"
    active.message = "后端重启或任务超时，状态已回收"
    active.finished_at = utc_now()
    await active.save()
  operation = await storage_crud.create_storage_operation(
    kind="cluster_heal",
    server=server,
    actor=actor,
  )
  operation.message = "自愈巡检任务已进入队列"
  await operation.save()
  _spawn(_run_heal_operation(operation))
  return _operation_dict(operation) or {}


async def list_storage_operations(limit: int = 20) -> dict[str, Any]:
  operations = await storage_crud.list_storage_operations(min(max(limit, 1), 100))
  return {"data": [_operation_dict(item) for item in operations]}


async def monitor_cluster_health_task() -> None:
  """Authority-only loop that records native MinIO healing when drives need it."""
  if settings.REGION != settings.SYNC_AUTHORITY_REGION:
    logger.info("非权威区域不执行 MinIO 自动自愈监控")
    return
  if not settings.AUTO_HEAL_ENABLED:
    logger.info("MinIO 自动自愈监控已禁用")
    return
  interval = max(float(settings.CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS), 30.0)
  while True:
    try:
      overview = await get_cluster_health_overview()
      for cluster in overview["clusters"]:
        if (
          not cluster.get("reachable")
          or _as_int(cluster.get("offline_disks")) > 0
          or _as_int(cluster.get("healing_disks")) <= 0
        ):
          continue
        latest = await storage_crud.read_latest_storage_operation(
          "cluster_heal", cluster["server"]
        )
        if latest and (
          utc_now() - _aware(latest.created_at)
        ).total_seconds() < settings.AUTO_HEAL_COOLDOWN_SECONDS:
          continue
        await start_cluster_heal(cluster["server"], "system:auto-heal")
    except asyncio.CancelledError:
      logger.info("MinIO 自动自愈监控已停止")
      raise
    except Exception as e:
      metrics_mod.incr("cluster_health_monitor_failures_total")
      logger.warning(f"MinIO 自动自愈监控失败: {e}")
    await asyncio.sleep(interval)


async def shutdown_background_operations() -> None:
  for task in list(_background_tasks):
    task.cancel()
  if _background_tasks:
    await asyncio.gather(*list(_background_tasks), return_exceptions=True)
