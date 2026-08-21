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


def _as_bool(value: Any) -> bool:
  if isinstance(value, bool):
    return value
  if isinstance(value, str):
    return value.strip().lower() in ("1", "true", "yes", "on")
  return bool(value)


def _ns_to_ms(value: Any) -> float:
  return _as_float(value) / 1_000_000


def _ns_to_seconds(value: Any) -> float:
  return _as_float(value) / 1_000_000_000


def _safe_datetime(value: Any) -> Any:
  if not value or str(value).startswith("0001-"):
    return None
  return value


def _status_reason(
  code: str,
  message: str,
  value: Any = None,
  *,
  severity: str = "info",
) -> dict[str, Any]:
  return {
    "code": code,
    "message": message,
    "value": value,
    "severity": severity,
  }


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
    "target": operation.target,
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


def _percent(numerator: int, denominator: int) -> float:
  if denominator <= 0:
    return 0.0
  return round(max(min(numerator / denominator * 100, 100.0), 0.0), 2)


def _health_thresholds() -> tuple[float, float, float, float, float]:
  """Normalize health settings so invalid deployment values cannot hide alerts."""
  warning = min(max(float(settings.MINIO_DRIVE_WARNING_PERCENT), 1.0), 99.0)
  critical = min(max(float(settings.MINIO_DRIVE_CRITICAL_PERCENT), warning), 100.0)
  inode_warning = min(max(float(settings.MINIO_DRIVE_INODE_WARNING_PERCENT), 1.0), 99.0)
  inode_critical = min(max(float(settings.MINIO_DRIVE_INODE_CRITICAL_PERCENT), inode_warning), 100.0)
  skew = min(max(float(settings.MINIO_DRIVE_CAPACITY_SKEW_PERCENT), 0.0), 99.0)
  return warning, critical, inode_warning, inode_critical, skew


def _evaluate_drive_health(drives: list[dict[str, Any]]) -> None:
  """Add Storagent write-capacity health to MinIO's native drive state.

  MinIO reports ``state=ok`` when a drive is attached and readable. That
  state does not guarantee that another object can be written, so disk space,
  inodes, and erasure-set capacity skew are evaluated independently.
  """
  warning, critical, inode_warning, inode_critical, skew_limit = _health_thresholds()
  totals = [item["total_bytes"] for item in drives if item["total_bytes"] > 0]
  largest_total = max(totals, default=0)

  for item in drives:
    native_state = item["state"].strip().lower()
    reasons: list[str] = []
    usage_percent = _percent(item["used_bytes"], item["total_bytes"])
    inode_total = item["used_inodes"] + item["free_inodes"]
    inode_usage_percent = _percent(item["used_inodes"], inode_total)
    capacity_skew = bool(
      skew_limit > 0
      and largest_total > 0
      and item["total_bytes"] > 0
      and item["total_bytes"] < largest_total * (1 - skew_limit / 100)
    )

    health = "healthy"
    if native_state not in ("ok", "online", "healthy"):
      health = "offline"
      reasons.append(f"MinIO 磁盘状态为 {item['state']}")
    elif (
      (item["total_bytes"] > 0 and item["available_bytes"] <= 0)
      or (inode_total > 0 and item["free_inodes"] <= 0)
    ):
      health = "critical"
      reasons.append("磁盘空间或 inode 已耗尽，无法保证继续写入")
    elif usage_percent >= critical or inode_usage_percent >= inode_critical:
      health = "critical"
      reasons.append(
        f"容量 {usage_percent:.1f}% / inode {inode_usage_percent:.1f}% 已达到严重阈值"
        f"（剩余 {item['available_bytes']}B，空闲 inode {item['free_inodes']}）"
      )
    elif usage_percent >= warning or inode_usage_percent >= inode_warning:
      health = "warning"
      reasons.append(f"容量 {usage_percent:.1f}% / inode {inode_usage_percent:.1f}% 已达到预警阈值")

    if capacity_skew:
      reasons.append(f"容量显著低于同一 Erasure Set 最大盘 {largest_total}B")
      if health == "healthy":
        health = "warning"

    item.update({
      "health": health,
      "health_reasons": reasons,
      "usage_percent": usage_percent,
      "inode_usage_percent": inode_usage_percent,
      "capacity_skew": capacity_skew,
    })


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
        "used_inodes": _as_int(drive.get("used_inodes")),
        "free_inodes": _as_int(drive.get("free_inodes")),
        "waiting_operations": _as_int(metrics.get("totalWaiting")),
      })

  _evaluate_drive_health(drives)

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
  warning_disks = sum(item["health"] == "warning" for item in drives)
  critical_disks = sum(item["health"] == "critical" for item in drives)
  health_reasons = [reason for item in drives for reason in item["health_reasons"]]
  critical = critical_disks > 0
  degraded = (
    mode != "online"
    or offline_disks > 0
    or healing_disks > 0
    or warning_disks > 0
    or critical_disks > 0
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
    "status": "critical" if critical else ("degraded" if degraded else "online"),
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
    "warning_disks": warning_disks,
    "critical_disks": critical_disks,
    "health_reasons": health_reasons,
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
  critical = sum(item["status"] == "critical" for item in clusters)
  offline = sum(item["status"] == "offline" for item in clusters)
  overall = "offline" if not clusters or offline == len(clusters) else (
    "critical" if critical else ("degraded" if offline or degraded else "online")
  )
  summary = {
    "status": overall,
    "cluster_count": len(clusters),
    "online_clusters": online,
    "degraded_clusters": degraded,
    "critical_clusters": critical,
    "offline_clusters": offline,
    "online_disks": sum(_as_int(item.get("online_disks")) for item in clusters),
    "offline_disks": sum(_as_int(item.get("offline_disks")) for item in clusters),
    "healing_disks": sum(_as_int(item.get("healing_disks")) for item in clusters),
    "warning_disks": sum(_as_int(item.get("warning_disks")) for item in clusters),
    "critical_disks": sum(_as_int(item.get("critical_disks")) for item in clusters),
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
  if isinstance(targets, dict):
    targets = [targets]
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
    failed_count = _as_int(item.get("failedReplicationCount"))
    failed_bytes = _as_int(
      item.get("failedReplicationSize") or item.get("failedReplicationBytes")
    )
    status = status_map.get(raw_status, "unknown")
    # MinIO reports Completed even when individual objects could not be
    # replicated. Keep that outcome distinct from a fully successful run.
    if status == "completed" and failed_count > 0:
      status = "partial"
    result[arn] = {
      "resync_status": status,
      "resync_reset_id": str(item.get("resetid") or item.get("resetID") or ""),
      "resync_started_at": _safe_datetime(item.get("startTime")),
      "resync_updated_at": _safe_datetime(item.get("endTime")),
      "resync_completed_bytes": _as_int(item.get("completedReplicationSize")),
      "resync_object_count": _as_int(item.get("replicationCount")),
      "resync_failed_count": failed_count,
      "resync_failed_bytes": failed_bytes,
      "resync_current_object": str(item.get("object") or ""),
      "resync_error": str(
        item.get("error") or item.get("errorMessage") or item.get("message") or ""
      ),
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
  failed_recent = failed.get("lastHour") if isinstance(failed.get("lastHour"), dict) else {}
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
  remote_targets = payload.get("remoteTargets")
  if isinstance(remote_targets, dict):
    remote_targets = [remote_targets]
  if not isinstance(remote_targets, list):
    remote_targets = []
  for target in remote_targets:
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
    target_failed_recent = (
      target_failed.get("lastHour")
      if isinstance(target_failed.get("lastHour"), dict)
      else {}
    )
    online = _as_bool(target.get("isOnline"))
    failed_count = _as_int(target_failed_totals.get("count"))
    recent_failed_count = _as_int(target_failed_recent.get("count"))
    latency = target.get("latency") if isinstance(target.get("latency"), dict) else {}
    resync = resync_by_arn.get(arn) or {
      "resync_status": "idle" if resync_status_known else "unknown",
    }
    resync_status = str(resync.get("resync_status") or "unknown")
    current_rate = rates.get(arn, 0.0)
    target_reasons: list[dict[str, Any]] = []
    if not arn:
      target_reasons.append(_status_reason(
        "replication_rule_missing",
        "目标方向缺少可用的复制规则",
        target_name,
        severity="critical",
      ))
    if not online:
      target_reasons.append(_status_reason(
        "target_offline",
        "复制目标当前不可达",
        target_name,
        severity="critical",
      ))
    if recent_failed_count:
      target_reasons.append(_status_reason(
        "recent_replication_failures",
        "近 1 小时仍有复制失败",
        recent_failed_count,
        severity="degraded",
      ))
    if resync_status == "partial":
      target_reasons.append(_status_reason(
        "resync_partial",
        "最近一次对象补传存在失败对象",
        _as_int(resync.get("resync_failed_count")),
        severity="degraded",
      ))
    elif resync_status == "failed":
      target_reasons.append(_status_reason(
        "resync_failed",
        "最近一次对象补传失败",
        str(resync.get("resync_error") or "") or _as_int(
          resync.get("resync_failed_count")
        ),
        severity="degraded",
      ))
    if failed_count > 0 and resync_status != "completed":
      target_reasons.append(_status_reason(
        "unresolved_historical_failures",
        "历史复制失败尚无一次完整成功的补传作为已解决依据",
        failed_count,
        severity="degraded",
      ))
    if resync_status == "running":
      target_reasons.append(_status_reason(
        "resync_running",
        "对象补传任务正在运行",
        _as_int(resync.get("resync_object_count")),
        severity="syncing",
      ))
    if current_rate > 0:
      target_reasons.append(_status_reason(
        "replication_transfer_active",
        "复制链路当前有数据传输",
        current_rate,
        severity="syncing",
      ))
    if not arn or not online:
      status = "critical"
    elif resync_status == "running":
      status = "syncing"
    elif (
      recent_failed_count
      or resync_status in ("partial", "failed")
      or (failed_count > 0 and resync_status != "completed")
    ):
      # Historical failure counters only become resolved evidence after a
      # fully successful resync. MinIO deliberately does not reset totals.
      status = "degraded"
    else:
      status = "syncing" if rates.get(arn, 0) > 0 else "healthy"
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
      "recent_failed_count": recent_failed_count,
      "recent_failed_bytes": _as_int(target_failed_recent.get("bytes")),
      "current_rate_bps": current_rate,
      **resync,
      "status_reasons": target_reasons,
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
      "status_reasons": [_status_reason(
        "replication_rule_missing",
        "目标方向缺少可用的复制规则",
        missing,
        severity="critical",
      )],
    })

  actual_target_count = sum(
    bool(str(item.get("arn") or ""))
    for item in remote_targets
    if isinstance(item, dict)
  )
  queued_count = _as_int(queue_current.get("count"))
  failed_count = _as_int(failed_totals.get("count"))
  recent_failed_count = _as_int(failed_recent.get("count"))
  current_rate = sum(rates.values())
  critical_target_count = sum(item["status"] == "critical" for item in targets)
  degraded_target_count = sum(item["status"] == "degraded" for item in targets)
  syncing_target_count = sum(item["status"] == "syncing" for item in targets)
  source_reasons: list[dict[str, Any]] = []
  if critical_target_count:
    source_reasons.append(_status_reason(
      "critical_target_links",
      "存在异常复制链路",
      critical_target_count,
      severity="critical",
    ))
  if actual_target_count != len(expected_targets):
    source_reasons.append(_status_reason(
      "target_count_mismatch",
      "实际复制目标数与预期不一致",
      {"actual": actual_target_count, "expected": len(expected_targets)},
      severity="degraded",
    ))
  if recent_failed_count:
    source_reasons.append(_status_reason(
      "recent_replication_failures",
      "源站近 1 小时仍有复制失败",
      recent_failed_count,
      severity="degraded",
    ))
  if degraded_target_count:
    source_reasons.append(_status_reason(
      "degraded_target_links",
      "存在需要关注的复制链路",
      degraded_target_count,
      severity="degraded",
    ))
  if queued_count:
    source_reasons.append(_status_reason(
      "replication_queue_pending",
      "源站仍有对象等待复制",
      queued_count,
      severity="syncing",
    ))
  if current_rate > 0:
    source_reasons.append(_status_reason(
      "replication_transfer_active",
      "源站当前有复制流量",
      current_rate,
      severity="syncing",
    ))
  if syncing_target_count:
    source_reasons.append(_status_reason(
      "syncing_target_links",
      "存在正在同步的复制链路",
      syncing_target_count,
      severity="syncing",
    ))
  if mrf_failed:
    source_reasons.append(_status_reason(
      "mrf_recent_backlog_observed",
      "MinIO 报告过 MRF 补偿队列记录；该计数可能粘滞，仅供诊断",
      mrf_failed,
    ))
  if critical_target_count:
    status = "critical"
  elif (
    actual_target_count != len(expected_targets)
    or recent_failed_count
    or degraded_target_count
  ):
    status = "degraded"
  elif queued_count or current_rate > 0 or any(
    item.get("status") == "syncing" for item in targets
  ):
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
    "recent_failed_count": recent_failed_count,
    "recent_failed_bytes": _as_int(failed_recent.get("bytes")),
    "mrf_failed_last_5m": mrf_failed,
    "retries_total": retries_total,
    "current_rate_bps": current_rate,
    "expected_target_count": len(expected_targets),
    "actual_target_count": actual_target_count,
    "targets": sorted(targets, key=lambda item: item["target"]),
    "status_reasons": source_reasons,
  }


def _worst_replication_status(statuses: list[str]) -> str:
  for status in ("unreachable", "critical", "degraded", "syncing", "healthy"):
    if status in statuses:
      return status
  return "healthy"


def _aggregate_source_status_reasons(
  sources: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  reasons: list[dict[str, Any]] = []
  messages = {
    "unreachable": "存在无法读取复制指标的源站检查",
    "critical": "存在异常源站检查",
    "degraded": "存在需要关注的源站检查",
    "syncing": "存在正在同步的源站检查",
  }
  for status in ("unreachable", "critical", "degraded", "syncing"):
    count = sum(item.get("status") == status for item in sources)
    if count:
      reasons.append(_status_reason(
        f"{status}_sources",
        messages[status],
        count,
        severity=status,
      ))
  mrf_failed = sum(_as_int(item.get("mrf_failed_last_5m")) for item in sources)
  if mrf_failed:
    reasons.append(_status_reason(
      "mrf_recent_backlog_observed",
      "MinIO 报告过 MRF 补偿队列记录；该计数可能粘滞，仅供诊断",
      mrf_failed,
    ))
  return reasons


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
        "status_reasons": [_status_reason(
          "metrics_unreachable",
          "无法读取源站的 MinIO 复制指标",
          error,
          severity="unreachable",
        )],
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
    bucket_status = _worst_replication_status([item["status"] for item in sources])
    buckets.append({
      "bucket": bucket_name,
      "shown_name": app_names.get(bucket_name, ""),
      "status": bucket_status,
      "sources": sources,
      "status_reasons": _aggregate_source_status_reasons(sources),
    })

  all_targets = [target for source in all_sources for target in source.get("targets", []) if target.get("arn")]
  expected_links = len(bucket_names) * len(server_names) * max(len(server_names) - 1, 0)
  summary_status = (
    "degraded"
    if not bucket_names or len(server_names) < 2
    else _worst_replication_status([item["status"] for item in all_sources])
  )
  summary_reasons: list[dict[str, Any]] = []
  if not bucket_names:
    summary_reasons.append(_status_reason(
      "no_managed_buckets",
      "当前没有可检查的已启用存储桶",
      0,
      severity="degraded",
    ))
  if len(server_names) < 2:
    summary_reasons.append(_status_reason(
      "insufficient_servers",
      "至少需要两个存储节点才能形成复制链路",
      len(server_names),
      severity="degraded",
    ))
  summary_reasons.extend(_aggregate_source_status_reasons(all_sources))
  summary = {
    "status": summary_status,
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
    "recent_failed_count": sum(
      _as_int(item.get("recent_failed_count")) for item in all_sources
    ),
    "recent_failed_bytes": sum(
      _as_int(item.get("recent_failed_bytes")) for item in all_sources
    ),
    "mrf_failed_last_5m": sum(_as_int(item.get("mrf_failed_last_5m")) for item in all_sources),
    "current_rate_bps": sum(_as_float(item.get("current_rate_bps")) for item in all_sources),
    "status_reasons": summary_reasons,
  }
  return {
    "generated_at": utc_now(),
    "servers": server_names,
    "summary": summary,
    "buckets": buckets,
  }


async def get_orphan_bucket_overview() -> dict[str, Any]:
  """Compare physical MinIO buckets with the application catalog."""
  server_names = sorted(set(await storage_crud.read_minio_server_names()))
  applications = await public_crud.read_application_list()
  app_by_name = {str(app.name): app for app in applications}
  system_buckets = {settings.OBJECT_ARCHIVE_BUCKET.strip()}
  system_buckets.discard("")
  semaphore = asyncio.Semaphore(5)

  async def inspect(server: str):
    async with semaphore:
      return server, await minio_op.list_server_buckets(
        server,
        timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
      )

  inspections = await asyncio.gather(*(inspect(server) for server in server_names))
  bucket_servers: dict[str, set[str]] = {}
  errors: dict[str, str] = {}
  for server, (success, buckets, error, _elapsed_ms) in inspections:
    if not success:
      errors[server] = error
      continue
    for bucket_name in buckets:
      bucket_servers.setdefault(bucket_name, set()).add(server)

  rows: list[dict[str, Any]] = []
  for bucket_name, present_on in bucket_servers.items():
    app = app_by_name.get(bucket_name)
    if bucket_name in system_buckets:
      kind = "system"
      app_name = ""
      shown_name = "过期对象归档"
    elif app is None:
      kind = "orphan"
      app_name = ""
      shown_name = ""
    elif not getattr(app, "enabled", False):
      kind = "disabled_application"
      app_name = str(app.name)
      shown_name = str(getattr(app, "shown_name", "") or "")
    else:
      continue
    rows.append({
      "name": bucket_name,
      "kind": kind,
      "app_name": app_name,
      "app_shown_name": shown_name,
      "servers": sorted(present_on),
      "missing_servers": sorted(set(server_names) - present_on),
    })

  kind_order = {"orphan": 0, "disabled_application": 1, "system": 2}
  rows.sort(key=lambda item: (kind_order[item["kind"]], item["name"]))
  return {
    "generated_at": utc_now(),
    "servers": server_names,
    "summary": {
      "orphan_count": sum(item["kind"] == "orphan" for item in rows),
      "disabled_application_count": sum(item["kind"] == "disabled_application" for item in rows),
      "system_bucket_count": sum(item["kind"] == "system" for item in rows),
      "unavailable_server_count": len(errors),
    },
    "buckets": rows,
    "errors": errors,
  }


def _replication_operation_response(operation: StorageOperation) -> dict[str, Any]:
  detail = dict(operation.result or {})
  detail.update({
    "operation_id": str(operation.id),
    "operation_status": operation.status,
  })
  return {
    "message": operation.message,
    "bucket": operation.bucket,
    "source_server": detail.get("source_server") or (
      operation.server if operation.kind == "replication_resync" else None
    ),
    "target_server": detail.get("target_server") or operation.target or None,
    "detail": detail,
  }


async def _reuse_active_operation(
  kind: str,
  server: str,
  bucket: str,
  target: str,
  *,
  max_age_seconds: float,
) -> StorageOperation | None:
  active = await storage_crud.read_active_storage_operation(
    kind,
    server,
    bucket=bucket,
    target=target,
  )
  if active is None:
    return None
  if (utc_now() - _aware(active.created_at)).total_seconds() <= max_age_seconds:
    return active
  active.status = "failed"
  active.message = "后端重启或任务超时，状态已回收"
  active.finished_at = utc_now()
  await active.save()
  return None


async def _run_replication_reconcile_operation(operation: StorageOperation) -> None:
  from src.core import sync as sync_module

  try:
    async with _distributed_operation_lock(f"replication-reconcile/{operation.bucket}"):
      operation.status = "running"
      operation.started_at = utc_now()
      operation.message = "正在校准存储桶复制规则"
      await operation.save()
      server_names = await storage_crud.read_minio_server_names()
      async with sync_module.application_replication_lock(operation.bucket):
        policy = await sync_module.setup_bucket_replication(operation.bucket, server_names)
  except StorageOperationLockBusy as error:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = str(error)
    await operation.save()
    return
  except asyncio.CancelledError:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = "后端停止，复制规则校准任务已回收"
    await operation.save()
    raise
  except Exception as error:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = str(error)
    await operation.save()
    metrics_mod.incr("replication_reconcile_failures_total")
    audit.audit(
      "replication.reconcile",
      actor=operation.actor,
      resource=operation.bucket,
      detail=str(error),
      success=False,
    )
    return

  operation.status = "succeeded"
  operation.finished_at = utc_now()
  operation.message = "复制规则校准完成"
  operation.result = {**operation.result, "policy": policy}
  await operation.save()
  metrics_mod.incr("replication_reconcile_runs_total")
  audit.audit(
    "replication.reconcile",
    actor=operation.actor,
    resource=operation.bucket,
    detail=policy,
  )


async def reconcile_bucket_replication(bucket: str, actor: str) -> dict[str, Any]:
  bucket_name = _validate_bucket(bucket)
  operation = await _reuse_active_operation(
    "replication_reconcile",
    "all",
    bucket_name,
    "",
    max_age_seconds=max(float(settings.MINIO_OPERATION_TIMEOUT_SECONDS) * 30, 300.0),
  )
  if operation is not None:
    return _replication_operation_response(operation)

  operation = await storage_crud.create_storage_operation(
    kind="replication_reconcile",
    server="all",
    bucket=bucket_name,
    actor=actor,
  )
  operation.message = "复制规则校准任务已进入队列"
  operation.result = {"scope": "full_mesh"}
  await operation.save()
  _spawn(_run_replication_reconcile_operation(operation))
  return _replication_operation_response(operation)


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


async def _run_replication_resync_operation(operation: StorageOperation) -> None:
  source = operation.server
  target = operation.target
  remote_arn = str(operation.result.get("remote_arn") or "")
  older_than = operation.result.get("older_than") or None
  detail: dict[str, Any] = {}
  elapsed_ms = 0.0
  try:
    async with _distributed_operation_lock(
      f"replication-resync/{operation.bucket}/{source}/{target}"
    ):
      operation.status = "running"
      operation.started_at = utc_now()
      operation.message = "正在向 MinIO 提交对象补传"
      await operation.save()
      running = await _read_running_resync(source, operation.bucket, remote_arn)
      if running:
        detail = {**running, "already_running": True}
      else:
        success, detail, error, elapsed_ms = await minio_op.start_bucket_replication_resync(
          source,
          operation.bucket,
          remote_arn,
          older_than=older_than,
          timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
        )
        if not success:
          # A concurrent request can become visible just after the first
          # status read. Treat the native task as the authoritative result.
          running = await _read_running_resync(source, operation.bucket, remote_arn)
          if running:
            detail = {**running, "already_running": True}
          else:
            raise RuntimeError(error)
  except StorageOperationLockBusy as error:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = str(error)
    await operation.save()
    return
  except asyncio.CancelledError:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = "后端停止，对象补传启动任务已回收"
    await operation.save()
    raise
  except Exception as error:
    operation.status = "failed"
    operation.finished_at = utc_now()
    operation.message = str(error)
    await operation.save()
    metrics_mod.incr("replication_resync_failures_total")
    audit.audit(
      "replication.resync",
      actor=operation.actor,
      resource=f"{operation.bucket}:{source}->{target}",
      detail=str(error),
      success=False,
    )
    return

  operation.status = "succeeded"
  operation.finished_at = utc_now()
  operation.message = (
    "已检测到 MinIO 正在执行对象补传"
    if detail.get("already_running")
    else "对象补传任务已提交 MinIO"
  )
  operation.result = {
    **operation.result,
    "native": detail,
    "elapsed_ms": elapsed_ms,
  }
  await operation.save()
  metrics_mod.incr("replication_resync_runs_total")
  audit.audit(
    "replication.resync",
    actor=operation.actor,
    resource=f"{operation.bucket}:{source}->{target}",
    detail=operation.result,
  )


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

  operation = await _reuse_active_operation(
    "replication_resync",
    source,
    bucket_name,
    target,
    max_age_seconds=max(float(settings.MINIO_OPERATION_TIMEOUT_SECONDS) * 30, 300.0),
  )
  if operation is not None:
    return _replication_operation_response(operation)

  operation = await storage_crud.create_storage_operation(
    kind="replication_resync",
    server=source,
    bucket=bucket_name,
    target=target,
    actor=actor,
  )
  operation.message = "对象补传启动任务已进入队列"
  operation.result = {
    "source_server": source,
    "target_server": target,
    "remote_arn": remote_arn,
    "older_than": older_than or "",
  }
  await operation.save()
  _spawn(_run_replication_resync_operation(operation))
  return _replication_operation_response(operation)


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
