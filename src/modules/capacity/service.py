"""Collect and serve capacity planning data without putting MinIO scans on page load."""
from __future__ import annotations

import asyncio
from datetime import timedelta

from src.configs.configs import settings
from src.modules.files.model import ObjectCatalog
from src.modules.storage import operations
from src.modules.storage.model import RegionCapacitySnapshot
from src.utils.helpers import utc_now
from src.utils.logger import logger


def _day(value) -> str:
  return value.astimezone().strftime("%Y-%m-%d") if value.tzinfo else value.strftime("%Y-%m-%d")


async def collect_snapshot() -> None:
  """Persist one per-region sample. Only the authority performs shared collection."""
  if settings.REGION != settings.SYNC_AUTHORITY_REGION:
    return
  overview, replication = await asyncio.gather(
    operations.get_cluster_health_overview(),
    operations.get_replication_overview(),
  )
  replication_by_server = {
    item.get("server"): item
    for bucket in replication.get("buckets", [])
    for item in bucket.get("sources", [])
    if isinstance(item, dict)
  }
  archive_rows = await ObjectCatalog.aggregate([
    {"$match": {"state": {"$in": ["archived", "archive_pending"]}}},
    {"$group": {"_id": "$source_region", "archive_bytes": {"$sum": "$size_bytes"}, "archived_object_count": {"$sum": 1}}},
  ]).to_list()
  archive_by_region = {str(row.get("_id") or ""): row for row in archive_rows}
  now = utc_now()
  sample_day = now.strftime("%Y-%m-%d")
  for cluster in overview.get("clusters", []):
    if not cluster.get("reachable"):
      continue
    region = str(cluster.get("region") or cluster.get("server") or "")
    replication_source = replication_by_server.get(cluster.get("server"), {})
    archive = archive_by_region.get(region, {})
    row = RegionCapacitySnapshot(
      region=region,
      shown_name=str(cluster.get("shown_name") or region),
      raw_capacity_bytes=max(int(cluster.get("raw_capacity_bytes") or 0), 0),
      raw_used_bytes=max(int(cluster.get("raw_used_bytes") or 0), 0),
      logical_usage_bytes=max(int(cluster.get("logical_usage_bytes") or 0), 0),
      object_count=max(int(cluster.get("object_count") or 0), 0),
      archive_bytes=max(int(archive.get("archive_bytes") or 0), 0),
      archived_object_count=max(int(archive.get("archived_object_count") or 0), 0),
      expected_replica_count=max(int(replication_source.get("expected_target_count") or 0), 0),
      actual_replica_count=max(int(replication_source.get("actual_target_count") or 0), 0),
      captured_at=now,
      sample_day=sample_day,
    )
    existing = await RegionCapacitySnapshot.find_one(
      RegionCapacitySnapshot.region == region,
      RegionCapacitySnapshot.sample_day == sample_day,
    )
    if existing:
      # 一天只保留一个样本，但允许当天的采集任务不断刷新最新值。
      existing = row.model_copy(update={"id": existing.id})
      await existing.save()
    else:
      await row.insert()


async def capacity_snapshot_task() -> None:
  """Periodic collector; failures are logged and the next interval continues."""
  while True:
    try:
      await collect_snapshot()
    except asyncio.CancelledError:
      raise
    except Exception as error:
      logger.warning("容量规划快照采集失败: %s", error)
    await asyncio.sleep(max(int(settings.CAPACITY_SNAPSHOT_INTERVAL_SECONDS), 300))


def _growth(rows: list[RegionCapacitySnapshot]) -> float:
  if len(rows) < 2:
    return 0.0
  first, last = rows[0], rows[-1]
  elapsed = max((last.captured_at - first.captured_at).total_seconds() / 86400, 1.0)
  return max((last.raw_used_bytes - first.raw_used_bytes) / elapsed, 0.0)


def _days_to(capacity: int, used: int, daily_growth: float, target: float) -> int | None:
  if capacity <= 0 or daily_growth <= 0:
    return None
  remaining = capacity * target - used
  return 0 if remaining <= 0 else int(remaining / daily_growth)


async def get_planning() -> dict:
  rows = await RegionCapacitySnapshot.find_all().sort("+region", "+captured_at").to_list()
  grouped: dict[str, list[RegionCapacitySnapshot]] = {}
  for row in rows:
    grouped.setdefault(row.region, []).append(row)
  data = []
  for region, samples in grouped.items():
    samples = samples[-30:]
    latest = samples[-1]
    growth = _growth(samples)
    waterline = latest.raw_used_bytes / latest.raw_capacity_bytes * 100 if latest.raw_capacity_bytes else 0.0
    days70 = _days_to(latest.raw_capacity_bytes, latest.raw_used_bytes, growth, 0.70)
    days85 = _days_to(latest.raw_capacity_bytes, latest.raw_used_bytes, growth, 0.85)
    days95 = _days_to(latest.raw_capacity_bytes, latest.raw_used_bytes, growth, 0.95)
    risks = []
    if waterline >= 95: risks.append("容量已达到 95% 水位")
    elif waterline >= 85: risks.append("容量已达到 85% 水位")
    elif waterline >= 70: risks.append("容量已达到 70% 水位")
    if days95 is not None and days95 <= 30: risks.append("预计 30 天内达到 95% 水位")
    if latest.actual_replica_count < latest.expected_replica_count: risks.append("复制冗余低于预期")
    data.append({
      "region": region, "shown_name": latest.shown_name,
      "raw_capacity_bytes": latest.raw_capacity_bytes, "raw_used_bytes": latest.raw_used_bytes,
      "logical_usage_bytes": latest.logical_usage_bytes, "object_count": latest.object_count,
      "archive_bytes": latest.archive_bytes, "archived_object_count": latest.archived_object_count,
      "expected_replica_count": latest.expected_replica_count, "actual_replica_count": latest.actual_replica_count,
      "waterline_percent": round(waterline, 2), "daily_growth_bytes": growth,
      "estimated_days_to_70": days70, "estimated_days_to_85": days85, "estimated_days_to_95": days95,
      "risks": risks,
      "trend": [{"captured_at": item.captured_at, "raw_capacity_bytes": item.raw_capacity_bytes, "raw_used_bytes": item.raw_used_bytes, "logical_usage_bytes": item.logical_usage_bytes, "object_count": item.object_count, "archive_bytes": item.archive_bytes} for item in samples],
    })
  return {"generated_at": utc_now(), "data": data}
