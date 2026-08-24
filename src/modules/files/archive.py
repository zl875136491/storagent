"""Archive expired soft-deleted objects before releasing application storage."""
from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from minio.commonconfig import CopySource

from src.configs.configs import settings
from src.core.minio_op import get_minio_client
from src.modules.files import crud, quota
from src.modules.files.model import ObjectCatalog
from src.modules.storage import crud as storage_crud
from src.utils.helpers import utc_now
from src.utils.logger import logger


def archive_object_key(item: ObjectCatalog) -> str:
  """Keep archive names deterministic so a retry can safely resume a copy."""
  source = (
    f"{item.app_name}\0{item.object_id}\0{item.deletion_generation}\0"
    f"{item.storage_key}"
  ).encode("utf-8")
  digest = hashlib.sha256(source).hexdigest()
  return f"expired/{item.app_name}/{item.object_id}/g{item.deletion_generation}/{digest}"


async def _get_client(item: ObjectCatalog):
  source_region = str(item.source_region or settings.REGION).strip() or settings.REGION
  server = await storage_crud.read_minio_server_by_region_name(source_region)
  if server is None and source_region != settings.REGION:
    server = await storage_crud.read_minio_server_by_region_name(settings.REGION)
  if server is None:
    raise RuntimeError(f"归档源站点未配置: {source_region}")
  access_key, secret_key = storage_crud.plain_minio_credentials(server)
  return get_minio_client(server.host, server.minio_port, access_key, secret_key)


def _normalize_etag(value: Any) -> str:
  text = str(value or "").strip()
  return text[1:-1] if len(text) >= 2 and text[0] == '"' and text[-1] == '"' else text


def _as_utc(value: datetime) -> datetime:
  """Normalize MongoDB's naive BSON timestamps before comparing deadlines."""
  if value.tzinfo is None:
    return value.replace(tzinfo=timezone.utc)
  return value.astimezone(timezone.utc)


def _is_missing_object(error: BaseException) -> bool:
  text = f"{getattr(error, 'code', '')} {error}".lower().replace("_", "")
  return any(value in text for value in (
    "nosuchkey",
    "nosuchobject",
    "object does not exist",
  ))


def _is_single_part_md5_etag(value: str) -> bool:
  return bool(re.fullmatch(r"[0-9a-f]{32}", value, flags=re.IGNORECASE))


def _verify_source_object(item: ObjectCatalog, source: Any) -> str:
  size = int(getattr(source, "size", -1))
  if size != int(item.size_bytes):
    raise RuntimeError(f"归档源校验失败：大小 {size} != {item.size_bytes}")
  source_etag = _normalize_etag(getattr(source, "etag", ""))
  expected_etag = _normalize_etag(item.etag)
  if expected_etag and source_etag and source_etag != expected_etag:
    raise RuntimeError("归档源校验失败：ETag 不一致")
  return source_etag or expected_etag


def _verify_archive_object(
  item: ObjectCatalog,
  source_etag: str,
  archived: Any,
) -> str:
  size = int(getattr(archived, "size", -1))
  if size != int(item.size_bytes):
    raise RuntimeError(f"归档校验失败：大小 {size} != {item.size_bytes}")
  archive_etag = _normalize_etag(getattr(archived, "etag", ""))
  if not archive_etag:
    raise RuntimeError("归档校验失败：归档副本缺少 ETag")
  # ETags are opaque for multipart, encrypted, and transformed objects. MinIO
  # rewrites the multipart `-1` form produced by mc pipe during server-side
  # copies. The source version is pinned in CopySource and independently
  # validated above; equality is only reliable for regular single-part MD5
  # ETags.
  if source_etag and _is_single_part_md5_etag(source_etag) and archive_etag != source_etag:
    raise RuntimeError("归档校验失败：ETag 不一致")
  return source_etag or archive_etag


def _copy_then_remove(
  client: Any,
  item: ObjectCatalog,
  archive_bucket: str,
  archive_key: str,
) -> str:
  if not item.minio_version_id:
    raise RuntimeError("对象缺少版本 ID，无法安全删除归档前的源版本")
  if not client.bucket_exists(archive_bucket):
    client.make_bucket(archive_bucket)

  source_stat = client.stat_object(
    item.bucket,
    item.storage_key,
    version_id=item.minio_version_id or None,
  )
  source_etag = _verify_source_object(item, source_stat)

  archived = None
  try:
    archived = client.stat_object(archive_bucket, archive_key)
  except Exception as error:
    if not _is_missing_object(error):
      raise
    source = CopySource(
      item.bucket,
      item.storage_key,
      version_id=item.minio_version_id or None,
    )
    client.copy_object(archive_bucket, archive_key, source)
    archived = client.stat_object(archive_bucket, archive_key)

  checksum = _verify_archive_object(item, source_etag, archived)

  # Delete only the catalogued version. A later upload of the same key must not
  # be removed by a delayed archive task.
  try:
    client.remove_object(
      item.bucket,
      item.storage_key,
      version_id=item.minio_version_id,
    )
  except Exception as error:
    # A worker can stop after a successful delete and before persisting the
    # archived state. The already-verified archive makes this retry complete.
    if not _is_missing_object(error):
      raise
  return checksum or item.etag


async def archive_expired_object(
  candidate: ObjectCatalog,
  *,
  now=None,
) -> str:
  """Archive one due object. Failures retain the source object and are retried."""
  now = _as_utc(now or utc_now())
  retry_after = timedelta(seconds=max(float(settings.OBJECT_ARCHIVE_RETRY_SECONDS), 30.0))
  item = await crud.claim_expired_object_for_archive(
    candidate.app_name,
    candidate.object_id,
    now=now,
    retry_after=retry_after,
  )
  if item is None:
    return "skipped"

  archive_bucket = settings.OBJECT_ARCHIVE_BUCKET.strip()
  archive_key = archive_object_key(item)
  archive_id = f"{archive_bucket}/{archive_key}"
  try:
    # Restore and archive both use the application quota lock. Re-read after
    # acquiring it so a restore that won the race cannot be overwritten by a
    # stale archive worker.
    async with quota.application_quota_lock(item.app_name):
      current = await crud.read_object_by_id(item.app_name, item.object_id)
      if current is None or current.state != "archive_pending":
        return "skipped"
      if current.restore_until is None or _as_utc(current.restore_until) > now:
        return "skipped"
      client = await _get_client(current)
      try:
        checksum = await asyncio.to_thread(
          _copy_then_remove,
          client,
          current,
          archive_bucket,
          archive_key,
        )
      except Exception as error:
        message = str(error)[:1000]
        await crud.save_object(
          current,
          state="archive_failed",
          archive_after=utc_now() + retry_after,
          archive_error=message,
          last_operation_id=f"archive-failed:{current.object_id}",
        )
        logger.warning(
          "过期对象归档失败 app={} object={}: {}",
          current.app_name,
          current.object_key,
          message,
        )
        return "failed"

      await crud.save_object(
        current,
        state="archived",
        archive_after=None,
        purge_after=None,
        archive_id=archive_id,
        archive_checksum=checksum,
        archive_error="",
        last_operation_id=f"archive:{current.object_id}",
      )
  except Exception as error:
    logger.warning(
      "过期对象归档任务无法获取应用锁 app={} object={}: {}",
      item.app_name,
      item.object_key,
      error,
    )
    return "failed"
  logger.info(
    "过期对象已归档 app={} object={} archive={}",
    item.app_name,
    item.object_key,
    archive_id,
  )
  return "archived"


async def archive_expired_objects_once() -> dict[str, int]:
  """Run one bounded archive pass so the scheduler cannot monopolize the loop."""
  now = utc_now()
  rows = await crud.list_expired_objects_for_archive(
    now,
    limit=max(int(settings.OBJECT_ARCHIVE_BATCH_SIZE), 1),
  )
  result = {"candidates": len(rows), "archived": 0, "failed": 0, "skipped": 0}
  for row in rows:
    outcome = await archive_expired_object(row, now=now)
    result[outcome] = result.get(outcome, 0) + 1
  return result


async def archive_expired_objects_task() -> None:
  """Periodically archive catalog rows whose recovery window has ended."""
  interval = max(float(settings.OBJECT_ARCHIVE_INTERVAL_SECONDS), 30.0)
  while True:
    try:
      result = await archive_expired_objects_once()
      if result["candidates"]:
        logger.info(
          "过期对象归档扫描 candidates=%s archived=%s failed=%s skipped=%s",
          result["candidates"],
          result["archived"],
          result["failed"],
          result["skipped"],
        )
    except asyncio.CancelledError:
      raise
    except Exception as error:
      logger.warning("过期对象归档扫描失败: %s", error)
    await asyncio.sleep(interval)
