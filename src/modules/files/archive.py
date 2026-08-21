"""Archive expired soft-deleted objects before releasing application storage."""
from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta
from typing import Any

from minio.commonconfig import CopySource

from src.configs.configs import settings
from src.core.minio_op import get_minio_client
from src.modules.files import crud
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


def _copy_then_remove(
  client: Any,
  item: ObjectCatalog,
  archive_bucket: str,
  archive_key: str,
) -> str:
  if not client.bucket_exists(archive_bucket):
    client.make_bucket(archive_bucket)

  checksum = ""
  try:
    existing = client.stat_object(archive_bucket, archive_key)
    checksum = str(getattr(existing, "etag", "") or "")
  except Exception:
    source = CopySource(
      item.bucket,
      item.storage_key,
      version_id=item.minio_version_id or None,
    )
    copied = client.copy_object(archive_bucket, archive_key, source)
    checksum = str(getattr(copied, "etag", "") or "")

  # Do this only after the archive object is known to exist. Omitting a version
  # id creates the normal delete marker expected by versioned, replicated buckets.
  client.remove_object(item.bucket, item.storage_key)
  return checksum or item.etag


async def archive_expired_object(
  candidate: ObjectCatalog,
  *,
  now=None,
) -> str:
  """Archive one due object. Failures retain the source object and are retried."""
  now = now or utc_now()
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
    client = await _get_client(item)
    checksum = await asyncio.to_thread(
      _copy_then_remove,
      client,
      item,
      archive_bucket,
      archive_key,
    )
  except Exception as error:
    message = str(error)[:1000]
    await crud.save_object(
      item,
      state="archive_failed",
      archive_after=utc_now() + retry_after,
      archive_error=message,
      last_operation_id=f"archive-failed:{item.object_id}",
    )
    logger.warning(
      "过期对象归档失败 app={} object={}: {}",
      item.app_name,
      item.object_key,
      message,
    )
    return "failed"

  await crud.save_object(
    item,
    state="archived",
    archive_after=None,
    purge_after=None,
    archive_id=archive_id,
    archive_checksum=checksum,
    archive_error="",
    last_operation_id=f"archive:{item.object_id}",
  )
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
