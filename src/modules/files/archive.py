"""Archive expired soft-deleted objects before releasing application storage."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from minio.commonconfig import CopySource
from minio.commonconfig import ENABLED
from minio.lifecycleconfig import (
  Expiration,
  LifecycleConfig,
  NoncurrentVersionExpiration,
  Rule,
)
from minio.versioningconfig import VersioningConfig

from src.configs.configs import settings
from src.core.minio_op import get_minio_client
from src.modules.files import crud, quota
from src.modules.files.model import ObjectCatalog
from src.modules.storage import crud as storage_crud
from src.utils.helpers import utc_now
from src.utils.logger import logger


ARCHIVE_LIFECYCLE_RULE_ID = "storagent-expired-archive-retention"
ETCD_KEY_OBJECT_ARCHIVE_POLICY = "object_archive_policy"
_policy_consensus_lock = asyncio.Lock()
_policy_consensus_checked_at = 0.0
_policy_consensus_fingerprint = ""
_bucket_policy_lock = asyncio.Lock()
_bucket_policy_checked_at: dict[tuple[str, str], float] = {}


def _local_region() -> str:
  return str(settings.REGION).strip().lower()


def archive_object_key(item: ObjectCatalog) -> str:
  """Keep archive names deterministic so a retry can safely resume a copy."""
  source = (
    f"{item.app_name}\0{item.object_id}\0{item.deletion_generation}\0"
    f"{item.storage_key}"
  ).encode("utf-8")
  digest = hashlib.sha256(source).hexdigest()
  return f"expired/{item.app_name}/{item.object_id}/g{item.deletion_generation}/{digest}"


async def _get_client(item: ObjectCatalog):
  source_region = str(item.source_region or "").strip().lower()
  if not source_region:
    raise RuntimeError("对象缺少源区域，禁止归档到当前站点")
  if source_region != _local_region():
    raise RuntimeError(
      f"对象源区域与归档 Worker 不匹配: source={source_region} worker={_local_region()}",
    )
  server = await storage_crud.read_minio_server_by_region_name(source_region)
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
    "nosuchversion",
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


def _archive_policy_payload() -> dict[str, object]:
  fields = {
    "version": 1,
    "enabled": bool(settings.OBJECT_ARCHIVE_ENABLED),
    "bucket": settings.OBJECT_ARCHIVE_BUCKET.strip(),
    "recovery_days": int(settings.OBJECT_RECOVERY_PERIOD_DAYS),
    "retention_days": int(settings.OBJECT_ARCHIVE_RETENTION_DAYS),
    "rule_id": ARCHIVE_LIFECYCLE_RULE_ID,
  }
  encoded = json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
  return {**fields, "fingerprint": hashlib.sha256(encoded).hexdigest()}


async def _assert_archive_policy_consensus() -> None:
  """Require all archive Workers to use the authority-approved policy."""
  global _policy_consensus_checked_at, _policy_consensus_fingerprint
  if not settings.OBJECT_ARCHIVE_ENABLED:
    return
  expected = _archive_policy_payload()
  interval = max(float(settings.OBJECT_ARCHIVE_POLICY_CHECK_SECONDS), 15.0)
  now = time.monotonic()
  if (
    _policy_consensus_fingerprint == expected["fingerprint"]
    and now - _policy_consensus_checked_at < interval
  ):
    return
  async with _policy_consensus_lock:
    now = time.monotonic()
    if (
      _policy_consensus_fingerprint == expected["fingerprint"]
      and now - _policy_consensus_checked_at < interval
    ):
      return
    from src.core import etcd_op

    if _local_region() == str(settings.SYNC_AUTHORITY_REGION).strip().lower():
      await etcd_op.push_to_etcd(ETCD_KEY_OBJECT_ARCHIVE_POLICY, expected)
    else:
      published = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_OBJECT_ARCHIVE_POLICY)
      if not isinstance(published, dict) or published.get("fingerprint") != expected["fingerprint"]:
        raise RuntimeError("归档策略尚未由权威区域发布或各区域配置不一致")
    _policy_consensus_fingerprint = str(expected["fingerprint"])
    _policy_consensus_checked_at = now


def clear_archive_policy_cache() -> None:
  """Reset process-local archive policy caches for tests and diagnostics."""
  global _policy_consensus_checked_at, _policy_consensus_fingerprint
  _policy_consensus_checked_at = 0.0
  _policy_consensus_fingerprint = ""
  _bucket_policy_checked_at.clear()


def _matching_archive_rule(lifecycle: Any) -> Any | None:
  rules = getattr(lifecycle, "rules", None)
  if not isinstance(rules, list):
    return None
  for rule in rules:
    if str(getattr(rule, "rule_id", "") or "") == ARCHIVE_LIFECYCLE_RULE_ID:
      return rule
  return None


def _archive_rule_is_valid(rule: Any) -> bool:
  expiration = getattr(rule, "expiration", None)
  noncurrent = getattr(rule, "noncurrent_version_expiration", None)
  return (
    str(getattr(rule, "status", "") or "") == ENABLED
    and int(getattr(expiration, "days", 0) or 0) >= int(settings.OBJECT_ARCHIVE_RETENTION_DAYS)
    and int(getattr(noncurrent, "noncurrent_days", 0) or 0) >= int(settings.OBJECT_ARCHIVE_RETENTION_DAYS)
  )


def _new_archive_rule() -> Rule:
  retention_days = max(int(settings.OBJECT_ARCHIVE_RETENTION_DAYS), 1)
  return Rule(
    ENABLED,
    rule_id=ARCHIVE_LIFECYCLE_RULE_ID,
    expiration=Expiration(days=retention_days),
    noncurrent_version_expiration=NoncurrentVersionExpiration(
      noncurrent_days=retention_days,
    ),
  )


def _is_missing_lifecycle(error: BaseException) -> bool:
  """Recognize MinIO's missing-lifecycle response without masking other errors."""
  code = str(getattr(error, "code", "") or "").strip().lower()
  if code in {"nosuchlifecycleconfiguration", "nosuchbucketlifecycle"}:
    return True
  text = str(error).lower()
  return "no such lifecycle configuration" in text


def _ensure_archive_bucket_policy(client: Any, archive_bucket: str) -> None:
  """Validate the named archive lifecycle rule before any source deletion."""
  if not client.bucket_exists(archive_bucket):
    if not settings.OBJECT_ARCHIVE_AUTOCONFIGURE:
      raise RuntimeError("归档桶不存在；请先完成归档桶预配置后再开启归档")
    client.make_bucket(archive_bucket)

  # Minimal mock clients in isolated tests do not model bucket policy APIs.
  # Production MinIO clients do, and a failure there aborts archival rather
  # than creating an unprotected, indefinite archive store.
  if not hasattr(client, "set_bucket_versioning"):
    return
  versioning = client.get_bucket_versioning(archive_bucket)
  if str(getattr(versioning, "status", "")) != ENABLED:
    if not settings.OBJECT_ARCHIVE_AUTOCONFIGURE:
      raise RuntimeError("归档桶未启用版本控制")
    client.set_bucket_versioning(archive_bucket, VersioningConfig(ENABLED))

  if not hasattr(client, "get_bucket_lifecycle"):
    return
  try:
    lifecycle = client.get_bucket_lifecycle(archive_bucket)
  except Exception as error:
    if not _is_missing_lifecycle(error):
      raise
    lifecycle = None
  if lifecycle is None:
    if not settings.OBJECT_ARCHIVE_AUTOCONFIGURE:
      raise RuntimeError("归档桶缺少受控生命周期策略")
    client.set_bucket_lifecycle(
      archive_bucket,
      LifecycleConfig([_new_archive_rule()]),
    )
    return
  matching_rule = _matching_archive_rule(lifecycle)
  if matching_rule is not None:
    if not _archive_rule_is_valid(matching_rule):
      raise RuntimeError("归档桶受控生命周期策略未启用或保留期不足")
    return
  if not settings.OBJECT_ARCHIVE_AUTOCONFIGURE:
    raise RuntimeError("归档桶缺少名为 storagent-expired-archive-retention 的生命周期策略")
  rules = list(getattr(lifecycle, "rules", None) or [])
  rules.append(_new_archive_rule())
  client.set_bucket_lifecycle(archive_bucket, LifecycleConfig(rules))


async def _ensure_archive_bucket_policy_once(
  client: Any,
  *,
  source_region: str,
  archive_bucket: str,
) -> None:
  key = (source_region, archive_bucket)
  interval = max(float(settings.OBJECT_ARCHIVE_POLICY_CHECK_SECONDS), 15.0)
  now = time.monotonic()
  if now - _bucket_policy_checked_at.get(key, 0.0) < interval:
    return
  async with _bucket_policy_lock:
    now = time.monotonic()
    if now - _bucket_policy_checked_at.get(key, 0.0) < interval:
      return
    await asyncio.to_thread(_ensure_archive_bucket_policy, client, archive_bucket)
    _bucket_policy_checked_at[key] = now


def _copy_then_remove(
  client: Any,
  item: ObjectCatalog,
  archive_bucket: str,
  archive_key: str,
  *,
  ensure_policy: bool = True,
) -> str:
  if not item.minio_version_id:
    raise RuntimeError("对象缺少版本 ID，无法安全删除归档前的源版本")
  if ensure_policy:
    _ensure_archive_bucket_policy(client, archive_bucket)

  try:
    source_stat = client.stat_object(
      item.bucket,
      item.storage_key,
      version_id=item.minio_version_id or None,
    )
  except Exception as error:
    if not _is_missing_object(error):
      raise
    # A worker can stop after the source removal but before the catalog state
    # is committed. The archive key is deterministic, so verify that copy and
    # finish the idempotent state transition without touching source again.
    try:
      archived = client.stat_object(archive_bucket, archive_key)
    except Exception as archive_error:
      if _is_missing_object(archive_error):
        raise RuntimeError("源对象已不存在且未找到可验证的归档副本") from error
      raise
    return _verify_archive_object(item, _normalize_etag(item.etag), archived) or item.etag
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
  if not settings.OBJECT_ARCHIVE_ENABLED:
    return "disabled"
  if str(candidate.source_region or "").strip().lower() != _local_region():
    return "skipped"
  now = _as_utc(now or utc_now())
  retry_after = timedelta(seconds=max(float(settings.OBJECT_ARCHIVE_RETRY_SECONDS), 30.0))
  item = await crud.claim_expired_object_for_archive(
    candidate.app_name,
    candidate.object_id,
    now=now,
    retry_after=retry_after,
    source_region=_local_region(),
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
      if str(current.source_region or "").strip().lower() != _local_region():
        logger.warning(
          "归档对象区域发生变化，拒绝删除源版本 app={} object={} source={} worker={}",
          current.app_name,
          current.object_key,
          current.source_region,
          _local_region(),
        )
        return "skipped"
      if current.restore_until is None or _as_utc(current.restore_until) > now:
        return "skipped"
      client = await _get_client(current)
      await _ensure_archive_bucket_policy_once(
        client,
        source_region=_local_region(),
        archive_bucket=archive_bucket,
      )
      try:
        checksum = await asyncio.to_thread(
          _copy_then_remove,
          client,
          current,
          archive_bucket,
          archive_key,
          ensure_policy=False,
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
  if not settings.OBJECT_ARCHIVE_ENABLED:
    return {"candidates": 0, "archived": 0, "failed": 0, "skipped": 0, "disabled": 1}
  await _assert_archive_policy_consensus()
  now = utc_now()
  rows = await crud.list_expired_objects_for_archive(
    now,
    limit=max(int(settings.OBJECT_ARCHIVE_BATCH_SIZE), 1),
    source_region=_local_region(),
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
