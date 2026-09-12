"""Celery-backed full MinIO listing used to rebuild the server file index."""
from __future__ import annotations

import time
from datetime import timedelta
from typing import Any, Literal

from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from src.configs.configs import settings
from src.core import metrics as metrics_mod
from src.modules.storage import crud as storage_crud
from src.modules.storage import inventory
from src.modules.storage.model import FileInventorySyncLease, ServerFileInventoryMeta
from src.utils.helpers import utc_now
from src.utils.logger import logger

TASK_NAME = "storagent.storage.sync_file_inventory"
Trigger = Literal["beat", "manual", "bootstrap"]


def _region() -> str:
  return str(settings.REGION).strip().lower() or "unknown"


def _interval_seconds() -> int:
  value = int(getattr(settings, "FILE_INVENTORY_SYNC_INTERVAL_SECONDS", 0) or 0)
  if value <= 0:
    value = int(settings.SERVER_DETAILS_CACHE_TTL_SECONDS)
  return max(value, 1)


def _lock_ttl_seconds() -> int:
  return max(int(getattr(settings, "FILE_INVENTORY_SYNC_LOCK_TTL_SECONDS", 3600) or 3600), 60)


def _fresh_window_seconds() -> int:
  return max(int(_interval_seconds() * 0.9), 60)


def _is_fresh(meta: ServerFileInventoryMeta, now) -> bool:
  fetched = inventory._aware_utc(meta.fetched_at)
  return (now - fetched).total_seconds() < _fresh_window_seconds()


async def is_lease_held() -> bool:
  now = utc_now()
  row = await FileInventorySyncLease.get_motor_collection().find_one({"region": _region()})
  if not row:
    return False
  if str(row.get("status") or "") != "running":
    return False
  expires = row.get("expires_at")
  if expires is None:
    return False
  expires = inventory._aware_utc(expires)
  return expires > now


async def try_acquire_lease(*, trigger: str, actor: str, task_id: str) -> bool:
  now = utc_now()
  expires = now + timedelta(seconds=_lock_ttl_seconds())
  payload = {
    "region": _region(),
    "status": "running",
    "trigger": trigger,
    "actor": actor,
    "task_id": task_id,
    "started_at": now,
    "expires_at": expires,
    "updated_at": now,
  }
  collection = FileInventorySyncLease.get_motor_collection()
  try:
    doc = await collection.find_one_and_update(
      {
        "region": _region(),
        "$or": [
          {"status": {"$ne": "running"}},
          {"expires_at": {"$lte": now}},
          {"expires_at": None},
        ],
      },
      {"$set": payload},
      upsert=True,
      return_document=ReturnDocument.AFTER,
    )
  except DuplicateKeyError:
    return False
  return bool(doc) and str(doc.get("status") or "") == "running"


async def release_lease(*, task_id: str, last_status: str) -> None:
  now = utc_now()
  await FileInventorySyncLease.get_motor_collection().update_one(
    {"region": _region(), "task_id": task_id, "status": "running"},
    {
      "$set": {
        "status": "idle",
        "expires_at": now,
        "last_finished_at": now,
        "last_status": last_status,
        "updated_at": now,
      }
    },
  )


def enqueue_file_inventory_sync(*, trigger: Trigger, actor: str = "") -> str | None:
  from src.core.celery_client import dispatch_task

  return dispatch_task(
    TASK_NAME,
    trigger=trigger,
    actor=actor,
    expires=float(_lock_ttl_seconds()),
  )


async def _sync_one_server(server: Any, *, trigger: str) -> dict[str, Any]:
  server_id = str(server.id)
  name = str(getattr(server, "name", "") or server_id)
  now = utc_now()
  meta = await inventory._read_meta(server_id)
  if trigger == "bootstrap" and meta is not None:
    return {"server_id": server_id, "name": name, "status": "skipped", "reason": "already-indexed"}
  if trigger == "beat" and meta is not None and _is_fresh(meta, now):
    return {"server_id": server_id, "name": name, "status": "skipped", "reason": "fresh"}
  started = time.monotonic()
  try:
    snapshot = await inventory._sync_from_minio(server)
    return {
      "server_id": server_id,
      "name": name,
      "status": "synced",
      "object_count": snapshot.object_count,
      "total_size": snapshot.total_size,
      "duration_ms": int((time.monotonic() - started) * 1000),
    }
  except Exception as error:
    logger.warning("文件索引同步失败 server={} error={}", name, error)
    return {
      "server_id": server_id,
      "name": name,
      "status": "failed",
      "error": str(error)[:300],
      "duration_ms": int((time.monotonic() - started) * 1000),
    }


async def sync_file_inventory_once(
  *,
  trigger: str = "beat",
  actor: str = "",
  task_id: str = "",
) -> dict[str, Any]:
  """Rebuild this region's Mongo object index from MinIO. Skip if already running."""
  if trigger not in {"beat", "manual", "bootstrap"}:
    trigger = "beat"
  region = _region()
  acquired = await try_acquire_lease(trigger=trigger, actor=actor, task_id=task_id)
  if not acquired:
    metrics_mod.incr("file_inventory_sync_skipped_total")
    return {"status": "skipped", "reason": "already-running", "trigger": trigger, "region": region}

  last_status = "failed"
  try:
    servers = await storage_crud.read_minio_server_list()
    if not servers:
      last_status = "succeeded"
      return {
        "status": "succeeded",
        "reason": "no-servers",
        "trigger": trigger,
        "region": region,
        "servers": [],
      }
    rows = [await _sync_one_server(server, trigger=trigger) for server in servers]
    failed = [row for row in rows if row.get("status") == "failed"]
    synced = [row for row in rows if row.get("status") == "synced"]
    skipped = [row for row in rows if row.get("status") == "skipped"]
    if failed and not synced:
      metrics_mod.incr("file_inventory_sync_failures_total")
      last_status = "failed"
      raise RuntimeError(str(failed[0].get("error") or "文件索引同步失败"))
    status = "partial" if failed else "succeeded"
    last_status = status
    metrics_mod.incr("file_inventory_sync_runs_total")
    return {
      "status": status,
      "trigger": trigger,
      "region": region,
      "synced": len(synced),
      "skipped": len(skipped),
      "failed": len(failed),
      "servers": rows,
    }
  finally:
    await release_lease(task_id=task_id, last_status=last_status)
