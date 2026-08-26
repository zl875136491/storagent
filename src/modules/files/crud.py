"""Shared object catalog persistence for versioned file Services."""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Iterable

from beanie.operators import GT, In, LTE
from pymongo import ReturnDocument

from src.core.exception import CustomException, ErrorDesc
from src.modules.files.model import ObjectCatalog
from src.utils.helpers import utc_now


async def upsert_completed_object(
  *,
  app_name: str,
  object_key: str,
  size_bytes: int,
  etag: str | None,
  version_id: str | None,
  content_type: str,
  source_region: str,
) -> ObjectCatalog:
  now = utc_now()
  item = await read_object_by_key(app_name, object_key)
  if item is None:
    item = ObjectCatalog(
      object_id=f"obj_{uuid.uuid4().hex}", app_name=app_name, bucket=app_name,
      object_key=object_key, storage_key=object_key, size_bytes=max(int(size_bytes), 0),
      etag=etag or "", minio_version_id=version_id,
      content_type=content_type or "application/octet-stream", source_region=source_region,
      created_at=now, updated_at=now,
    )
    await item.insert()
    return item
  item.storage_key = object_key
  item.size_bytes = max(int(size_bytes), 0)
  item.etag = etag or ""
  item.minio_version_id = version_id
  item.content_type = content_type or "application/octet-stream"
  item.source_region = source_region
  item.state = "active"
  item.deleted_at = None
  item.restore_until = None
  item.archive_after = None
  item.purge_after = None
  item.archive_id = ""
  item.archive_checksum = ""
  item.archive_error = ""
  item.deletion_generation += 1
  item.updated_at = now
  await item.save()
  return item


async def read_object_by_id(app_name: str, object_id: str) -> ObjectCatalog | None:
  return await ObjectCatalog.find_one(ObjectCatalog.app_name == app_name, ObjectCatalog.object_id == object_id)


async def read_object_by_key(app_name: str, object_key: str) -> ObjectCatalog | None:
  return await ObjectCatalog.find_one(ObjectCatalog.app_name == app_name, ObjectCatalog.object_key == object_key)


async def require_active_object(app_name: str, object_key: str) -> ObjectCatalog | None:
  item = await read_object_by_key(app_name, object_key)
  if item is not None and item.state != "active":
    raise CustomException(ErrorDesc.OBJECT_DELETED, {"object_id": item.object_id})
  return item


async def list_objects(
  app_name: str,
  states: Iterable[str],
  *,
  prefix: str = "",
  after: str = "",
  limit: int = 100,
) -> list[ObjectCatalog]:
  filters = [
    ObjectCatalog.app_name == app_name,
    In(ObjectCatalog.state, list(states)),
  ]
  if after:
    # Apply the cursor before limit. Filtering a limited result in Service
    # makes later pages appear empty when an App has many earlier keys.
    filters.append(GT(ObjectCatalog.object_key, after))
  items = await ObjectCatalog.find(*filters).sort(
    +ObjectCatalog.object_key, +ObjectCatalog.object_id,
  ).limit(max(limit, 1)).to_list()
  return [item for item in items if not prefix or item.object_key.startswith(prefix)]


async def save_object(item: ObjectCatalog, **changes) -> ObjectCatalog:
  for key, value in changes.items():
    setattr(item, key, value)
  item.updated_at = utc_now()
  await item.save()
  return item


async def transition_object_state(
  app_name: str,
  object_id: str,
  *,
  from_states: Iterable[str],
  changes: dict,
) -> ObjectCatalog | None:
  """Change state only if it is still eligible for this lifecycle action.

  The enclosing application quota lock serializes quota accounting. This
  conditional read avoids applying the same transition after an archive or
  repair worker has already changed the catalog record.
  """
  item = await ObjectCatalog.find_one(
    ObjectCatalog.app_name == app_name,
    ObjectCatalog.object_id == object_id,
    In(ObjectCatalog.state, list(from_states)),
  )
  if item is None:
    return None
  return await save_object(item, **changes)


async def list_expired_objects_for_archive(
  now: datetime,
  *,
  limit: int,
  source_region: str | None = None,
) -> list[ObjectCatalog]:
  """Find catalog rows that are past their recovery period and due to retry."""
  filters = [
    In(ObjectCatalog.state, ["soft_deleted", "archive_pending", "archive_failed"]),
    LTE(ObjectCatalog.restore_until, now),
    LTE(ObjectCatalog.archive_after, now),
  ]
  if source_region is not None:
    filters.append(ObjectCatalog.source_region == source_region)
  return await ObjectCatalog.find(*filters).sort(
    +ObjectCatalog.archive_after, +ObjectCatalog.object_id,
  ).limit(max(int(limit), 1)).to_list()


async def claim_expired_object_for_archive(
  app_name: str,
  object_id: str,
  *,
  now: datetime,
  retry_after: timedelta,
  source_region: str | None = None,
) -> ObjectCatalog | None:
  """Atomically lease one eligible row so concurrent workers cannot archive it twice."""
  query = {
      "app_name": app_name,
      "object_id": object_id,
      "state": {"$in": ["soft_deleted", "archive_pending", "archive_failed"]},
      "restore_until": {"$ne": None, "$lte": now},
      "archive_after": {"$ne": None, "$lte": now},
  }
  if source_region is not None:
    query["source_region"] = source_region
  raw = await ObjectCatalog.get_motor_collection().find_one_and_update(
    query,
    {
      "$set": {
        "state": "archive_pending",
        "archive_after": now + retry_after,
        "updated_at": now,
        "archive_error": "",
      },
    },
    return_document=ReturnDocument.AFTER,
  )
  return ObjectCatalog.model_validate(raw) if raw else None
