"""下载、列表、软删除和恢复。软删除把 used 转入 trashed，占用不变。"""
from __future__ import annotations

from datetime import timedelta

from src.virtual_storage.core.database import transaction
from src.virtual_storage.core.exception import ConflictError, VirtualStorageError
from src.virtual_storage.core.minio_gateway import MinioGateway
from src.virtual_storage.models.documents import VirtualObject
from src.virtual_storage.services.quota import move_trash_to_used, move_used_to_trash, release_trash_once
from src.virtual_storage.services.support import stamp, write_audit
from src.virtual_storage.services.uploads import _inflight, _open_calls


def object_view(obj: VirtualObject) -> dict:
  return {
    "object_id": obj.object_id,
    "tenant_id": obj.tenant_id,
    "object_key": obj.object_key,
    "size_bytes": obj.size_bytes,
    "etag": obj.etag,
    "content_type": obj.content_type,
    "state": obj.state,
    "created_at": obj.created_at.isoformat(),
  }


async def list_objects(tenant_id: str, prefix: str = "", limit: int = 50) -> dict:
  query = {"tenant_id": tenant_id, "state": "active"}
  if prefix:
    import re
    query["object_key"] = {"$regex": f"^{re.escape(prefix)}"}
  rows = await VirtualObject.find(query).sort(VirtualObject.object_key).limit(min(limit, 200)).to_list()
  return {"objects": [object_view(row) for row in rows]}


async def read_object(tenant_id: str, object_id: str, gateway: MinioGateway, range_header: str | None):
  obj = await VirtualObject.find_one(VirtualObject.object_id == object_id, VirtualObject.tenant_id == tenant_id)
  if obj is None or obj.state != "active":
    raise VirtualStorageError("对象不存在", "OBJECT_NOT_FOUND", 404)
  if obj.size_bytes == 0 and not range_header:
    return {
      "data": b"",
      "content_type": obj.content_type,
      "etag": obj.etag,
      "status": 200,
      "start": 0,
      "end": 0,
      "size": 0,
    }
  start, end, partial = _parse_range(range_header, obj.size_bytes)
  length = end - start + 1
  data = gateway.get_range(obj.bucket, obj.storage_key, start, length)
  return {
    "data": data,
    "content_type": obj.content_type,
    "etag": obj.etag,
    "status": 206 if partial else 200,
    "start": start,
    "end": end,
    "size": obj.size_bytes,
  }


def _parse_range(header: str | None, size: int) -> tuple[int, int, bool]:
  if not header:
    return 0, max(size - 1, 0), False
  if not header.startswith("bytes=") or "," in header:
    raise VirtualStorageError("只支持单一 bytes 范围", "RANGE_NOT_SATISFIABLE", 416)
  raw = header.removeprefix("bytes=")
  start_text, _, end_text = raw.partition("-")
  if start_text == "":
    raise VirtualStorageError("只支持起始偏移的范围", "RANGE_NOT_SATISFIABLE", 416)
  start = int(start_text)
  end = int(end_text) if end_text else size - 1
  if start < 0 or end < start or start >= size:
    raise VirtualStorageError("范围超出对象", "RANGE_NOT_SATISFIABLE", 416)
  end = min(end, size - 1)
  return start, end, True


async def soft_delete(tenant_id: str, object_id: str) -> dict:
  obj = await VirtualObject.find_one(VirtualObject.object_id == object_id, VirtualObject.tenant_id == tenant_id)
  if obj is None or obj.state == "purged":
    raise VirtualStorageError("对象不存在", "OBJECT_NOT_FOUND", 404)
  if obj.state == "soft_deleted":
    return object_view(obj)
  if obj.state != "active":
    raise ConflictError("对象已进入不可逆清除")
  async with transaction() as session:
    updated = await VirtualObject.get_motor_collection().update_one(
      {"object_id": object_id, "tenant_id": tenant_id, "state": "active"},
      {"$set": {
        "state": "soft_deleted",
        "soft_deleted_at": stamp(),
        "purge_after": stamp() + timedelta(days=7),
        "updated_at": stamp(),
      }},
      session=session,
    )
    if updated.modified_count != 1:
      current = await VirtualObject.find_one(VirtualObject.object_id == object_id, session=session)
      return object_view(current)
    await move_used_to_trash(
      account_id=obj.account_id,
      tenant_id=tenant_id,
      size_bytes=obj.size_bytes,
      object_id=object_id,
      session=session,
    )
    await write_audit(
      account_id=obj.account_id,
      tenant_id=tenant_id,
      actor_type="tenant_key",
      actor_id=tenant_id,
      action="object.soft_delete",
      resource_id=object_id,
      detail={"bytes": obj.size_bytes, "quota": "trashed_still_occupies"},
      session=session,
    )
  refreshed = await VirtualObject.find_one(VirtualObject.object_id == object_id)
  return object_view(refreshed)


async def restore_object(tenant_id: str, object_id: str) -> dict:
  obj = await VirtualObject.find_one(VirtualObject.object_id == object_id, VirtualObject.tenant_id == tenant_id)
  if obj is None or obj.state == "purged":
    raise VirtualStorageError("对象不存在", "OBJECT_NOT_FOUND", 404)
  if obj.state == "active":
    return object_view(obj)
  if obj.state != "soft_deleted":
    raise ConflictError("清除已经开始，不能恢复")
  async with transaction() as session:
    updated = await VirtualObject.get_motor_collection().update_one(
      {"object_id": object_id, "state": "soft_deleted"},
      {"$set": {"state": "active", "soft_deleted_at": None, "purge_after": None, "updated_at": stamp()}},
      session=session,
    )
    if updated.modified_count != 1:
      raise ConflictError("清除已经开始，不能恢复")
    await move_trash_to_used(
      account_id=obj.account_id,
      tenant_id=tenant_id,
      size_bytes=obj.size_bytes,
      object_id=object_id,
      session=session,
    )
  refreshed = await VirtualObject.find_one(VirtualObject.object_id == object_id)
  return object_view(refreshed)


async def purge_object(object_id: str, gateway: MinioGateway, *, force: bool = False) -> dict:
  obj = await VirtualObject.find_one(VirtualObject.object_id == object_id)
  if obj is None or obj.state == "purged":
    return {"action": "gone", "object_id": object_id}
  if obj.state == "active":
    return {"action": "skip", "object_id": object_id}
  if obj.state == "soft_deleted" and not force:
    from src.virtual_storage.services.support import as_utc
    if obj.purge_after and as_utc(obj.purge_after) > stamp():
      return {"action": "waiting", "object_id": object_id}
  if await _open_calls(object_id):
    return {"action": "hold", "reason": "inflight", "object_id": object_id}
  if obj.state == "soft_deleted":
    async with transaction() as session:
      moved = await VirtualObject.get_motor_collection().update_one(
        {"object_id": object_id, "state": "soft_deleted"},
        {"$set": {"state": "purge_pending", "updated_at": stamp()}},
        session=session,
      )
      if moved.modified_count == 1:
        await write_audit(
          account_id=obj.account_id,
          tenant_id=obj.tenant_id,
          actor_type="worker",
          actor_id="purge",
          action="object.purge_pending",
          resource_id=object_id,
          detail={"bytes": obj.size_bytes},
          session=session,
        )
    obj = await VirtualObject.find_one(VirtualObject.object_id == object_id)
  if obj.state != "purge_pending":
    return {"action": "skip", "state": obj.state}
  from src.virtual_storage.core.exception import UnknownMinioResult
  from minio.error import S3Error
  from src.virtual_storage.core.minio_gateway import missing_object
  try:
    async with _inflight(object_id, "remove", 1):
      gateway.remove(obj.bucket, obj.storage_key)
  except UnknownMinioResult:
    return {"action": "hold", "reason": "unknown_remove"}
  except S3Error as exc:
    if not missing_object(exc):
      return {"action": "hold", "reason": exc.code}
  if await _open_calls(object_id):
    return {"action": "hold", "reason": "inflight"}
  async with transaction() as session:
    updated = await VirtualObject.get_motor_collection().update_one(
      {"object_id": object_id, "state": "purge_pending", "trash_released": False},
      {"$set": {"state": "purged", "trash_released": True, "purged_at": stamp(), "updated_at": stamp()}},
      session=session,
    )
    if updated.modified_count != 1:
      return {"action": "already_released"}
    await release_trash_once(
      account_id=obj.account_id,
      tenant_id=obj.tenant_id,
      size_bytes=obj.size_bytes,
      object_id=object_id,
      session=session,
    )
  return {"action": "purged", "object_id": object_id}
