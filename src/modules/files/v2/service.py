"""v2 file Service: wrappers plus object lifecycle operations."""
from __future__ import annotations

import base64
from datetime import timedelta
from typing import Iterable

from fastapi import Request

from src.configs.configs import settings
from src.core.exception import CustomException, ErrorDesc
from src.modules.files import crud, quota
from src.modules.files import service as v1_service
from src.modules.files.v2 import schema
from src.modules.public import service as public_service
from src.modules.storage import download as storage_download
from src.utils.helpers import utc_now


def request_id(request: Request) -> str:
  return str(getattr(request.state, "request_id", ""))


def data(value):
  return value.model_dump(mode="json") if hasattr(value, "model_dump") else value


async def wrap(request: Request, value):
  return {"data": data(value), "request_id": request_id(request)}


async def multipart_init(context, content_type, *, size_bytes):
  return await v1_service.multipart_init(context, content_type, size_bytes=size_bytes)


async def multipart_part(**kwargs):
  return await v1_service.multipart_upload_part(**kwargs)


async def multipart_complete(context, upload_id, object_key, parts):
  return await v1_service.multipart_complete(context, upload_id, object_key, parts)


async def multipart_abort(body, context):
  return await v1_service.multipart_abort(body, context)


async def multipart_list_parts(context, object_key, upload_id, marker):
  return await v1_service.multipart_list_parts(context, object_key, upload_id, marker)


async def locate(app_name, object_key, offset, length):
  await crud.require_active_object(app_name, object_key)
  return await v1_service.locate_object(app_name, object_key, offset, length)


async def stat(app_name, object_key):
  await crud.require_active_object(app_name, object_key)
  return await v1_service.stat_object(app_name, object_key)


async def download(context, object_key, offset, length):
  await crud.require_active_object(context["app_name"], object_key)
  return await v1_service.download_chunk(context, object_key, offset, length)


def _item(item):
  return schema.ObjectItem(
    object_id=item.object_id, object_key=item.object_key, size_bytes=item.size_bytes,
    etag=item.etag, content_type=item.content_type, state=item.state,
    created_at=item.created_at, updated_at=item.updated_at,
    deleted_at=item.deleted_at, restore_until=item.restore_until,
  )


def _decode_cursor(value):
  if not value:
    return ""
  try:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).decode()
  except Exception as error:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "cursor 无效") from error


def _encode_cursor(value):
  return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")


async def list_objects(request, app_name, *, prefix, state, limit, cursor):
  allowed: dict[str, Iterable[str]] = {
    "active": ("active",),
    "trash": ("soft_deleted", "archive_pending", "archived", "archive_failed"),
    "all": ("active", "soft_deleted", "archive_pending", "archived", "archive_failed"),
  }
  if state not in allowed:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "state 必须是 active、trash 或 all")
  after = _decode_cursor(cursor)
  rows = await crud.list_objects(
    app_name, allowed[state], prefix=prefix, after=after, limit=limit + 1,
  )
  more = len(rows) > limit
  rows = rows[:limit]
  return schema.ObjectListResponse(
    data=schema.ObjectListData(
      items=[_item(row) for row in rows],
      next_cursor=_encode_cursor(rows[-1].object_key) if more and rows else None,
      has_more=more,
    ),
    request_id=request_id(request),
  )


def _mutation(request, item):
  return schema.ObjectMutationResponse(
    data=schema.ObjectMutationData(
      object_id=item.object_id, object_key=item.object_key, state=item.state,
      deleted_at=item.deleted_at, restore_until=item.restore_until,
    ),
    request_id=request_id(request),
  )


async def delete(request, app_name, object_id):
  item = await crud.read_object_by_id(app_name, object_id)
  if item is None:
    raise CustomException(ErrorDesc.OBJECT_NOT_FOUND, {"object_id": object_id})
  if item.state in {"soft_deleted", "archive_pending", "archived", "archive_failed"}:
    return _mutation(request, item)
  if item.state != "active":
    raise CustomException(ErrorDesc.STATUS_ERR, "对象当前状态不允许删除")
  now = utc_now()
  restore_until = now + timedelta(days=max(int(settings.OBJECT_RECOVERY_PERIOD_DAYS), 1))
  # The App lock serializes quota accounting. The conditional catalog change
  # means only its winner can release the logical bytes.
  async with quota.application_quota_lock(app_name) as quota_client:
    item = await crud.transition_object_state(
      app_name, object_id, from_states=("active",), changes={
        "state": "soft_deleted",
        "deleted_at": now,
        "restore_until": restore_until,
        # Do not move the source object while it can still be restored.
        "archive_after": restore_until,
        "purge_after": None,
        "archive_id": "",
        "archive_checksum": "",
        "archive_error": "",
        "deletion_generation": item.deletion_generation + 1,
        "last_operation_id": request_id(request),
      },
    )
    if item is None:
      current = await crud.read_object_by_id(app_name, object_id)
      if current and current.state in {"soft_deleted", "archive_pending", "archived", "archive_failed"}:
        return _mutation(request, current)
      raise CustomException(ErrorDesc.STATUS_ERR, "对象状态已变更，请重试")
    try:
      await quota.mark_object_deleted_locked(app_name, item.size_bytes, quota_client)
    except Exception:
      # Do not hide an object if releasing logical quota could not be made
      # durable. A retry will repeat the whole idempotent transition.
      await crud.transition_object_state(
        app_name, object_id, from_states=("soft_deleted",), changes={
          "state": "active", "deleted_at": None, "restore_until": None,
          "archive_after": None, "purge_after": None,
          "archive_id": "", "archive_checksum": "", "archive_error": "",
        },
      )
      raise
  return _mutation(request, item)


async def restore(request, app_name, object_id):
  item = await crud.read_object_by_id(app_name, object_id)
  if item is None:
    raise CustomException(ErrorDesc.OBJECT_NOT_FOUND, {"object_id": object_id})
  if item.state == "active":
    return _mutation(request, item)
  if item.restore_until is None or item.restore_until <= utc_now():
    raise CustomException(ErrorDesc.OBJECT_PURGED, "对象已超过可恢复期限")
  quota_bytes = await public_service.get_application_quota_limit(app_name)
  async with quota.application_quota_lock(app_name) as quota_client:
    # Re-read beneath the lock so an archive worker cannot make this state
    # stale between the initial authorization check and quota reservation.
    item = await crud.read_object_by_id(app_name, object_id)
    if item is None:
      raise CustomException(ErrorDesc.OBJECT_NOT_FOUND, {"object_id": object_id})
    if item.state == "active":
      return _mutation(request, item)
    if item.restore_until is None or item.restore_until <= utc_now():
      raise CustomException(ErrorDesc.OBJECT_PURGED, "对象已超过可恢复期限")
    await quota.restore_deleted_object_locked(
      app_name, item.size_bytes, quota_bytes, quota_client,
    )
    updated = await crud.transition_object_state(
      app_name, object_id,
      from_states=("soft_deleted", "archive_pending", "archived", "archive_failed"),
      changes={
        "state": "active", "deleted_at": None, "restore_until": None,
        "archive_after": None, "purge_after": None,
        "archive_id": "", "archive_checksum": "", "archive_error": "",
        "last_operation_id": request_id(request),
      },
    )
    if updated is None:
      # Restore quota accounting to its soft-deleted state when Mongo changed
      # concurrently; otherwise retries would leak logical usage.
      await quota.mark_object_deleted_locked(app_name, item.size_bytes, quota_client)
      raise CustomException(ErrorDesc.STATUS_ERR, "对象状态已变更，请重试")
    item = updated
  return _mutation(request, item)


async def create_share(request, app_name, object_id, payload):
  item = await crud.read_object_by_id(app_name, object_id)
  if item is None or item.state != "active":
    raise CustomException(ErrorDesc.OBJECT_NOT_FOUND, {"object_id": object_id})
  issued = await storage_download.issue_app_one_time_download(
    app_name=app_name,
    object_id=item.object_id,
    object_key=item.storage_key,
    region_name=item.source_region,
    deletion_generation=item.deletion_generation,
    filename=payload.download_name,
    ttl=payload.expires_in_seconds,
  )
  token = issued.pop("token")
  path = str(request.app.url_path_for("v2_one_time_share_exchange"))
  return schema.ShareCreateResponse(
    data=schema.ShareData(
      share_id=issued["share_id"], download_url=f"{path}#token={token}",
      expires_at=issued["expires_at"],
      expires_in_seconds=issued["expires_in_seconds"], filename=issued["filename"],
    ),
    request_id=request_id(request),
  )
