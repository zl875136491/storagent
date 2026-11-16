"""上传会话。预留只在确认 MinIO 结果后转移，租约过期本身不放额度。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import timedelta

from minio.error import S3Error
from pymongo.errors import DuplicateKeyError

from src.virtual_storage.core.database import run_transaction, transaction
from src.virtual_storage.core.exception import (
  ConflictError,
  NotReadyError,
  UnknownMinioResult,
)
from src.virtual_storage.core.minio_gateway import MinioGateway, missing_object, missing_upload
from src.virtual_storage.models.documents import (
  OCCUPYING_OBJECT_STATES,
  OPEN_SESSION_STATES,
  MinioCall,
  UploadSession,
  VirtualObject,
)
from src.virtual_storage.services.quota import commit_reservation, release_reservation, reserve_upload
from src.virtual_storage.services.support import new_id, stamp, write_audit
from src.virtual_storage.settings import get_settings


def session_view(session: UploadSession) -> dict:
  parts = [
    {"part_number": int(number), "etag": value.get("etag"), "size": value.get("size")}
    for number, value in sorted(session.confirmed_parts.items(), key=lambda item: int(item[0]))
  ]
  return {
    "upload_id": session.upload_id,
    "tenant_id": session.tenant_id,
    "object_key": session.object_key,
    "object_id": session.object_id,
    "state": session.state,
    "size_bytes": session.size_bytes,
    "reservation_held": session.reservation_held,
    "result_uncertain": session.result_uncertain,
    "fence_epoch": session.fence_epoch,
    "confirmed_parts": parts,
  }


async def _open_calls(resource_id: str) -> int:
  return await MinioCall.find(MinioCall.resource_id == resource_id, MinioCall.state == "open").count()


@asynccontextmanager
async def _inflight(resource_id: str, op: str, fence_epoch: int):
  call_id = new_id("call")
  await MinioCall(
    call_id=call_id,
    resource_id=resource_id,
    op=op,
    fence_epoch=fence_epoch,
    state="open",
    started_at=stamp(),
  ).insert()
  try:
    yield
  finally:
    await MinioCall.get_motor_collection().update_one(
      {"call_id": call_id},
      {"$set": {"state": "finished", "finished_at": stamp()}},
    )


async def _create_empty_object(tenant, account, object_key: str, content_type: str, gateway: MinioGateway) -> dict:
  upload_id = new_id("up")
  object_id = new_id("obj")
  storage_key = f"vs/v1/{account.account_id}/{tenant.tenant_id}/{object_id}"
  settings = get_settings()
  async with transaction() as session:
    await reserve_upload(
      account_id=account.account_id,
      tenant_id=tenant.tenant_id,
      size_bytes=0,
      upload_id=upload_id,
      session=session,
    )
    await UploadSession(
      upload_id=upload_id,
      account_id=account.account_id,
      tenant_id=tenant.tenant_id,
      home_region=account.home_region,
      object_key=object_key,
      object_id=object_id,
      storage_key=storage_key,
      bucket=settings.minio_bucket,
      size_bytes=0,
      state="completing",
      reservation_held=True,
      created_at=stamp(),
      updated_at=stamp(),
    ).insert(session=session)
  stored = await UploadSession.find_one(UploadSession.upload_id == upload_id)
  try:
    etag = gateway.put_bytes(stored.bucket, stored.storage_key, b"", content_type)
  except UnknownMinioResult:
    await _mark_uncertain(upload_id)
    stored = await UploadSession.find_one(UploadSession.upload_id == upload_id)
    return session_view(stored)
  except S3Error:
    await _release(stored, "failed")
    raise ConflictError("空对象写入失败")
  await _publish(stored, etag)
  stored = await UploadSession.find_one(UploadSession.upload_id == upload_id)
  return session_view(stored)


async def create_upload(
  *,
  tenant,
  account,
  object_key: str,
  size_bytes: int,
  content_type: str,
  gateway: MinioGateway,
) -> dict:
  if size_bytes < 0:
    raise ConflictError("对象大小不能为负")
  if size_bytes == 0:
    return await _create_empty_object(tenant, account, object_key, content_type, gateway)
  existing = await UploadSession.find_one(
    UploadSession.tenant_id == tenant.tenant_id,
    UploadSession.object_key == object_key,
    {"state": {"$in": list(OPEN_SESSION_STATES)}},
  )
  if existing:
    return session_view(existing)
  occupying = await VirtualObject.find_one(
    VirtualObject.tenant_id == tenant.tenant_id,
    VirtualObject.object_key == object_key,
    {"state": {"$in": list(OCCUPYING_OBJECT_STATES)}},
  )
  if occupying:
    raise ConflictError("同名对象已存在", "OBJECT_ALREADY_EXISTS")

  upload_id = new_id("up")
  object_id = new_id("obj")
  storage_key = f"vs/v1/{account.account_id}/{tenant.tenant_id}/{object_id}"
  settings = get_settings()
  try:
    async def _reserve(session):
      await reserve_upload(
        account_id=account.account_id,
        tenant_id=tenant.tenant_id,
        size_bytes=size_bytes,
        upload_id=upload_id,
        session=session,
      )
      await UploadSession(
        upload_id=upload_id,
        account_id=account.account_id,
        tenant_id=tenant.tenant_id,
        home_region=account.home_region,
        object_key=object_key,
        object_id=object_id,
        storage_key=storage_key,
        bucket=settings.minio_bucket,
        size_bytes=size_bytes,
        state="initializing",
        reservation_held=True,
        created_at=stamp(),
        updated_at=stamp(),
      ).insert(session=session)
    await run_transaction(_reserve)
  except DuplicateKeyError as exc:
    current = await UploadSession.find_one(
      UploadSession.tenant_id == tenant.tenant_id,
      UploadSession.object_key == object_key,
      {"state": {"$in": list(OPEN_SESSION_STATES)}},
    )
    if current:
      return session_view(current)
    raise ConflictError("上传会话冲突") from exc

  stored = await UploadSession.find_one(UploadSession.upload_id == upload_id)
  await _bind_or_initiate(stored, gateway, content_type)
  stored = await UploadSession.find_one(UploadSession.upload_id == upload_id)
  return session_view(stored)


async def _bind_or_initiate(session: UploadSession, gateway: MinioGateway, content_type: str) -> None:
  if session.minio_upload_id or session.state not in {"initializing", "uploading"}:
    return
  if await _open_calls(session.upload_id):
    return
  try:
    async with _inflight(session.upload_id, "initiate", session.fence_epoch):
      found = gateway.find_upload_id(session.bucket, session.storage_key)
      if len(found) > 1:
        await _mark_uncertain(session.upload_id)
        return
      minio_upload_id = found[0] if found else gateway.create_multipart(
        session.bucket, session.storage_key, session.upload_id, content_type,
      )
  except UnknownMinioResult:
    await _mark_uncertain(session.upload_id)
    return
  except S3Error:
    await _mark_uncertain(session.upload_id)
    return
  await UploadSession.get_motor_collection().update_one(
    {"upload_id": session.upload_id, "state": "initializing"},
    {"$set": {
      "minio_upload_id": minio_upload_id,
      "state": "uploading",
      "result_uncertain": False,
      "updated_at": stamp(),
    }},
  )


async def _mark_uncertain(upload_id: str) -> None:
  await UploadSession.get_motor_collection().update_one(
    {"upload_id": upload_id, "reservation_held": True},
    {"$set": {"result_uncertain": True, "updated_at": stamp()}},
  )


async def upload_part(
  *,
  tenant,
  upload_id: str,
  part_number: int,
  data: bytes,
  gateway: MinioGateway,
  checksum: str | None = None,
) -> dict:
  if part_number < 1:
    raise ConflictError("分片序号无效")
  if checksum is not None:
    import hashlib
    actual = hashlib.sha256(data).hexdigest()
    if actual != checksum.strip().lower():
      raise ConflictError("分片 SHA-256 与内容不一致", "CHECKSUM_MISMATCH")
  session = await _owned_session(tenant.tenant_id, upload_id)
  if len(data) > session.size_bytes:
    raise ConflictError("分片长度超过对象声明大小", "PART_TOO_LARGE")
  if session.state != "uploading" or not session.minio_upload_id:
    raise NotReadyError("上传会话尚未绑定对象存储，请沿原会话重试")
  if await _open_calls(upload_id):
    raise NotReadyError("原对象存储调用尚未收敛")
  try:
    async with _inflight(upload_id, "upload_part", session.fence_epoch):
      etag = gateway.upload_part(
        session.bucket, session.storage_key, session.minio_upload_id, part_number, data,
      )
  except UnknownMinioResult as exc:
    await _mark_uncertain(upload_id)
    raise exc
  await UploadSession.get_motor_collection().update_one(
    {"upload_id": upload_id, "state": "uploading"},
    {"$set": {
      f"confirmed_parts.{part_number}": {"etag": etag, "size": len(data)},
      "updated_at": stamp(),
      "result_uncertain": False,
    }},
  )
  return {"part_number": part_number, "etag": etag, "size": len(data)}


async def request_complete(tenant, upload_id: str) -> dict:
  """完成请求不读取客户端 ETag，只把会话推进到 completing。"""
  session = await _owned_session(tenant.tenant_id, upload_id)
  if session.state == "completed":
    return session_view(session)
  if session.state in {"completing", "reconciling"}:
    return session_view(session)
  if session.state != "uploading":
    raise NotReadyError("当前状态不能完成")
  await UploadSession.get_motor_collection().update_one(
    {"upload_id": upload_id, "state": "uploading"},
    {"$set": {"state": "completing", "updated_at": stamp()}},
  )
  session = await UploadSession.find_one(UploadSession.upload_id == upload_id)
  return session_view(session)


async def request_abort(tenant, upload_id: str) -> dict:
  session = await _owned_session(tenant.tenant_id, upload_id)
  if session.state in {"aborted", "completed"}:
    return session_view(session)
  if session.state not in OPEN_SESSION_STATES:
    raise NotReadyError("当前状态不能中止")
  await UploadSession.get_motor_collection().update_one(
    {"upload_id": upload_id, "state": {"$in": list(OPEN_SESSION_STATES)}, "reservation_held": True},
    {"$set": {"state": "aborting", "updated_at": stamp()}},
  )
  session = await UploadSession.find_one(UploadSession.upload_id == upload_id)
  return session_view(session)


async def get_upload(tenant_id: str, upload_id: str) -> dict:
  return session_view(await _owned_session(tenant_id, upload_id))


async def _owned_session(tenant_id: str, upload_id: str) -> UploadSession:
  session = await UploadSession.find_one(
    UploadSession.upload_id == upload_id,
    UploadSession.tenant_id == tenant_id,
  )
  if session is None:
    from src.virtual_storage.core.exception import VirtualStorageError
    raise VirtualStorageError("上传会话不存在", "UPLOAD_SESSION_NOT_FOUND", 404)
  return session


async def acquire_lease(upload_id: str, worker_id: str) -> UploadSession | None:
  settings = get_settings()
  now = stamp()
  until = now + timedelta(seconds=settings.lease_seconds)
  collection = UploadSession.get_motor_collection()
  renewed = await collection.update_one(
    {"upload_id": upload_id, "$or": [
      {"worker_lease_id": ""},
      {"worker_lease_id": worker_id},
      {"worker_lease_until": None},
    ]},
    {"$set": {"worker_lease_id": worker_id, "worker_lease_until": until, "updated_at": now}},
  )
  if renewed.modified_count == 1:
    return await UploadSession.find_one(UploadSession.upload_id == upload_id)
  taken_over = await collection.update_one(
    {"upload_id": upload_id, "worker_lease_until": {"$lt": now}, "worker_lease_id": {"$ne": worker_id}},
    {
      "$set": {"worker_lease_id": worker_id, "worker_lease_until": until, "updated_at": now},
      "$inc": {"fence_epoch": 1},
    },
  )
  if taken_over.modified_count != 1:
    return None
  return await UploadSession.find_one(UploadSession.upload_id == upload_id)


async def drive_session(upload_id: str, gateway: MinioGateway, worker_id: str) -> dict:
  session = await acquire_lease(upload_id, worker_id)
  if session is None:
    return {"action": "busy", "upload_id": upload_id}
  if not session.reservation_held or session.state not in OPEN_SESSION_STATES:
    return {"action": "idle", "upload_id": upload_id, "state": session.state}
  if await _open_calls(upload_id):
    return {"action": "hold", "reason": "inflight", "upload_id": upload_id}
  if session.state == "initializing":
    await _bind_or_initiate(session, gateway, "application/octet-stream")
    return {"action": "initiate", "upload_id": upload_id}
  if session.state == "aborting":
    return await _drive_abort(session, gateway)
  if session.state in {"completing", "reconciling"}:
    return await _drive_complete(session, gateway)
  return {"action": "hold", "reason": session.state, "upload_id": upload_id}


async def _drive_complete(session: UploadSession, gateway: MinioGateway) -> dict:
  if not session.minio_upload_id:
    await _mark_uncertain(session.upload_id)
    await _set_state(session.upload_id, "reconciling")
    return {"action": "hold", "reason": "missing_upload_id"}
  try:
    async with _inflight(session.upload_id, "complete", session.fence_epoch):
      parts = gateway.list_server_parts(session.bucket, session.storage_key, session.minio_upload_id)
      total = sum(part["size"] for part in parts)
      if total != session.size_bytes or not parts:
        outcome = ("size_mismatch", parts)
      else:
        result = gateway.complete(
          session.bucket, session.storage_key, session.minio_upload_id, parts,
        )
        outcome = ("completed", _etag_of(result), parts)
  except UnknownMinioResult:
    await _mark_uncertain(session.upload_id)
    await _set_state(session.upload_id, "reconciling")
    return {"action": "hold", "reason": "unknown_complete"}
  except S3Error as exc:
    if missing_upload(exc):
      return await _recover_finished_object(session, gateway)
    await _mark_uncertain(session.upload_id)
    await _set_state(session.upload_id, "reconciling")
    return {"action": "hold", "reason": exc.code}

  if outcome[0] == "size_mismatch":
    await UploadSession.get_motor_collection().update_one(
      {"upload_id": session.upload_id, "state": {"$in": ["completing", "reconciling"]}},
      {"$set": {"state": "uploading", "result_uncertain": False, "updated_at": stamp()}},
    )
    return {"action": "retry_parts", "server_parts": outcome[1]}
  await _publish(session, outcome[1])
  return {"action": "completed", "etag": outcome[1]}


def _etag_of(result) -> str:
  etag = getattr(result, "etag", None) or ""
  return str(etag).strip('"')


async def _recover_finished_object(session: UploadSession, gateway: MinioGateway) -> dict:
  try:
    async with _inflight(session.upload_id, "stat", session.fence_epoch):
      stat = gateway.stat(session.bucket, session.storage_key)
  except UnknownMinioResult:
    await _mark_uncertain(session.upload_id)
    await _set_state(session.upload_id, "reconciling")
    return {"action": "hold", "reason": "unknown_stat"}
  except S3Error as exc:
    if missing_object(exc):
      if await _open_calls(session.upload_id):
        return {"action": "hold", "reason": "inflight"}
      await _release(session, "aborted")
      return {"action": "aborted"}
    await _mark_uncertain(session.upload_id)
    return {"action": "hold", "reason": exc.code}
  if int(getattr(stat, "size", -1)) != session.size_bytes:
    await _mark_uncertain(session.upload_id)
    await _set_state(session.upload_id, "reconciling")
    return {"action": "hold", "reason": "size_differs"}
  await _publish(session, str(getattr(stat, "etag", "")).strip('"'))
  return {"action": "completed_from_stat"}


async def _drive_abort(session: UploadSession, gateway: MinioGateway) -> dict:
  if session.minio_upload_id:
    try:
      async with _inflight(session.upload_id, "abort", session.fence_epoch):
        gateway.abort(session.bucket, session.storage_key, session.minio_upload_id)
    except UnknownMinioResult:
      await _mark_uncertain(session.upload_id)
      return {"action": "hold", "reason": "unknown_abort"}
    except S3Error as exc:
      if not missing_upload(exc):
        await _mark_uncertain(session.upload_id)
        return {"action": "hold", "reason": exc.code}
  if await _open_calls(session.upload_id):
    return {"action": "hold", "reason": "inflight"}
  try:
    async with _inflight(session.upload_id, "stat", session.fence_epoch):
      gateway.stat(session.bucket, session.storage_key)
    await _mark_uncertain(session.upload_id)
    return {"action": "hold", "reason": "object_exists"}
  except UnknownMinioResult:
    await _mark_uncertain(session.upload_id)
    return {"action": "hold", "reason": "unknown_stat"}
  except S3Error as exc:
    if not missing_object(exc):
      await _mark_uncertain(session.upload_id)
      return {"action": "hold", "reason": exc.code}
  await _release(session, "aborted")
  return {"action": "aborted"}


async def _publish(session: UploadSession, etag: str) -> None:
  async with transaction() as db_session:
    updated = await UploadSession.get_motor_collection().update_one(
      {
        "upload_id": session.upload_id,
        "reservation_held": True,
        "state": {"$in": ["completing", "reconciling"]},
      },
      {"$set": {
        "state": "completed",
        "reservation_held": False,
        "result_uncertain": False,
        "updated_at": stamp(),
      }},
      session=db_session,
    )
    if updated.modified_count != 1:
      return
    await commit_reservation(
      account_id=session.account_id,
      tenant_id=session.tenant_id,
      size_bytes=session.size_bytes,
      upload_id=session.upload_id,
      session=db_session,
    )
    await VirtualObject(
      object_id=session.object_id,
      account_id=session.account_id,
      tenant_id=session.tenant_id,
      object_key=session.object_key,
      storage_key=session.storage_key,
      bucket=session.bucket,
      size_bytes=session.size_bytes,
      etag=etag,
      state="active",
      created_at=stamp(),
      updated_at=stamp(),
    ).insert(session=db_session)
    await write_audit(
      account_id=session.account_id,
      tenant_id=session.tenant_id,
      actor_type="worker",
      actor_id="complete",
      action="upload.complete",
      resource_id=session.object_id,
      detail={"etag": etag, "bytes": session.size_bytes},
      session=db_session,
    )


async def _release(session: UploadSession, state: str) -> None:
  async with transaction() as db_session:
    updated = await UploadSession.get_motor_collection().update_one(
      {"upload_id": session.upload_id, "reservation_held": True},
      {"$set": {
        "state": state,
        "reservation_held": False,
        "result_uncertain": False,
        "updated_at": stamp(),
      }},
      session=db_session,
    )
    if updated.modified_count != 1:
      return
    await release_reservation(
      account_id=session.account_id,
      tenant_id=session.tenant_id,
      size_bytes=session.size_bytes,
      upload_id=session.upload_id,
      session=db_session,
    )


async def _set_state(upload_id: str, state: str) -> None:
  await UploadSession.get_motor_collection().update_one(
    {"upload_id": upload_id, "reservation_held": True},
    {"$set": {"state": state, "updated_at": stamp()}},
  )
