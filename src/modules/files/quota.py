"""Cross-region multipart upload quota reservations backed by Etcd."""
from __future__ import annotations

import asyncio
import logging
import secrets
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, Awaitable, Callable

from src.configs.configs import settings
from src.core import etcd_op
from src.core.exception import CustomException, ErrorDesc
from src.utils.helpers import utc_now


logger = logging.getLogger(__name__)

_LOCK_FAILURES = weakref.WeakKeyDictionary()

_STATE_VERSION = 1
_STATE_PREFIX = "quota/apps"
_SESSION_PREFIX = "quota/uploads"
_LOCK_PREFIX = "/storagent/locks/quota"
_QUOTA_EXCEEDED_REASON = "APP 存储超出限额，请联系管理员处理"


@dataclass(frozen=True)
class UploadReservation:
  app_name: str
  api_key_id: str
  object_key: str
  upload_id: str
  source_server: str
  declared_size_bytes: int
  expires_at: datetime
  content_type: str = "application/octet-stream"
  status: str = "active"


@dataclass(frozen=True)
class PreparedPart:
  reservation: UploadReservation
  operation_id: str
  previous: dict[str, Any] | None


@dataclass(frozen=True)
class PreparedCompletion:
  reservation: UploadReservation
  already_completed: bool
  recovering: bool = False
  result: dict[str, Any] | None = None


@dataclass(frozen=True)
class PreparedAbort:
  reservation: UploadReservation
  already_aborted: bool
  recovering: bool


class QuotaLockLostCancellation(asyncio.CancelledError):
  pass


def raise_if_quota_lock_lost() -> None:
  task = asyncio.current_task()
  error = _LOCK_FAILURES.get(task) if task is not None else None
  if error is not None:
    raise QuotaLockLostCancellation() from error


def _state_key(app_name: str) -> str:
  return f"{_STATE_PREFIX}/{app_name}"


def _session_key(app_name: str, object_key: str) -> str:
  return f"{_SESSION_PREFIX}/{app_name}/{object_key}"


def _full_key(key: str) -> bytes:
  return f"{etcd_op.ETCD_PREFIX}{key}".encode()


def _parse_datetime(value: Any) -> datetime | None:
  if not value:
    return None
  try:
    parsed = datetime.fromisoformat(str(value))
  except (TypeError, ValueError):
    return None
  if parsed.tzinfo is None:
    return parsed.replace(tzinfo=timezone.utc)
  return parsed.astimezone(timezone.utc)


def _new_expiry(now: datetime | None = None) -> datetime:
  now = now or utc_now()
  ttl = max(int(settings.APPLICATION_QUOTA_RESERVATION_TTL_SECONDS), 60)
  return now + timedelta(seconds=ttl)


def _normalize_state(raw: dict | None) -> dict:
  state = dict(raw or {})
  reservations = state.get("reservations")
  if not isinstance(reservations, dict):
    reservations = {}
  state.update({
    "version": _STATE_VERSION,
    "observed_usage_bytes": max(int(state.get("observed_usage_bytes") or 0), 0),
    "active_usage_bytes": max(int(state.get("active_usage_bytes") or 0), 0),
    "deleted_retained_bytes": max(int(state.get("deleted_retained_bytes") or 0), 0),
    "logical_usage_initialized": bool(state.get("logical_usage_initialized", False)),
    "observed_usage_updated_at": state.get("observed_usage_updated_at"),
    "reservations": reservations,
  })
  return state


def _reservation_from_dict(app_name: str, data: dict) -> UploadReservation:
  expiry = _parse_datetime(data.get("expires_at")) or utc_now()
  return UploadReservation(
    app_name=app_name,
    api_key_id=str(data.get("api_key_id") or ""),
    object_key=str(data.get("object_key") or ""),
    upload_id=str(data.get("upload_id") or ""),
    source_server=str(data.get("source_server") or ""),
    declared_size_bytes=max(int(data.get("declared_size_bytes") or 0), 0),
    expires_at=expiry,
    content_type=str(data.get("content_type") or "application/octet-stream"),
    status=str(data.get("status") or "active"),
  )


def _reservation_expired(data: dict, now: datetime) -> bool:
  expires_at = _parse_datetime(data.get("expires_at"))
  return expires_at is None or expires_at <= now


def _reservation_needs_cleanup(data: dict, now: datetime) -> bool:
  return (
    str(data.get("status") or "") == "cleanup_pending"
    or _reservation_expired(data, now)
  )


def _reservation_counts_toward_quota(data: dict, now: datetime) -> bool:
  # Failed cleanup must fail closed even though the original lease expired.
  return (
    str(data.get("status") or "") == "cleanup_pending"
    or not _reservation_expired(data, now)
  )


def _usage_is_fresh(state: dict, now: datetime) -> bool:
  updated_at = _parse_datetime(state.get("observed_usage_updated_at"))
  if updated_at is None:
    return False
  max_age = max(float(settings.APPLICATION_QUOTA_USAGE_CACHE_SECONDS), 0.0)
  return (now - updated_at).total_seconds() <= max_age


def _validate_identity(
  data: dict,
  *,
  api_key_id: str,
  object_key: str,
  upload_id: str,
) -> None:
  if (
    str(data.get("object_key") or "") != object_key
    or str(data.get("upload_id") or "") != upload_id
  ):
    raise CustomException(ErrorDesc.INVALID_PARAMS, "上传会话不存在或已过期")
  if str(data.get("api_key_id") or "") != api_key_id:
    raise CustomException(
      ErrorDesc.INSUFFICIENT_PERMISSIONS,
      "上传会话不属于当前 API-KEY",
    )


@asynccontextmanager
async def _distributed_lock(lock_name: str) -> AsyncIterator[Any]:
  client = await etcd_op.get_etcd_client()
  ttl = max(int(settings.REPLICATION_LOCK_TTL_SECONDS), 30)
  timeout = max(int(settings.REPLICATION_LOCK_TIMEOUT_SECONDS), 1)
  lock = client.lock(f"{_LOCK_PREFIX}/{lock_name}".encode(), ttl=ttl)
  acquired = False
  refresh_task = None
  owner_task = asyncio.current_task()
  refresh_error: BaseException | None = None

  async def refresh_lock():
    nonlocal refresh_error
    while True:
      await asyncio.sleep(max(ttl / 3, 5))
      try:
        await lock.refresh()
        if not await lock.is_acquired():
          raise RuntimeError("Etcd lock ownership was lost")
      except asyncio.CancelledError:
        raise
      except BaseException as error:
        refresh_error = error
        if owner_task is not None:
          _LOCK_FAILURES[owner_task] = error
          owner_task.cancel()
        return

  try:
    acquired = await lock.acquire(timeout=timeout)
    if not acquired:
      raise CustomException(ErrorDesc.STATUS_ERR, "应用配额正在由其他请求更新，请稍后重试")
    refresh_task = asyncio.create_task(refresh_lock())
    try:
      yield client
      if refresh_error is not None:
        raise CustomException(
          ErrorDesc.SYNC_FAILED,
          "应用配额锁续租失败，本次操作已安全中止",
        ) from refresh_error
    except asyncio.CancelledError as cancellation:
      if refresh_error is not None:
        raise CustomException(
          ErrorDesc.SYNC_FAILED,
          "应用配额锁续租失败，本次操作已安全中止",
        ) from refresh_error
      raise cancellation
  finally:
    if refresh_task is not None:
      refresh_task.cancel()
      try:
        await refresh_task
      except asyncio.CancelledError:
        pass
      except Exception as error:
        logger.warning("Quota lock refresh failed lock=%s: %s", lock_name, error)
    if acquired:
      try:
        await lock.release()
      except Exception as error:
        logger.warning("Quota lock release failed lock=%s: %s", lock_name, error)
    if owner_task is not None:
      _LOCK_FAILURES.pop(owner_task, None)
    await client.close()


@asynccontextmanager
async def application_quota_lock(app_name: str) -> AsyncIterator[Any]:
  async with _distributed_lock(f"application/{app_name}") as client:
    yield client


@asynccontextmanager
async def upload_part_lock(
  app_name: str,
  object_key: str,
  part_number: int,
) -> AsyncIterator[Any]:
  async with _distributed_lock(
    f"part/{app_name}/{object_key}/{part_number}"
  ) as client:
    yield client


async def _read_dict(key: str, client: Any) -> dict:
  return await etcd_op.pull_from_etcd_by_key(key, client=client)


async def get_observed_usage_bytes(app_name: str, client: Any = None) -> int:
  """Read the monotonic cross-region logical usage tracked in Etcd."""
  own_client = client is None
  if own_client:
    client = await etcd_op.get_etcd_client()
  try:
    state = _normalize_state(await _read_dict(_state_key(app_name), client))
    return max(int(state.get("observed_usage_bytes") or 0), 0)
  finally:
    if own_client:
      await client.close()


async def get_usage_aggregate(app_name: str, client: Any = None) -> dict[str, Any]:
  """Read the replicated logical-usage aggregate without contacting MinIO."""
  own_client = client is None
  if own_client:
    client = await etcd_op.get_etcd_client()
  try:
    state = _normalize_state(await _read_dict(_state_key(app_name), client))
    active = max(int(state.get("active_usage_bytes") or 0), 0)
    observed = max(int(state.get("observed_usage_bytes") or 0), 0)
    initialized = bool(state.get("logical_usage_initialized"))
    return {
      "usage_bytes": active if initialized else observed,
      "active_usage_bytes": active,
      "observed_usage_bytes": observed,
      "deleted_retained_bytes": max(int(state.get("deleted_retained_bytes") or 0), 0),
      "updated_at": _parse_datetime(state.get("observed_usage_updated_at")),
      "initialized": initialized,
    }
  finally:
    if own_client:
      await client.close()


async def _delete_key(key: str, client: Any) -> None:
  await client.delete(_full_key(key))


async def _new_session_lease(client: Any):
  ttl = max(int(settings.APPLICATION_QUOTA_RESERVATION_TTL_SECONDS), 60)
  return await client.lease(ttl)


def _is_no_such_upload_error(error: Exception) -> bool:
  values = [
    getattr(error, "code", ""),
    getattr(error, "message", ""),
    str(error),
  ]
  normalized = " ".join(str(value) for value in values if value).lower()
  return "nosuchupload" in normalized or "no such upload" in normalized


def _same_reservation_identity(current: dict, expected: dict) -> bool:
  return all(
    str(current.get(field) or "") == str(expected.get(field) or "")
    for field in ("api_key_id", "object_key", "upload_id")
  )


async def _cleanup_minio_reservations(
  app_name: str,
  reservations: list[dict],
) -> list[dict]:
  """Return only reservations whose MPU is absent or was aborted."""
  if not reservations:
    return []
  from src.core.minio_op import get_minio_client
  from src.modules.storage import crud as storage_crud

  async def abort_one(data: dict) -> bool:
    upload_id = str(data.get("upload_id") or "")
    object_key = str(data.get("object_key") or "")
    source_server = str(data.get("source_server") or "")
    # A process may stop before MinIO returned an upload ID. No addressable
    # multipart upload exists in that state, so the reservation can be removed.
    if not upload_id:
      return True
    if not object_key or not source_server:
      logger.warning(
        "Expired multipart cleanup retained incomplete reservation app=%s "
        "object=%s source=%s",
        app_name,
        object_key,
        source_server,
      )
      return False
    try:
      server = await storage_crud.read_minio_server_by_region_name(source_server)
      if not server:
        logger.warning(
          "Expired multipart cleanup retained reservation because source is "
          "unknown app=%s object=%s source=%s",
          app_name,
          object_key,
          source_server,
        )
        return False
      access_key, secret_key = storage_crud.plain_minio_credentials(server)
      minio_client = get_minio_client(
        server.host,
        server.minio_port,
        access_key,
        secret_key,
      )
      await asyncio.to_thread(
        minio_client._abort_multipart_upload,
        app_name,
        object_key,
        upload_id,
      )
      return True
    except Exception as error:
      if _is_no_such_upload_error(error):
        return True
      logger.warning(
        "Expired multipart abort failed app=%s object=%s source=%s: %s",
        app_name,
        object_key,
        source_server,
        error,
      )
      return False

  results = await asyncio.gather(*(abort_one(data) for data in reservations))
  return [data for data, success in zip(reservations, results) if success]


async def _delete_session_if_matches(
  app_name: str,
  reservation: dict,
  client: Any,
) -> bool:
  """CAS-delete an expired session without deleting a concurrently changed key."""
  object_key = str(reservation.get("object_key") or "")
  if not object_key:
    return False
  key = _session_key(app_name, object_key)
  full_key = _full_key(key)
  try:
    for attempt in range(etcd_op.CAS_MAX_RETRIES):
      session, mod_revision = await etcd_op.pull_from_etcd_by_key_with_rev(
        key,
        client=client,
      )
      if mod_revision is None:
        return True
      if not isinstance(session, dict) or not _same_reservation_identity(
        session,
        reservation,
      ):
        logger.warning(
          "Expired quota session identity changed; retaining reservation "
          "app=%s object=%s",
          app_name,
          object_key,
        )
        return False
      status, _ = await client.transaction(
        compare=[client.transactions.mod(full_key) == mod_revision],
        success=[client.transactions.delete(full_key)],
        failure=[],
      )
      if status:
        return True
      await asyncio.sleep(0.05 * (attempt + 1))
  except Exception as error:
    logger.warning(
      "Expired quota session cleanup failed app=%s object=%s: %s",
      app_name,
      object_key,
      error,
    )
    return False
  return False


async def _reconcile_state_locked(
  app_name: str,
  client: Any,
  *,
  usage_loader: Callable[[], Awaitable[int]] | None = None,
  observed_usage: int | None = None,
) -> tuple[dict, list[dict]]:
  now = utc_now()
  current = _normalize_state(await _read_dict(_state_key(app_name), client))
  cleanup_now = [
    value for value in current["reservations"].values()
    if isinstance(value, dict) and _reservation_needs_cleanup(value, now)
  ]
  must_refresh = bool(cleanup_now) or not _usage_is_fresh(current, now)
  refreshed_usage = observed_usage
  if must_refresh and refreshed_usage is None:
    if usage_loader is None:
      raise CustomException(
        ErrorDesc.MINIO_ACCESS_FAILED,
        "存储用量尚未刷新，无法安全清理上传预留",
      )
    refreshed_usage = max(int(await usage_loader()), 0)

  cleanup_attempt_id = secrets.token_urlsafe(18)
  pending: dict[str, dict] = {}

  def mutator(raw: dict) -> dict:
    # merge_update_etcd_key may retry the mutator after a failed CAS. Only
    # clean up reservations marked by the successful attempt; a concurrent
    # part upload may have renewed one between attempts.
    pending.clear()
    state = _normalize_state(raw)
    if refreshed_usage is not None:
      # There is no object-deletion API today, so confirmed logical usage can
      # only grow. During replication lag, max(per-region scan) may temporarily
      # be lower than the already confirmed cross-region total.
      state["observed_usage_bytes"] = max(
        int(state.get("observed_usage_bytes") or 0),
        int(refreshed_usage),
        0,
      )
      state["observed_usage_updated_at"] = now.isoformat()
      if not state["logical_usage_initialized"]:
        # Seed the active logical counter from the reconciled high-water mark.
        # A regional MinIO scan can lag replication and report less than Etcd
        # already confirmed, so initializing from `refreshed_usage` here could
        # temporarily admit an upload beyond the application's hard quota.
        state["active_usage_bytes"] = max(
          int(state.get("active_usage_bytes") or 0),
          int(state["observed_usage_bytes"]),
        )
        state["logical_usage_initialized"] = True
    for reservation_id, data in list(state["reservations"].items()):
      if not isinstance(data, dict):
        state["reservations"].pop(reservation_id, None)
        continue
      if not _reservation_needs_cleanup(data, now):
        continue
      data.update({
        "status": "cleanup_pending",
        "cleanup_attempt_id": cleanup_attempt_id,
        "cleanup_attempted_at": now.isoformat(),
      })
      pending[reservation_id] = dict(data)
    return state

  state = await etcd_op.merge_update_etcd_key(
    _state_key(app_name),
    mutator,
    client=client,
  )
  pending_values = list(pending.values())
  if not pending_values:
    return _normalize_state(state), []

  aborted = await _cleanup_minio_reservations(app_name, pending_values)
  session_cleaned = []
  for data in aborted:
    if await _delete_session_if_matches(app_name, data, client):
      session_cleaned.append(data)
  if not session_cleaned:
    return _normalize_state(state), []

  cleaned: dict[str, dict] = {}

  def remove_mutator(raw: dict) -> dict:
    cleaned.clear()
    current_state = _normalize_state(raw)
    for expected in session_cleaned:
      reservation_id = str(expected.get("object_key") or "")
      stored = current_state["reservations"].get(reservation_id)
      if (
        isinstance(stored, dict)
        and stored.get("status") == "cleanup_pending"
        and stored.get("cleanup_attempt_id") == cleanup_attempt_id
        and _same_reservation_identity(stored, expected)
      ):
        cleaned[reservation_id] = dict(stored)
        current_state["reservations"].pop(reservation_id, None)
    return current_state

  state = await etcd_op.merge_update_etcd_key(
    _state_key(app_name),
    remove_mutator,
    client=client,
  )
  return _normalize_state(state), list(cleaned.values())


async def reserve_upload(
  *,
  app_name: str,
  api_key_id: str,
  object_key: str,
  source_server: str,
  declared_size_bytes: int,
  content_type: str = "application/octet-stream",
  quota_loader: Callable[[Any], Awaitable[int]],
  usage_loader: Callable[[], Awaitable[int]],
) -> UploadReservation:
  if declared_size_bytes <= 0:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "size_bytes 必须大于 0")
  if not api_key_id:
    raise CustomException(ErrorDesc.API_KEY_INVALID, "API-KEY 缺少稳定标识")
  now = utc_now()
  expires_at = _new_expiry(now)
  reservation_id = object_key

  async with application_quota_lock(app_name) as client:
    state, _ = await _reconcile_state_locked(
      app_name,
      client,
      usage_loader=usage_loader,
    )
    # Read the authoritative quota while holding the same global APP lock as
    # quota updates. A remote node's Mongo watch may still lag behind Etcd.
    quota_bytes = max(int(await quota_loader(client)), 1)
    created: dict[str, Any] = {}

    def mutator(raw: dict) -> dict:
      current = _normalize_state(raw)
      reservations = current["reservations"]
      if reservation_id in reservations:
        raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "上传预留已存在")
      max_active = max(
        int(settings.APPLICATION_QUOTA_MAX_ACTIVE_RESERVATIONS),
        1,
      )
      if len(reservations) >= max_active:
        raise CustomException(
          ErrorDesc.STATUS_ERR,
          "当前 APP 的活动上传任务过多，请稍后重试",
        )
      active_reserved = sum(
        max(int(item.get("declared_size_bytes") or 0), 0)
        for item in reservations.values()
        if isinstance(item, dict) and _reservation_counts_toward_quota(item, now)
      )
      observed = max(int(current.get("observed_usage_bytes") or 0), 0)
      active_usage = max(int(current.get("active_usage_bytes") or 0), 0)
      usage_for_quota = active_usage if current["logical_usage_initialized"] else observed
      if usage_for_quota + active_reserved + declared_size_bytes > quota_bytes:
        raise CustomException(
          ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED,
          _QUOTA_EXCEEDED_REASON,
        )
      created.update({
        "api_key_id": api_key_id,
        "object_key": object_key,
        "upload_id": "",
        "source_server": source_server,
        "declared_size_bytes": int(declared_size_bytes),
        "content_type": content_type or "application/octet-stream",
        "status": "initializing",
        "created_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
      })
      reservations[reservation_id] = dict(created)
      return current

    await etcd_op.merge_update_etcd_key(
      _state_key(app_name),
      mutator,
      client=client,
    )
  return _reservation_from_dict(app_name, created)


async def activate_reservation(
  reservation: UploadReservation,
  upload_id: str,
) -> UploadReservation:
  now = utc_now()
  expires_at = _new_expiry(now)
  app_name = reservation.app_name
  session = {
    "version": _STATE_VERSION,
    "app_name": app_name,
    "api_key_id": reservation.api_key_id,
    "object_key": reservation.object_key,
    "upload_id": upload_id,
    "source_server": reservation.source_server,
    "declared_size_bytes": reservation.declared_size_bytes,
    "content_type": reservation.content_type,
    "status": "active",
    "parts": {},
    "created_at": now.isoformat(),
    "expires_at": expires_at.isoformat(),
  }
  async with application_quota_lock(app_name) as client:
    try:
      await etcd_op.merge_update_etcd_key(
        _session_key(app_name, reservation.object_key),
        lambda current: session if not current else current,
        client=client,
      )

      def mutator(raw: dict) -> dict:
        state = _normalize_state(raw)
        stored = state["reservations"].get(reservation.object_key)
        if not isinstance(stored, dict):
          raise CustomException(ErrorDesc.STATUS_ERR, "上传预留不存在或已过期")
        _validate_identity(
          {**stored, "upload_id": upload_id},
          api_key_id=reservation.api_key_id,
          object_key=reservation.object_key,
          upload_id=upload_id,
        )
        stored.update({
          "upload_id": upload_id,
          "status": "active",
          "expires_at": expires_at.isoformat(),
        })
        return state

      await etcd_op.merge_update_etcd_key(
        _state_key(app_name),
        mutator,
        client=client,
      )
    except Exception:
      try:
        await _delete_key(_session_key(app_name, reservation.object_key), client)
      except Exception:
        pass
      raise
  return UploadReservation(
    **{
      **reservation.__dict__,
      "upload_id": upload_id,
      "expires_at": expires_at,
      "status": "active",
    }
  )


async def cancel_reservation(reservation: UploadReservation) -> None:
  async with application_quota_lock(reservation.app_name) as client:
    def mutator(raw: dict) -> dict:
      state = _normalize_state(raw)
      stored = state["reservations"].get(reservation.object_key)
      if isinstance(stored, dict) and stored.get("api_key_id") == reservation.api_key_id:
        state["reservations"].pop(reservation.object_key, None)
      return state

    await etcd_op.merge_update_etcd_key(
      _state_key(reservation.app_name),
      mutator,
      client=client,
    )
    await _delete_key(
      _session_key(reservation.app_name, reservation.object_key),
      client,
    )


def _validate_session(
  raw: dict,
  *,
  app_name: str,
  api_key_id: str,
  object_key: str,
  upload_id: str,
  allowed_statuses: set[str],
) -> dict:
  if not raw or raw.get("app_name") != app_name:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "上传会话不存在或已过期")
  _validate_identity(
    raw,
    api_key_id=api_key_id,
    object_key=object_key,
    upload_id=upload_id,
  )
  if _reservation_expired(raw, utc_now()):
    raise CustomException(ErrorDesc.STATUS_ERR, "上传会话已过期")
  if str(raw.get("status") or "") not in allowed_statuses:
    raise CustomException(ErrorDesc.STATUS_ERR, "上传会话当前状态不允许此操作")
  parts = raw.get("parts")
  raw["parts"] = parts if isinstance(parts, dict) else {}
  return raw


async def get_upload_session(
  client: Any,
  *,
  app_name: str,
  api_key_id: str,
  object_key: str,
  upload_id: str,
) -> UploadReservation:
  """Read an active upload session after validating its full ownership."""
  session = _validate_session(
    dict(await _read_dict(_session_key(app_name, object_key), client)),
    app_name=app_name,
    api_key_id=api_key_id,
    object_key=object_key,
    upload_id=upload_id,
    allowed_statuses={"active"},
  )
  return _reservation_from_dict(app_name, session)


async def _touch_state_reservation(
  client: Any,
  reservation: UploadReservation,
  expires_at: datetime,
) -> None:
  def mutator(raw: dict) -> dict:
    state = _normalize_state(raw)
    stored = state["reservations"].get(reservation.object_key)
    if not isinstance(stored, dict):
      raise CustomException(ErrorDesc.STATUS_ERR, "上传预留不存在或已过期")
    _validate_identity(
      stored,
      api_key_id=reservation.api_key_id,
      object_key=reservation.object_key,
      upload_id=reservation.upload_id,
    )
    if str(stored.get("status") or "") == "cleanup_pending":
      raise CustomException(
        ErrorDesc.STATUS_ERR,
        "上传预留已进入过期清理，不能继续上传",
      )
    stored["expires_at"] = expires_at.isoformat()
    return state

  await etcd_op.merge_update_etcd_key(
    _state_key(reservation.app_name),
    mutator,
    client=client,
  )


async def prepare_part(
  client: Any,
  *,
  app_name: str,
  api_key_id: str,
  object_key: str,
  upload_id: str,
  part_number: int,
  size_bytes: int,
) -> PreparedPart:
  operation_id = secrets.token_urlsafe(18)
  expires_at = _new_expiry()
  previous: dict[str, Any] | None = None
  stored_session: dict[str, Any] = {}

  def mutator(raw: dict) -> dict:
    nonlocal previous
    session = _validate_session(
      dict(raw),
      app_name=app_name,
      api_key_id=api_key_id,
      object_key=object_key,
      upload_id=upload_id,
      allowed_statuses={"active"},
    )
    key = str(part_number)
    existing = session["parts"].get(key)
    previous = dict(existing) if isinstance(existing, dict) else None
    total = sum(
      max(int(item.get("size_bytes") or 0), 0)
      for number, item in session["parts"].items()
      if number != key and isinstance(item, dict)
    ) + size_bytes
    declared = max(int(session.get("declared_size_bytes") or 0), 0)
    if total > declared:
      raise CustomException(
        ErrorDesc.INVALID_PARAMS,
        "上传分片累计大小超过初始化声明的 size_bytes",
      )
    session["parts"][key] = {
      "size_bytes": int(size_bytes),
      "etag": "",
      "status": "uploading",
      "operation_id": operation_id,
    }
    session["expires_at"] = expires_at.isoformat()
    stored_session.clear()
    stored_session.update(session)
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(app_name, object_key),
    mutator,
    client=client,
  )
  reservation = _reservation_from_dict(app_name, stored_session)
  await _touch_state_reservation(client, reservation, expires_at)
  return PreparedPart(reservation, operation_id, previous)


async def commit_part(
  client: Any,
  prepared: PreparedPart,
  part_number: int,
  etag: str,
) -> None:
  reservation = prepared.reservation
  expires_at = _new_expiry()

  def mutator(raw: dict) -> dict:
    session = _validate_session(
      dict(raw),
      app_name=reservation.app_name,
      api_key_id=reservation.api_key_id,
      object_key=reservation.object_key,
      upload_id=reservation.upload_id,
      allowed_statuses={"active"},
    )
    part = session["parts"].get(str(part_number))
    if not isinstance(part, dict) or part.get("operation_id") != prepared.operation_id:
      raise CustomException(ErrorDesc.STATUS_ERR, "上传分片状态已变化，请重试")
    part.update({"etag": etag, "status": "uploaded"})
    session["expires_at"] = expires_at.isoformat()
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(reservation.app_name, reservation.object_key),
    mutator,
    client=client,
  )
  await _touch_state_reservation(client, reservation, expires_at)


async def rollback_part(
  client: Any,
  prepared: PreparedPart,
  part_number: int,
) -> None:
  reservation = prepared.reservation
  current = await _read_dict(
    _session_key(reservation.app_name, reservation.object_key),
    client,
  )
  if not current:
    return

  def mutator(raw: dict) -> dict:
    session = _validate_session(
      dict(raw),
      app_name=reservation.app_name,
      api_key_id=reservation.api_key_id,
      object_key=reservation.object_key,
      upload_id=reservation.upload_id,
      allowed_statuses={"active"},
    )
    key = str(part_number)
    part = session["parts"].get(key)
    if isinstance(part, dict) and part.get("operation_id") == prepared.operation_id:
      if prepared.previous is None:
        session["parts"].pop(key, None)
      else:
        session["parts"][key] = dict(prepared.previous)
    return session

  try:
    await etcd_op.merge_update_etcd_key(
      _session_key(reservation.app_name, reservation.object_key),
      mutator,
      client=client,
    )
  except CustomException:
    return


async def prepare_completion(
  client: Any,
  *,
  app_name: str,
  api_key_id: str,
  object_key: str,
  upload_id: str,
  parts: list[tuple[int, str]],
) -> PreparedCompletion:
  submitted = {str(number): etag.strip('"') for number, etag in parts}
  if len(submitted) != len(parts):
    raise CustomException(ErrorDesc.INVALID_PARAMS, "完成上传的分片编号不能重复")
  expires_at = _new_expiry()
  prepared: dict[str, Any] = {}
  completed_result: dict[str, Any] | None = None
  recovering = False
  current = await _read_dict(_session_key(app_name, object_key), client)
  validated = _validate_session(
    dict(current),
    app_name=app_name,
    api_key_id=api_key_id,
    object_key=object_key,
    upload_id=upload_id,
    allowed_statuses={"active", "completing", "completed"},
  )
  already_completed = validated["status"] == "completed"
  terminal_lease = (
    await _new_session_lease(client)
    if already_completed
    else None
  )

  def validate_parts(session: dict) -> None:
    if any(
      not isinstance(item, dict) or item.get("status") != "uploaded"
      for item in session["parts"].values()
    ):
      raise CustomException(ErrorDesc.STATUS_ERR, "仍有分片正在上传，请稍后重试")
    stored_etags = {
      number: str(item.get("etag") or "").strip('"')
      for number, item in session["parts"].items()
      if isinstance(item, dict)
    }
    if stored_etags != submitted:
      raise CustomException(ErrorDesc.INVALID_PARAMS, "完成上传的分片与已上传记录不一致")
    total = sum(
      max(int(item.get("size_bytes") or 0), 0)
      for item in session["parts"].values()
      if isinstance(item, dict)
    )
    if total != int(session.get("declared_size_bytes") or 0):
      raise CustomException(
        ErrorDesc.INVALID_PARAMS,
        "上传分片总大小与初始化声明的 size_bytes 不一致",
      )

  if not already_completed:
    validate_parts(validated)
    # Renew the authoritative APP reservation before making the session
    # eligible for a MinIO complete side effect.
    await _touch_state_reservation(
      client,
      _reservation_from_dict(app_name, validated),
      expires_at,
    )

  def mutator(raw: dict) -> dict:
    nonlocal completed_result, recovering
    session = _validate_session(
      dict(raw),
      app_name=app_name,
      api_key_id=api_key_id,
      object_key=object_key,
      upload_id=upload_id,
      allowed_statuses={"active", "completing", "completed"},
    )
    if session["status"] == "completed":
      session["expires_at"] = expires_at.isoformat()
      completed_result = dict(session.get("result") or {})
      prepared.clear()
      prepared.update(session)
      return session
    recovering = session["status"] == "completing"
    validate_parts(session)
    session["status"] = "completing"
    session["expires_at"] = expires_at.isoformat()
    prepared.clear()
    prepared.update(session)
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(app_name, object_key),
    mutator,
    client=client,
    lease=terminal_lease,
  )
  return PreparedCompletion(
    reservation=_reservation_from_dict(app_name, prepared),
    already_completed=completed_result is not None,
    recovering=recovering,
    result=completed_result,
  )


async def restore_active_session(client: Any, reservation: UploadReservation) -> None:
  def mutator(raw: dict) -> dict:
    session = dict(raw)
    if session and session.get("status") == "completing":
      session["status"] = "active"
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(reservation.app_name, reservation.object_key),
    mutator,
    client=client,
  )


async def record_completed_session(
  client: Any,
  reservation: UploadReservation,
  result: dict[str, Any],
) -> None:
  lease = await _new_session_lease(client)

  def mutator(raw: dict) -> dict:
    session = _validate_session(
      dict(raw),
      app_name=reservation.app_name,
      api_key_id=reservation.api_key_id,
      object_key=reservation.object_key,
      upload_id=reservation.upload_id,
      allowed_statuses={"completing", "completed"},
    )
    session["status"] = "completed"
    session["result"] = dict(result)
    session["expires_at"] = _new_expiry().isoformat()
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(reservation.app_name, reservation.object_key),
    mutator,
    client=client,
    lease=lease,
  )


async def finalize_completed_session(
  client: Any,
  reservation: UploadReservation,
) -> None:
  now = utc_now()

  def mutator(raw: dict) -> dict:
    state = _normalize_state(raw)
    stored = state["reservations"].get(reservation.object_key)
    if not isinstance(stored, dict):
      return state
    _validate_identity(
      stored,
      api_key_id=reservation.api_key_id,
      object_key=reservation.object_key,
      upload_id=reservation.upload_id,
    )
    state["observed_usage_bytes"] = (
      max(int(state.get("observed_usage_bytes") or 0), 0)
      + reservation.declared_size_bytes
    )
    state["active_usage_bytes"] = (
      max(int(state.get("active_usage_bytes") or 0), 0)
      + reservation.declared_size_bytes
    )
    state["logical_usage_initialized"] = True
    state["observed_usage_updated_at"] = now.isoformat()
    state["reservations"].pop(reservation.object_key, None)
    return state

  await etcd_op.merge_update_etcd_key(
    _state_key(reservation.app_name),
    mutator,
    client=client,
  )


async def mark_object_deleted(app_name: str, size_bytes: int) -> None:
  """Release logical quota without physically deleting the MinIO object."""
  size = max(int(size_bytes), 0)
  async with application_quota_lock(app_name) as client:
    await mark_object_deleted_locked(app_name, size, client)


async def mark_object_deleted_locked(app_name: str, size_bytes: int, client: Any) -> None:
  """Release logical quota while a caller already owns the App lock."""
  size = max(int(size_bytes), 0)

  def mutator(raw: dict) -> dict:
    state = _normalize_state(raw)
    active = max(int(state.get("active_usage_bytes") or 0), 0)
    if not state["logical_usage_initialized"]:
      active = max(int(state.get("observed_usage_bytes") or 0), 0)
      state["logical_usage_initialized"] = True
    state["active_usage_bytes"] = max(active - size, 0)
    state["deleted_retained_bytes"] = max(int(state.get("deleted_retained_bytes") or 0), 0) + size
    return state

  await etcd_op.merge_update_etcd_key(_state_key(app_name), mutator, client=client)


async def restore_deleted_object(app_name: str, size_bytes: int, quota_bytes: int) -> None:
  """Restore logical quota under the same lock as upload reservations."""
  size = max(int(size_bytes), 0)
  now = utc_now()
  async with application_quota_lock(app_name) as client:
    await restore_deleted_object_locked(app_name, size, quota_bytes, client, now=now)


async def restore_deleted_object_locked(
  app_name: str,
  size_bytes: int,
  quota_bytes: int,
  client: Any,
  *,
  now: datetime | None = None,
) -> None:
  """Reclaim logical quota while a caller already owns the App lock."""
  size = max(int(size_bytes), 0)
  now = now or utc_now()

  def mutator(raw: dict) -> dict:
    state = _normalize_state(raw)
    active = max(int(state.get("active_usage_bytes") or 0), 0)
    if not state["logical_usage_initialized"]:
      active = max(int(state.get("observed_usage_bytes") or 0), 0)
      state["logical_usage_initialized"] = True
    reserved = sum(
      max(int(entry.get("declared_size_bytes") or 0), 0)
      for entry in state["reservations"].values()
      if isinstance(entry, dict) and _reservation_counts_toward_quota(entry, now)
    )
    if active + reserved + size > max(int(quota_bytes), 1):
      raise CustomException(ErrorDesc.QUOTA_RESTORE_EXCEEDED)
    state["active_usage_bytes"] = active + size
    state["deleted_retained_bytes"] = max(int(state.get("deleted_retained_bytes") or 0) - size, 0)
    return state

  await etcd_op.merge_update_etcd_key(_state_key(app_name), mutator, client=client)
async def prepare_abort(
  client: Any,
  *,
  app_name: str,
  api_key_id: str,
  object_key: str,
  upload_id: str,
) -> PreparedAbort:
  expires_at = _new_expiry()
  prepared: dict[str, Any] = {}
  already_aborted = False
  recovering = False
  current = await _read_dict(_session_key(app_name, object_key), client)
  validated = _validate_session(
    dict(current),
    app_name=app_name,
    api_key_id=api_key_id,
    object_key=object_key,
    upload_id=upload_id,
    allowed_statuses={"active", "aborting", "aborted"},
  )
  initially_aborted = validated["status"] == "aborted"
  initially_recovering = validated["status"] == "aborting"
  if not initially_aborted and not initially_recovering and any(
    isinstance(part, dict) and part.get("status") == "uploading"
    for part in validated["parts"].values()
  ):
    raise CustomException(
      ErrorDesc.STATUS_ERR,
      "仍有分片正在上传，请稍后再中止上传",
    )
  terminal_lease = (
    await _new_session_lease(client)
    if initially_aborted
    else None
  )
  if not initially_aborted:
    # Keep the APP-level reservation authoritative until abort reaches MinIO
    # and its terminal state is durably recorded.
    await _touch_state_reservation(
      client,
      _reservation_from_dict(app_name, validated),
      expires_at,
    )

  def mutator(raw: dict) -> dict:
    nonlocal already_aborted, recovering
    session = _validate_session(
      dict(raw),
      app_name=app_name,
      api_key_id=api_key_id,
      object_key=object_key,
      upload_id=upload_id,
      allowed_statuses={"active", "aborting", "aborted"},
    )
    already_aborted = session["status"] == "aborted"
    recovering = session["status"] == "aborting"
    session["expires_at"] = expires_at.isoformat()
    if already_aborted:
      prepared.clear()
      prepared.update(session)
      return session
    if not recovering and any(
      isinstance(part, dict) and part.get("status") == "uploading"
      for part in session["parts"].values()
    ):
      raise CustomException(
        ErrorDesc.STATUS_ERR,
        "仍有分片正在上传，请稍后再中止上传",
      )
    session["status"] = "aborting"
    prepared.clear()
    prepared.update(session)
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(app_name, object_key),
    mutator,
    client=client,
    lease=terminal_lease,
  )
  return PreparedAbort(
    reservation=_reservation_from_dict(app_name, prepared),
    already_aborted=already_aborted,
    recovering=recovering,
  )


async def restore_aborted_session(client: Any, reservation: UploadReservation) -> None:
  def mutator(raw: dict) -> dict:
    session = dict(raw)
    if session and session.get("status") == "aborting":
      session["status"] = "active"
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(reservation.app_name, reservation.object_key),
    mutator,
    client=client,
  )


async def record_aborted_session(client: Any, reservation: UploadReservation) -> None:
  lease = await _new_session_lease(client)

  def mutator(raw: dict) -> dict:
    session = _validate_session(
      dict(raw),
      app_name=reservation.app_name,
      api_key_id=reservation.api_key_id,
      object_key=reservation.object_key,
      upload_id=reservation.upload_id,
      allowed_statuses={"active", "completing", "aborting", "aborted"},
    )
    session["status"] = "aborted"
    session["expires_at"] = _new_expiry().isoformat()
    return session

  await etcd_op.merge_update_etcd_key(
    _session_key(reservation.app_name, reservation.object_key),
    mutator,
    client=client,
    lease=lease,
  )


async def finalize_aborted_session(client: Any, reservation: UploadReservation) -> None:
  def mutator(raw: dict) -> dict:
    state = _normalize_state(raw)
    stored = state["reservations"].get(reservation.object_key)
    if isinstance(stored, dict):
      _validate_identity(
        stored,
        api_key_id=reservation.api_key_id,
        object_key=reservation.object_key,
        upload_id=reservation.upload_id,
      )
      state["reservations"].pop(reservation.object_key, None)
    return state

  await etcd_op.merge_update_etcd_key(
    _state_key(reservation.app_name),
    mutator,
    client=client,
  )
@asynccontextmanager
async def quota_update_guard(
  app_name: str,
  observed_usage_bytes: int | None = None,
  *,
  usage_loader: Callable[[], Awaitable[int]] | None = None,
) -> AsyncIterator[tuple[int, int]]:
  """Serialize quota changes with admission and include all active reservations."""
  async with application_quota_lock(app_name) as client:
    if observed_usage_bytes is None:
      if usage_loader is None:
        raise CustomException(
          ErrorDesc.MINIO_ACCESS_FAILED,
          "存储用量尚未刷新，无法安全更新配额",
        )
      observed_usage_bytes = max(int(await usage_loader()), 0)
    state, _ = await _reconcile_state_locked(
      app_name,
      client,
      observed_usage=max(int(observed_usage_bytes), 0),
    )
    now = utc_now()
    active_reserved = sum(
      max(int(item.get("declared_size_bytes") or 0), 0)
      for item in state["reservations"].values()
      if isinstance(item, dict) and _reservation_counts_toward_quota(item, now)
    )
    # Reconciliation is intentionally monotonic: a lagging regional scan may
    # report less data than Etcd has already confirmed. Admission must use the
    # reconciled state, not the stale loader value that initiated this refresh.
    yield max(int(state.get("observed_usage_bytes") or 0), 0), active_reserved
