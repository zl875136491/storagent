"""管理员应急下载：短时、跨节点、严格一次性的流式对象下载。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import secrets
import unicodedata
from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote

from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, ValidationError

from src.configs.configs import settings
from src.core import audit
from src.core.etcd_op import get_etcd_client
from src.core.exception import CustomException, ErrorDesc
from src.core.minio_op import get_minio_client
from src.modules.storage import crud as storage_crud
from src.utils.helpers import utc_now
from src.utils.logger import logger


_READ_CHUNK = 1024 * 1024
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
# 独立于 /storagent/ 控制面 Watch，避免短时令牌制造无意义同步事件。
_ETCD_TOKEN_PREFIX = b"/storagent-ephemeral/one-time-download/"
_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")


class _DownloadGrant(BaseModel):
  version: int = 1
  region_name: str = Field(..., min_length=1, max_length=128)
  bucket: str = Field(..., min_length=3, max_length=63)
  object_key: str = Field(..., min_length=1, max_length=1024)
  filename: str = Field(..., min_length=1, max_length=1024)
  content_type: str = Field(default="application/octet-stream", max_length=255)
  size: int = Field(default=0, ge=0)
  actor: str = Field(default="-", max_length=128)
  expires_at: datetime


def _aware_utc(value: datetime) -> datetime:
  return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _token_key(token: str) -> bytes:
  digest = hashlib.sha256(token.encode("utf-8")).hexdigest().encode("ascii")
  return _ETCD_TOKEN_PREFIX + digest


def _download_ttl() -> int:
  return min(max(int(settings.ONE_TIME_DOWNLOAD_TTL_SECONDS), 30), 900)


def _validate_object_request(bucket: str, object_key: str) -> tuple[str, str]:
  bucket = bucket.strip()
  if not _BUCKET_PATTERN.fullmatch(bucket) or ".." in bucket:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "存储桶名称不合法")
  if not object_key or len(object_key.encode("utf-8")) > 1024 or "\x00" in object_key:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "对象键不合法")
  return bucket, object_key


def _find_cached_object(
  inventory: list[dict[str, Any]],
  bucket: str,
  object_key: str,
) -> dict[str, Any] | None:
  bucket_entry = next((
    item for item in inventory
    if item.get("name") in {bucket, f"Bucket: {bucket}"}
  ), None)
  if not bucket_entry:
    return None

  def walk(nodes: Any, parts: list[str]) -> dict[str, Any] | None:
    if not isinstance(nodes, list):
      return None
    for node in nodes:
      if not isinstance(node, dict) or not isinstance(node.get("name"), str):
        continue
      next_parts = [*parts, node["name"]]
      children = node.get("children")
      if isinstance(children, list):
        found = walk(children, next_parts)
        if found:
          return found
      elif "/".join(next_parts) == object_key:
        return node
    return None

  return walk(bucket_entry.get("files"), [])


async def _inventory_for_server(server: Any) -> list[dict[str, Any]]:
  now = utc_now()
  cache = await storage_crud.read_server_file_details_cache(str(server.id))
  if cache and _aware_utc(cache.expires_at) > now:
    return cache.data

  # Only refresh when the ten-minute snapshot is absent or expired.
  from src.modules.storage import service as storage_service

  details = await storage_service.get_server_details(server.id)
  return details["data"]


def _is_object_not_found(error: Exception) -> bool:
  code = str(getattr(error, "code", "") or "").lower()
  status = getattr(error, "status", None)
  return status == 404 or code in {
    "nosuchkey", "nosuchobject", "nosuchbucket", "notfound", "xminoerrordescnotfound",
  }


def _raise_minio_error(error: Exception, bucket: str, object_key: str) -> None:
  if _is_object_not_found(error):
    raise CustomException(
      ErrorDesc.OBJECT_NOT_FOUND,
      {"bucket": bucket, "object_key": object_key},
    ) from error
  logger.warning(f"管理员应急下载访问 MinIO 失败: {type(error).__name__}: {error}")
  raise CustomException(
    ErrorDesc.DOWNLOAD_SOURCE_UNAVAILABLE,
    "目标服务点暂时无法读取该对象，请稍后重试",
  ) from error


async def _store_grant(grant: _DownloadGrant, ttl: int) -> str:
  client = None
  try:
    client = await get_etcd_client()
    payload = json.dumps(
      grant.model_dump(mode="json"),
      ensure_ascii=False,
      separators=(",", ":"),
    ).encode("utf-8")
    for _ in range(3):
      token = secrets.token_urlsafe(32)
      key = _token_key(token)
      lease = await client.lease(ttl)
      stored, _ = await client.transaction(
        compare=[client.transactions.create(key) == 0],
        success=[client.transactions.put(key, payload, lease=lease)],
        failure=[],
      )
      if stored:
        return token
      await lease.revoke()
    raise RuntimeError("无法分配唯一下载令牌")
  except CustomException:
    raise
  except Exception as error:
    logger.warning(f"一次性下载令牌写入 Etcd 失败: {error}")
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "无法创建一次性下载凭据，请稍后重试",
    ) from error
  finally:
    if client is not None:
      try:
        await client.close()
      except Exception:
        pass


async def issue_one_time_download(
  minio_server_id: Any,
  bucket: str,
  object_key: str,
  actor: str,
) -> dict[str, Any]:
  """Validate a cached inventory row and issue a distributed capability URL token."""
  bucket, object_key = _validate_object_request(bucket, object_key)
  server = await storage_crud.read_minio_server_by_id(minio_server_id)
  if not server:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")

  inventory = await _inventory_for_server(server)
  if not _find_cached_object(inventory, bucket, object_key):
    raise CustomException(
      ErrorDesc.OBJECT_NOT_FOUND,
      "对象不在当前服务器文件清单中，请刷新清单后重试",
    )

  access_key, secret_key = storage_crud.plain_minio_credentials(server)
  client = get_minio_client(server.host, server.minio_port, access_key, secret_key)
  try:
    stat = await asyncio.to_thread(client.stat_object, bucket, object_key)
  except Exception as error:
    _raise_minio_error(error, bucket, object_key)

  filename = PurePosixPath(object_key.rstrip("/")).name or "download"
  region_name = str(getattr(getattr(server, "region", None), "name", "") or "")
  if not region_name:
    raise CustomException(
      ErrorDesc.DOWNLOAD_SOURCE_UNAVAILABLE,
      "下载源服务点缺少稳定的区域标识",
    )
  ttl = _download_ttl()
  expires_at = utc_now() + timedelta(seconds=ttl)
  grant = _DownloadGrant(
    region_name=region_name,
    bucket=bucket,
    object_key=object_key,
    filename=filename,
    content_type=getattr(stat, "content_type", None) or "application/octet-stream",
    size=max(int(getattr(stat, "size", 0) or 0), 0),
    actor=actor or "-",
    expires_at=expires_at,
  )
  token = await _store_grant(grant, ttl)
  audit.audit(
    "storage.object.one_time_download.issue",
    actor=actor,
    resource=f"{region_name}/{bucket}/{object_key}",
    detail={"expires_in_seconds": ttl},
  )
  return {
    "token": token,
    "expires_at": expires_at,
    "expires_in_seconds": ttl,
    "filename": filename,
  }


def _content_disposition(filename: str) -> str:
  cleaned = filename.replace("\r", "").replace("\n", "") or "download"
  fallback = unicodedata.normalize("NFKD", cleaned).encode("ascii", "ignore").decode("ascii")
  fallback = re.sub(r"[^A-Za-z0-9._ -]", "_", fallback).strip(" .") or "download"
  fallback = fallback[:180].replace('"', "_")
  return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(cleaned, safe='')}"


def _close_minio_response(response: Any) -> None:
  try:
    response.close()
  finally:
    release = getattr(response, "release_conn", None)
    if release:
      release()


def invalid_link_error() -> CustomException:
  return CustomException(
    ErrorDesc.ONE_TIME_DOWNLOAD_INVALID,
    "链接已过期、已使用或不存在",
  )


async def _restore_claimed_grant(
  key: bytes,
  payload: bytes,
  expires_at: datetime,
) -> bool:
  """Restore a claimed grant without ever extending its original expiry."""
  remaining_ttl = int((_aware_utc(expires_at) - utc_now()).total_seconds())
  if remaining_ttl < 1:
    return False

  client = None
  lease = None
  try:
    client = await get_etcd_client()
    lease = await client.lease(remaining_ttl)
    restored, _ = await client.transaction(
      compare=[client.transactions.create(key) == 0],
      success=[client.transactions.put(key, payload, lease=lease)],
      failure=[],
    )
    if not restored:
      await lease.revoke()
    return bool(restored)
  except Exception as error:
    logger.warning(f"一次性下载令牌恢复失败: {error}")
    return False
  finally:
    if client is not None:
      try:
        await client.close()
      except Exception:
        pass


def _close_open_task_response(task: asyncio.Task) -> None:
  """Close a response returned by a worker after its waiter was cancelled."""
  try:
    response = task.result()
  except BaseException:
    return
  try:
    _close_minio_response(response)
  except Exception as error:
    logger.warning(f"取消下载后关闭 MinIO 响应失败: {error}")


async def redeem_one_time_download(token: str) -> StreamingResponse:
  """Atomically claim the grant before opening exactly one MinIO response."""
  if not _TOKEN_PATTERN.fullmatch(token):
    raise invalid_link_error()

  client = None
  minio_response = None
  claim_task: asyncio.Task | None = None
  open_task: asyncio.Task | None = None
  grant: _DownloadGrant | None = None
  grant_payload: bytes | None = None
  grant_key: bytes | None = None
  claimed = False
  handed_off = False
  try:
    client = await get_etcd_client()
    key = _token_key(token)
    grant_key = key
    snapshot = await client.get(key)
    if not snapshot:
      raise invalid_link_error()
    grant_payload = snapshot.value
    try:
      grant = _DownloadGrant.model_validate_json(grant_payload)
    except (ValidationError, ValueError, TypeError):
      await client.delete(key)
      raise invalid_link_error()
    if _aware_utc(grant.expires_at) <= utc_now():
      await client.delete(key)
      raise invalid_link_error()

    claim_task = asyncio.create_task(
      client.transaction(
        compare=[client.transactions.mod(key) == snapshot.mod_revision],
        success=[client.transactions.delete(key)],
        failure=[],
      )
    )
    try:
      claimed, _ = await asyncio.shield(claim_task)
    except asyncio.CancelledError as cancellation:
      # Resolve the shielded transaction before unwinding. If Etcd committed
      # the delete, finally can restore it; if it did not, create==0 is never run.
      try:
        claimed, _ = await asyncio.shield(claim_task)
      except BaseException as error:
        logger.warning(f"取消下载时无法确认 Etcd claim 结果: {error}")
      raise cancellation
    if not claimed:
      raise invalid_link_error()

    server = await storage_crud.read_minio_server_by_region_name(grant.region_name)
    if not server:
      raise CustomException(
        ErrorDesc.DOWNLOAD_SOURCE_UNAVAILABLE,
        "下载源服务点配置不存在",
      )
    access_key, secret_key = storage_crud.plain_minio_credentials(server)
    minio_client = get_minio_client(
      server.host,
      server.minio_port,
      access_key,
      secret_key,
    )
    try:
      # Shield the worker so cancellation cannot discard a response that still
      # needs to be closed when the blocking SDK call eventually returns.
      open_task = asyncio.create_task(
        asyncio.to_thread(
          minio_client.get_object,
          grant.bucket,
          grant.object_key,
        )
      )
      minio_response = await asyncio.shield(open_task)
    except asyncio.CancelledError:
      raise
    except Exception as error:
      _raise_minio_error(error, grant.bucket, grant.object_key)

    response_headers = getattr(minio_response, "headers", {}) or {}
    content_type = response_headers.get("Content-Type") or grant.content_type
    content_length = response_headers.get("Content-Length") or str(grant.size)

    async def stream():
      try:
        while True:
          chunk = await asyncio.to_thread(minio_response.read, _READ_CHUNK)
          if not chunk:
            break
          yield chunk
      finally:
        await asyncio.to_thread(_close_minio_response, minio_response)

    audit.audit(
      "storage.object.one_time_download.consume",
      actor=grant.actor,
      resource=f"{grant.region_name}/{grant.bucket}/{grant.object_key}",
    )
    response = StreamingResponse(
      stream(),
      media_type=content_type,
      headers={
        "Content-Disposition": _content_disposition(grant.filename),
        "Content-Length": str(content_length),
        "Cache-Control": "private, no-store, max-age=0",
        "Pragma": "no-cache",
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
      },
    )
    handed_off = True
    return response
  except (CustomException, asyncio.CancelledError):
    raise
  except Exception as error:
    logger.warning(f"一次性下载令牌读取/消费 Etcd 失败: {error}")
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "无法验证一次性下载凭据，请稍后重试",
    ) from error
  finally:
    if not handed_off:
      if minio_response is not None:
        try:
          await asyncio.to_thread(_close_minio_response, minio_response)
        except BaseException as error:
          logger.warning(f"开始下载前关闭 MinIO 响应失败: {error}")
      elif open_task is not None:
        if open_task.done():
          _close_open_task_response(open_task)
        else:
          open_task.add_done_callback(_close_open_task_response)

      if claimed and grant_key is not None and grant_payload is not None and grant is not None:
        # Shield restoration so the first cancellation still gives Etcd a
        # chance to put the grant back. A second cancellation may stop waiting,
        # but the independent task continues with its own client.
        restore_task = asyncio.create_task(
          _restore_claimed_grant(grant_key, grant_payload, grant.expires_at)
        )
        try:
          await asyncio.shield(restore_task)
        except asyncio.CancelledError:
          # The restore task remains alive because it is shielded, while the
          # request cancellation must remain observable to Starlette.
          raise
        except Exception as error:
          logger.warning(f"一次性下载取消时等待令牌恢复失败: {error}")

    if client is not None:
      try:
        await client.close()
      except Exception:
        pass
