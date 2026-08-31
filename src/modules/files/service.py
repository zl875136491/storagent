import asyncio
import io
import weakref
from typing import Any, Callable, Optional, List

from beanie.exceptions import CollectionWasNotInitialized
from minio.datatypes import Part
from fastapi import UploadFile
from fastapi.responses import Response, StreamingResponse

from src.configs.configs import settings
from src.core.exception import CustomException, ErrorDesc
from src.core.minio_op import get_minio_client
from src.modules.storage import crud as storage_crud
from src.modules.files import schema as files_schema
from src.modules.files import locate as files_locate
from src.modules.files import quota as files_quota
from src.modules.files import crud as files_crud
from src.modules.public import service as public_service
from src.modules.usage.service import record_transfer

_READ_CHUNK = 1024 * 1024
_QUOTA_EXCEEDED_REASON = "APP 存储超出限额，请联系管理员处理"
_upload_part_semaphores = weakref.WeakKeyDictionary()


async def _upload_quota_warning(
  app_name: str,
  *,
  usage_bytes: int,
  declared_size_bytes: int,
) -> dict | None:
  """Best-effort alerting must never make a valid upload unavailable."""
  try:
    from src.modules.public import crud, quota_alert
    application = await crud.read_application_by_name(app_name)
    if application:
      return await quota_alert.evaluate_upload_warning(
        application,
        usage_bytes=usage_bytes,
        declared_size_bytes=declared_size_bytes,
      )
  except Exception as error:
    from src.utils.logger import logger
    logger.warning("上传前配额告警检查失败 app=%s: %s", app_name, error)
  return None


def _upload_part_semaphore() -> asyncio.Semaphore:
  loop = asyncio.get_running_loop()
  limit = max(int(settings.APPLICATION_UPLOAD_MAX_IN_MEMORY_PARTS), 1)
  existing = _upload_part_semaphores.get(loop)
  if existing is None or existing[0] != limit:
    existing = (limit, asyncio.Semaphore(limit))
    _upload_part_semaphores[loop] = existing
  return existing[1]


async def _run_thread_to_completion(call: Callable[[], Any]):
  """Let an in-flight MinIO call settle even if the HTTP task is cancelled."""
  task = asyncio.create_task(asyncio.to_thread(call))
  try:
    result = await asyncio.shield(task)
    files_quota.raise_if_quota_lock_lost()
    return result, None
  except asyncio.CancelledError as cancellation:
    result = await task
    files_quota.raise_if_quota_lock_lost()
    return result, cancellation


def _is_bucket_quota_exceeded(error: Exception) -> bool:
  values = [
    str(error),
    str(getattr(error, "code", "")),
    str(getattr(error, "message", "")),
  ]
  normalized = " ".join(values).lower()
  return (
    "xminioadminbucketquotaexceeded" in normalized
    or "bucket quota exceeded" in normalized
  )


def _is_no_such_upload(error: BaseException) -> bool:
  values = (
    str(error),
    str(getattr(error, "code", "")),
    str(getattr(error, "message", "")),
  )
  normalized = " ".join(values).lower().replace("_", "")
  return "nosuchupload" in normalized or "upload does not exist" in normalized


def _is_no_such_object(error: BaseException) -> bool:
  code = str(getattr(error, "code", "")).lower()
  normalized = f"{code} {error}".lower().replace("_", "")
  return any(value in normalized for value in (
    "nosuchkey",
    "nosuchobject",
    "object does not exist",
  ))


def _raise_minio_write_error(error: Exception) -> None:
  if _is_bucket_quota_exceeded(error):
    raise CustomException(
      ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED,
      _QUOTA_EXCEEDED_REASON,
    )
  raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(error))

def _normalize_etag(etag: str) -> str:
  e = etag.strip()
  if len(e) >= 2 and e[0] == '"' and e[-1] == '"':
    e = e[1:-1]
  return e

def gen_object_key() -> str:
  """
  生成非重复的对象键
  """
  # TODO: 检索所有节点的对象键, 确保不重复
  import uuid
  return str(uuid.uuid4())

async def _get_minio_client_with_server():
  ms = await storage_crud.read_minio_server_by_region_name(settings.REGION)
  if not ms:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  access_key, secret_key = storage_crud.plain_minio_credentials(ms)
  return (
    settings.REGION,
    get_minio_client(ms.host, ms.minio_port, access_key, secret_key),
  )


async def _get_minio_client():
  _server_name, client = await _get_minio_client_with_server()
  return client


async def _get_minio_client_for_server(server_name: str):
  server = await storage_crud.read_minio_server_by_region_name(server_name)
  if not server:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, f"MinioServer: {server_name}")
  access_key, secret_key = storage_crud.plain_minio_credentials(server)
  return get_minio_client(
    server.host,
    server.minio_port,
    access_key,
    secret_key,
  )


async def _recover_completed_result(
  client,
  app_name: str,
  object_key: str,
  declared_size_bytes: int,
) -> dict[str, Any] | None:
  def _stat():
    return client.stat_object(app_name, object_key)

  try:
    metadata = await asyncio.to_thread(_stat)
  except Exception as error:
    if _is_no_such_object(error):
      return None
    raise CustomException(
      ErrorDesc.MINIO_ACCESS_FAILED,
      f"无法确认完成中的对象状态: {error}",
    ) from error
  if int(getattr(metadata, "size", -1)) != declared_size_bytes:
    raise CustomException(
      ErrorDesc.STATUS_ERR,
      "完成中的对象大小与上传声明不一致，已保留配额预留等待人工确认",
    )
  return {
    "etag": getattr(metadata, "etag", None),
    "version_id": getattr(metadata, "version_id", None),
  }


async def multipart_init(
  app_context: dict,
  content_type: str,
  *,
  size_bytes: int,
) -> files_schema.MultipartInitResponse:
  app_name = app_context["app_name"]
  api_key_id = str(app_context.get("api_key_id") or "")

  # Admission reads the replicated logical counter only. A full
  # ``mc du --recursive --versions`` scan belongs to the authority Celery
  # reconciliation task and must never block this request.
  try:
    usage_aggregate = await files_quota.get_usage_aggregate(app_name)
  except CustomException:
    raise
  except Exception as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"读取应用配额聚合失败，上传已安全拒绝: {error}",
    ) from error
  if not usage_aggregate.get("admission_ready"):
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "应用配额聚合尚未初始化，请稍后重试",
    )
  usage_bytes = max(int(usage_aggregate.get("usage_bytes") or 0), 0)

  source_server, client = await _get_minio_client_with_server()
  object_key = gen_object_key()

  async def quota_loader(quota_client) -> int:
    quota_bytes = await public_service.get_application_quota_limit(
      app_name,
      client=quota_client,
    )
    # The global rule may intentionally reserve headroom below the physical
    # application quota. It is evaluated under the same distributed quota lock
    # as the reservation, so concurrent uploads cannot bypass the threshold.
    from src.modules.public import quota_alert
    try:
      rule = await quota_alert.get_rule()
      block_percent = rule.block_percent
    except CollectionWasNotInitialized:
      # Isolated admission tests intentionally omit database initialization.
      # Keep the historical full-quota admission limit in that context.
      block_percent = 100
    return max(int(quota_bytes * block_percent / 100), 1)

  reservation = await files_quota.reserve_upload(
    app_name=app_name,
    api_key_id=api_key_id,
    object_key=object_key,
    source_server=source_server,
    declared_size_bytes=size_bytes,
    content_type=content_type,
    quota_loader=quota_loader,
    usage_loader=None,
  )
  headers = {"Content-Type": content_type}

  def _create():
    return client._create_multipart_upload(app_name, object_key, headers)

  try:
    upload_id, cancellation = await _run_thread_to_completion(_create)
  except BaseException as error:
    await files_quota.cancel_reservation(reservation)
    if isinstance(error, asyncio.CancelledError):
      raise
    _raise_minio_write_error(error)

  if cancellation is not None:
    try:
      await asyncio.to_thread(
        client._abort_multipart_upload,
        app_name,
        object_key,
        upload_id,
      )
    finally:
      await files_quota.cancel_reservation(reservation)
    raise cancellation

  try:
    await files_quota.activate_reservation(reservation, upload_id)
  except BaseException:
    try:
      await asyncio.to_thread(
        client._abort_multipart_upload,
        app_name,
        object_key,
        upload_id,
      )
    except Exception:
      pass
    await files_quota.cancel_reservation(reservation)
    raise

  return files_schema.MultipartInitResponse(
    upload_id=upload_id,
    bucket=app_name,
    object_key=object_key,
    quota_warning=await _upload_quota_warning(
      app_name,
      usage_bytes=usage_bytes,
      declared_size_bytes=size_bytes,
    ),
  )


async def multipart_upload_part(
  app_context: dict,
  object_key: str,
  upload_id: str,
  part_number: int,
  file: UploadFile,
) -> files_schema.MultipartPartResponse:
  app_name = app_context["app_name"]
  if part_number < 1 or part_number > 10000:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "part_number 必须在 1-10000 之间")
  key = object_key.strip()
  api_key_id = str(app_context.get("api_key_id") or "")

  max_part_bytes = max(int(settings.APPLICATION_UPLOAD_MAX_PART_BYTES), 1)
  async with _upload_part_semaphore():
    async with files_quota.upload_part_lock(
      app_name,
      key,
      part_number,
    ) as quota_client:
      reservation = await files_quota.get_upload_session(
        quota_client,
        app_name=app_name,
        api_key_id=api_key_id,
        object_key=key,
        upload_id=upload_id,
      )
      read_limit = min(max_part_bytes, reservation.declared_size_bytes) + 1
      data = await file.read(read_limit)
      if len(data) > max_part_bytes:
        raise CustomException(
          ErrorDesc.UPLOAD_PART_TOO_LARGE,
          f"单个上传分片不能超过 {max_part_bytes} 字节",
        )
      prepared = await files_quota.prepare_part(
        quota_client,
        app_name=app_name,
        api_key_id=api_key_id,
        object_key=key,
        upload_id=upload_id,
        part_number=part_number,
        size_bytes=len(data),
      )
      try:
        client = await _get_minio_client_for_server(
          prepared.reservation.source_server
        )

        def _upload():
          return client._upload_part(app_name, key, data, None, upload_id, part_number)

        etag, cancellation = await _run_thread_to_completion(_upload)
      except BaseException as error:
        if isinstance(error, asyncio.CancelledError):
          raise
        await files_quota.rollback_part(quota_client, prepared, part_number)
        _raise_minio_write_error(error)
      try:
        await files_quota.commit_part(
          quota_client,
          prepared,
          part_number,
          _normalize_etag(etag),
        )
      except BaseException as error:
        if isinstance(error, asyncio.CancelledError):
          raise
        await files_quota.rollback_part(quota_client, prepared, part_number)
        raise CustomException(
          ErrorDesc.SYNC_FAILED,
          "分片已写入 MinIO，但上传状态同步失败，请重试该分片",
        ) from error

  await record_transfer(app_context, "upload", len(data))
  if cancellation is not None:
    raise cancellation
  return files_schema.MultipartPartResponse(
    part_number=part_number,
    etag=_normalize_etag(etag),
  )


async def multipart_complete(
  app_context: dict,
  upload_id: str,
  object_key: str,
  parts: List[files_schema.MultipartPartItem]) -> files_schema.MultipartCompleteResponse:
  """
  完成分片上传
  """
  app_name = app_context["app_name"]
  api_key_id = str(app_context.get("api_key_id") or "")
  parts_sorted = sorted(parts, key=lambda p: p.part_number)
  submitted_parts = [
    Part(part_number=p.part_number, etag=_normalize_etag(p.etag))
    for p in parts_sorted
  ]
  key = object_key.strip()

  async with files_quota.application_quota_lock(app_name) as quota_client:
    prepared = await files_quota.prepare_completion(
      quota_client,
      app_name=app_name,
      api_key_id=api_key_id,
      object_key=key,
      upload_id=upload_id,
      parts=[(part.part_number, part.etag) for part in parts_sorted],
    )
    if prepared.already_completed:
      saved_result = prepared.result or {}
      await files_quota.finalize_completed_session(
        quota_client,
        prepared.reservation,
      )
      return files_schema.MultipartCompleteResponse(
        bucket=app_name,
        object_key=key,
        etag=saved_result.get("etag"),
        version_id=saved_result.get("version_id"),
      )

    client = await _get_minio_client_for_server(
      prepared.reservation.source_server
    )

    def _complete():
      return client._complete_multipart_upload(
        app_name,
        key,
        upload_id,
        submitted_parts,
      )

    saved_result = None
    cancellation = None
    if prepared.recovering:
      saved_result = await _recover_completed_result(
        client,
        app_name,
        key,
        prepared.reservation.declared_size_bytes,
      )
    if saved_result is None:
      try:
        result, cancellation = await _run_thread_to_completion(_complete)
        saved_result = {
          "etag": result.etag,
          "version_id": result.version_id,
        }
      except BaseException as error:
        if isinstance(error, asyncio.CancelledError):
          raise
        if _is_no_such_upload(error):
          saved_result = await _recover_completed_result(
            client,
            app_name,
            key,
            prepared.reservation.declared_size_bytes,
          )
          if saved_result is None:
            await files_quota.record_aborted_session(
              quota_client,
              prepared.reservation,
            )
            await files_quota.finalize_aborted_session(
              quota_client,
              prepared.reservation,
            )
            raise CustomException(
              ErrorDesc.MINIO_ACCESS_FAILED,
              "上传会话已不在 MinIO 中，请重新初始化上传",
            ) from error
        if saved_result is None:
          await files_quota.restore_active_session(
            quota_client,
            prepared.reservation,
          )
          _raise_minio_write_error(error)

    try:
      await files_quota.record_completed_session(
        quota_client,
        prepared.reservation,
        saved_result,
      )
      await files_quota.finalize_completed_session(
        quota_client,
        prepared.reservation,
      )
    except BaseException as error:
      if isinstance(error, asyncio.CancelledError):
        raise
      raise CustomException(
        ErrorDesc.SYNC_FAILED,
        "对象已完成上传，但配额状态同步失败；预留将保留至自动校准",
      ) from error

  if cancellation is not None:
    raise cancellation
  # The catalog is intentionally written after the MinIO and quota commits.
  # A failed catalog write is retried by the v2 migration/reconcile job; it
  # must not make a completed object invisible to the existing v1 API.
  try:
    await files_crud.upsert_completed_object(
      app_name=app_name,
      object_key=key,
      size_bytes=prepared.reservation.declared_size_bytes,
      etag=saved_result.get("etag"),
      version_id=saved_result.get("version_id"),
      # Preserve the type declared when the multipart session was created.
      content_type=prepared.reservation.content_type,
      source_region=prepared.reservation.source_server,
    )
  except Exception as error:
    from src.utils.logger import logger
    logger.warning(f"对象目录写入延迟 app={app_name} object={key}: {error}")
  return files_schema.MultipartCompleteResponse(
    bucket=app_name,
    object_key=key,
    etag=saved_result.get("etag"),
    version_id=saved_result.get("version_id"),
  )


async def multipart_abort(body: files_schema.MultipartAbortRequest,
  app_context: dict) -> dict:
  app_name = app_context["app_name"]
  api_key_id = str(app_context.get("api_key_id") or "")
  b = app_name
  object_key = body.object_key.strip()
  async with files_quota.application_quota_lock(app_name) as quota_client:
    prepared = await files_quota.prepare_abort(
      quota_client,
      app_name=app_name,
      api_key_id=api_key_id,
      object_key=object_key,
      upload_id=body.upload_id,
    )
    reservation = prepared.reservation
    cancellation = None
    if not prepared.already_aborted:
      client = await _get_minio_client_for_server(reservation.source_server)

      def _abort():
        client._abort_multipart_upload(b, object_key, body.upload_id)

      try:
        _result, cancellation = await _run_thread_to_completion(_abort)
      except BaseException as error:
        if isinstance(error, asyncio.CancelledError):
          raise
        if not _is_no_such_upload(error):
          await files_quota.restore_aborted_session(quota_client, reservation)
          raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(error))
      await files_quota.record_aborted_session(quota_client, reservation)
    await files_quota.finalize_aborted_session(quota_client, reservation)

  if cancellation is not None:
    raise cancellation
  return {"bucket": b, "object_key": object_key, "upload_id": body.upload_id, "aborted": True}


async def multipart_list_parts(
  app_context: dict,
  object_key: str,
  upload_id: str,
  part_number_marker: Optional[str],
) -> files_schema.MultipartListPartsResponse:
  app_name = app_context["app_name"]
  api_key_id = str(app_context.get("api_key_id") or "")
  b = app_name
  key = object_key.strip()
  async with files_quota.application_quota_lock(app_name) as quota_client:
    reservation = await files_quota.get_upload_session(
      quota_client,
      app_name=app_name,
      api_key_id=api_key_id,
      object_key=key,
      upload_id=upload_id,
    )
  client = await _get_minio_client_for_server(reservation.source_server)

  def _list():
    return client._list_parts(
      b,
      key,
      upload_id,
      max_parts=1000,
      part_number_marker=part_number_marker,
    )

  try:
    result = await asyncio.to_thread(_list)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

  listed = [
    files_schema.MultipartPartListed(
      part_number=p.part_number,
      etag=p.etag,
      size=p.size,
      last_modified=p.last_modified,
    )
    for p in result.parts
  ]
  return files_schema.MultipartListPartsResponse(
    bucket=result.bucket_name,
    object_key=result.object_name,
    upload_id=upload_id,
    parts=listed,
  )


async def locate_object(
  app_name: str,
  object_key: str,
  offset: int = 0,
  length: int = 0,
) -> files_schema.ObjectLocateResponse:
  """
  查询对象在哪些服务点存在，并生成各节点的 stat / download 指引 URL
  """
  return await files_locate.find_object_locations(app_name, object_key, offset, length)


async def stat_object(
  app_name: str,
  object_key: str,
) -> files_schema.ObjectStatResponse:
  b = app_name
  key = object_key.strip()
  stat, _server = await files_locate.stat_object_local(b, key)
  return files_schema.ObjectStatResponse(
    bucket=b,
    object_key=key,
    size=stat.size,
    etag=stat.etag,
    content_type=stat.content_type,
    last_modified=stat.last_modified,
    region=settings.REGION,
    local=True,
  )


async def download_chunk(
  app_context: dict,
  object_key: str,
  offset: int,
  length: int) -> Response | StreamingResponse:
  """
  分片下载：length>0 时读取固定字节区间；length=0 时从 offset 起读到对象末尾（流式，适合大文件）。
  offset=0 且 length=0 表示整对象流式下载。
  本节点不存在时返回其他服务点的下载指引（见 OBJECT_NOT_FOUND_LOCAL）。
  """
  app_name = app_context["app_name"]
  b = app_name
  key = object_key.strip()
  stat, server = await files_locate.stat_object_local(b, key)
  access_key, secret_key = storage_crud.plain_minio_credentials(server)
  client = get_minio_client(server.host, server.minio_port, access_key, secret_key)

  if length > 0:

    def _read():
      resp = client.get_object(b, key, offset=offset, length=length)
      try:
        data = resp.read()
        h = {k.lower(): v for k, v in resp.headers.items()}
        st = getattr(resp, "status", 200)
        return data, h, st
      finally:
        resp.close()
        resp.release_conn()

    try:
      data, resp_headers, status = await asyncio.to_thread(_read)
    except Exception as e:
      if files_locate._is_object_not_found(e):
        await files_locate.raise_if_not_found_local(b, key, offset, length)
      raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

    out_headers = {}
    if "content-range" in resp_headers:
      out_headers["Content-Range"] = resp_headers["content-range"]
    if "content-length" in resp_headers:
      out_headers["Content-Length"] = resp_headers["content-length"]
    media = resp_headers.get("content-type", "application/octet-stream")
    code = 206 if status == 206 else 200
    await record_transfer(app_context, "download", len(data))
    return Response(
      content=data,
      media_type=media,
      headers=out_headers,
      status_code=code,
    )

  content_type = stat.content_type or "application/octet-stream"

  async def _stream_with_usage():
    # MinIO treats a supplied length=0 as an invalid byte-range request.
    # Omitting length is the SDK contract for streaming from offset to EOF.
    resp = await asyncio.to_thread(
      client.get_object,
      b,
      key,
      offset=offset,
    )
    transferred = 0
    completed = False
    try:
      while True:
        chunk = await asyncio.to_thread(resp.read, _READ_CHUNK)
        if not chunk:
          break
        transferred += len(chunk)
        yield chunk
      completed = True
    finally:
      resp.close()
      resp.release_conn()
      if transferred or completed:
        await record_transfer(app_context, "download", transferred)

  return StreamingResponse(
    _stream_with_usage(),
    media_type=content_type,
  )


# async def upload_file(file: UploadFile) -> dict:
#   """简单整文件上传（非分片），写入默认桶下以原文件名作为 key。"""
#   client = await _get_minio_client()
#   bucket = ""
#   name = (file.filename or "unnamed").strip() or "unnamed"
#   data = await file.read()
#   length = len(data)

#   def _put():
#     return client.put_object(
#       bucket,
#       name,
#       io.BytesIO(data),
#       length,
#       content_type=file.content_type or "application/octet-stream",
#     )

#   try:
#     result = await asyncio.to_thread(_put)
#   except Exception as e:
#     raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

#   return {
#     "bucket": bucket,
#     "object_key": name,
#     "etag": result.etag,
#     "version_id": result.version_id,
#   }
