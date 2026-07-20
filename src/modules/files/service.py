import asyncio
import io
from typing import Optional, List

from minio.datatypes import Part
from fastapi import UploadFile
from fastapi.responses import Response, StreamingResponse

from src.configs.configs import settings
from src.core.exception import CustomException, ErrorDesc
from src.core.minio_op import get_minio_client
from src.modules.storage import crud as storage_crud
from src.modules.files import schema as files_schema
from src.modules.files import locate as files_locate

_READ_CHUNK = 1024 * 1024

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

async def _get_minio_client():
  ms = await storage_crud.read_master_minio_server()
  if not ms:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  access_key, secret_key = storage_crud.plain_minio_credentials(ms)
  return get_minio_client(ms.host, ms.minio_port, access_key, secret_key)


async def multipart_init(
  app_name: str,
  content_type: str) -> files_schema.MultipartInitResponse:
  object_key = gen_object_key()
  client = await _get_minio_client()
  headers = {"Content-Type": content_type}

  def _create():
    return client._create_multipart_upload(app_name, object_key, headers)

  try:
    upload_id = await asyncio.to_thread(_create)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

  return files_schema.MultipartInitResponse(
    upload_id=upload_id,
    bucket=app_name,
    object_key=object_key,
  )


async def multipart_upload_part(
  app_name: str,
  object_key: str,
  upload_id: str,
  part_number: int,
  file: UploadFile,
) -> files_schema.MultipartPartResponse:
  if part_number < 1 or part_number > 10000:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "part_number 必须在 1-10000 之间")
  key = object_key.strip()
  data = await file.read()
  client = await _get_minio_client()

  def _upload():
    return client._upload_part(app_name, key, data, None, upload_id, part_number)

  try:
    etag = await asyncio.to_thread(_upload)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

  return files_schema.MultipartPartResponse(
    part_number=part_number,
    etag=_normalize_etag(etag),
  )


async def multipart_complete(
  app_name: str,
  upload_id: str,
  object_key: str,
  parts: List[files_schema.MultipartPartItem]) -> files_schema.MultipartCompleteResponse:
  """
  完成分片上传
  """
  parts_sorted = sorted(parts, key=lambda p: p.part_number)
  parts = [
    Part(part_number=p.part_number, etag=_normalize_etag(p.etag))
    for p in parts_sorted
  ]
  client = await _get_minio_client()

  def _complete():
    return client._complete_multipart_upload(app_name, object_key, upload_id, parts)

  try:
    result = await asyncio.to_thread(_complete)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

  return files_schema.MultipartCompleteResponse(
    bucket=app_name,
    object_key=object_key,
    etag=result.etag,
    version_id=result.version_id,
  )


async def multipart_abort(body: files_schema.MultipartAbortRequest,
  app_name: str) -> dict:
  b = app_name
  object_key = body.object_key.strip()
  client = await _get_minio_client()

  def _abort():
    client._abort_multipart_upload(b, object_key, body.upload_id)

  try:
    await asyncio.to_thread(_abort)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

  return {"bucket": b, "object_key": object_key, "upload_id": body.upload_id, "aborted": True}


async def multipart_list_parts(
  app_name: str,
  object_key: str,
  upload_id: str,
  part_number_marker: Optional[str],
) -> files_schema.MultipartListPartsResponse:
  b = app_name
  key = object_key.strip()
  client = await _get_minio_client()

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
  stat, server = await files_locate.stat_object_local(b, key)
  region = server.region
  return files_schema.ObjectStatResponse(
    bucket=b,
    object_key=key,
    size=stat.size,
    etag=stat.etag,
    content_type=stat.content_type,
    last_modified=stat.last_modified,
    region=region.name if region else settings.REGION,
    local=True,
  )


async def download_chunk(
  app_name: str,
  object_key: str,
  offset: int,
  length: int) -> Response | StreamingResponse:
  """
  分片下载：length>0 时读取固定字节区间；length=0 时从 offset 起读到对象末尾（流式，适合大文件）。
  offset=0 且 length=0 表示整对象流式下载。
  本节点不存在时返回其他服务点的下载指引（见 OBJECT_NOT_FOUND_LOCAL）。
  """
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
    return Response(
      content=data,
      media_type=media,
      headers=out_headers,
      status_code=code,
    )

  content_type = stat.content_type or "application/octet-stream"

  def _sync_gen():
    resp = client.get_object(b, key, offset=offset, length=0)
    try:
      while True:
        chunk = resp.read(_READ_CHUNK)
        if not chunk:
          break
        yield chunk
    finally:
      resp.close()
      resp.release_conn()

  return StreamingResponse(
    _sync_gen(),
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
