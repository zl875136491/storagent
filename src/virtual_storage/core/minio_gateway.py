"""专用 MinIO 池。超时视为结果不明，由调用方保留预留并沿原会话核实。"""
from __future__ import annotations

from minio.error import S3Error

from src.virtual_storage.core.exception import UnknownMinioResult
from src.virtual_storage.settings import get_settings


def _normalize_etag(value: str) -> str:
  return str(value or "").strip().strip('"')


def _as_unknown(exc: Exception) -> UnknownMinioResult:
  return UnknownMinioResult(f"对象存储结果不明: {type(exc).__name__}")


class MinioGateway:
  def __init__(self, client=None):
    self._client = client

  def raw(self):
    if self._client is None:
      from minio import Minio
      settings = get_settings()
      if not settings.minio_endpoint:
        raise UnknownMinioResult("未配置专用 MinIO 池")
      self._client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
      )
    return self._client

  def ensure_bucket(self, bucket: str) -> None:
    client = self.raw()
    try:
      if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc

  def create_multipart(self, bucket: str, storage_key: str, upload_id: str, content_type: str) -> str:
    headers = {
      "Content-Type": content_type or "application/octet-stream",
      "x-amz-meta-vs-upload-id": upload_id,
    }
    try:
      return self.raw()._create_multipart_upload(bucket, storage_key, headers)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc

  def upload_part(self, bucket: str, storage_key: str, minio_upload_id: str, part_number: int, data: bytes) -> str:
    try:
      etag = self.raw()._upload_part(
        bucket, storage_key, data, None, minio_upload_id, part_number,
      )
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc
    return _normalize_etag(etag)

  def list_server_parts(self, bucket: str, storage_key: str, minio_upload_id: str) -> list[dict]:
    try:
      result = self.raw()._list_parts(bucket, storage_key, minio_upload_id)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc
    if isinstance(result, tuple):
      result = result[0]
    if hasattr(result, "parts"):
      result = result.parts
    parts = []
    for part in result or []:
      number = int(getattr(part, "part_number"))
      size = int(getattr(part, "size", 0) or 0)
      parts.append({
        "part_number": number,
        "etag": _normalize_etag(getattr(part, "etag")),
        "size": size,
      })
    return sorted(parts, key=lambda item: item["part_number"])

  def complete(self, bucket: str, storage_key: str, minio_upload_id: str, parts: list[dict]):
    from minio.datatypes import Part
    payload = [Part(part["part_number"], part["etag"]) for part in parts]
    try:
      return self.raw()._complete_multipart_upload(
        bucket, storage_key, minio_upload_id, payload,
      )
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc

  def abort(self, bucket: str, storage_key: str, minio_upload_id: str) -> None:
    try:
      self.raw()._abort_multipart_upload(bucket, storage_key, minio_upload_id)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc

  def stat(self, bucket: str, storage_key: str):
    try:
      return self.raw().stat_object(bucket, storage_key)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc

  def find_upload_id(self, bucket: str, storage_key: str) -> list[str]:
    try:
      result = self.raw()._list_multipart_uploads(bucket, prefix=storage_key)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc
    uploads = getattr(result, "uploads", None)
    if uploads is None and isinstance(result, tuple):
      uploads = getattr(result[0], "uploads", result[0])
    found = []
    for item in uploads or []:
      key = getattr(item, "object_name", None) or getattr(item, "key", None)
      upload_id = getattr(item, "upload_id", None)
      if key == storage_key and upload_id:
        found.append(upload_id)
    return found

  def get_range(self, bucket: str, storage_key: str, offset: int, length: int) -> bytes:
    response = None
    try:
      response = self.raw().get_object(bucket, storage_key, offset=offset, length=length)
      return response.read()
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc
    finally:
      if response is not None:
        response.close()
        response.release_conn()

  def put_bytes(self, bucket: str, storage_key: str, data: bytes, content_type: str) -> str:
    import io
    try:
      result = self.raw().put_object(
        bucket, storage_key, io.BytesIO(data), len(data),
        content_type=content_type or "application/octet-stream",
      )
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc
    return _normalize_etag(getattr(result, "etag", "") or "")

  def remove(self, bucket: str, storage_key: str) -> None:
    try:
      self.raw().remove_object(bucket, storage_key)
    except S3Error:
      raise
    except Exception as exc:
      raise _as_unknown(exc) from exc


def missing_upload(exc: Exception) -> bool:
  return isinstance(exc, S3Error) and exc.code in {"NoSuchUpload", "NoSuchKey"}


def missing_object(exc: Exception) -> bool:
  return isinstance(exc, S3Error) and exc.code in {"NoSuchKey", "NoSuchObject"}
