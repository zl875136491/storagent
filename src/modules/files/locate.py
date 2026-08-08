"""
跨节点对象定位：当本节点 MinIO 不存在对象时，扫描其他服务点并生成下载指引。
"""
import asyncio
from typing import Optional
from urllib.parse import urlencode

from minio.error import S3Error

from src.configs.configs import settings
from src.configs.consts import API_V1_PREFIX
from src.core.exception import CustomException, ErrorDesc
from src.core.minio_op import get_minio_client
from src.modules.storage import crud as storage_crud
from src.modules.storage.model import MinioServer
from src.modules.files import schema as files_schema


def _is_object_not_found(exc: Exception) -> bool:
  if isinstance(exc, S3Error):
    return exc.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket")
  msg = str(exc).lower()
  return "nosuchkey" in msg or "not found" in msg or "does not exist" in msg


def _scheme() -> str:
  scheme = (settings.PUBLIC_SCHEME or "http").strip().lower()
  return "https" if scheme == "https" else "http"


def _build_api_url(host: str, port: int, path: str, params: dict) -> str:
  query = urlencode(params)
  return f"{_scheme()}://{host}:{port}{path}?{query}"


_GATEWAY_SEGMENTS = {
  "beijing": "bj",
  "tianjin": "tj",
  "kunshan": "ks",
  "shenzhen": "sz",
  "hangzhou": "hz",
}


def _server_api_base(server: MinioServer, region_name: str) -> str:
  """Return the public Nginx route; keep IP fallback for unmigrated sites."""
  domain = str(getattr(server, "domain", "") or settings.PUBLIC_DOMAIN or "").strip().rstrip("/")
  segment = _GATEWAY_SEGMENTS.get(region_name)
  if domain and segment:
    return f"{_scheme()}://{domain}/server/{segment}"
  return f"{_scheme()}://{server.host}:{server.server_port}"


def _build_location_item(
  server: MinioServer,
  object_key: str,
  offset: int = 0,
  length: int = 0,
) -> files_schema.ObjectLocationItem:
  region = server.region
  region_name = region.name if region else server.name
  shown_name = region.shown_name if region else server.name
  download_params = {"object_key": object_key, "offset": offset, "length": length}
  base = _server_api_base(server, region_name)
  return files_schema.ObjectLocationItem(
    region=region_name,
    shown_name=shown_name,
    master=server.master,
    endpoint=base,
    stat_url=f"{base}{API_V1_PREFIX}/files/object/stat",
    stat_method="POST",
    stat_body={"object_key": object_key},
    download_url=f"{base}{API_V1_PREFIX}/files/object/download?{urlencode(download_params)}",
  )


async def _stat_on_server(server: MinioServer, bucket: str, object_key: str):
  """确认对象由指定 MinIO 节点持有并返回 stat，不存在/超时则返回 None。"""

  def _do():
    from src.modules.storage import crud as storage_crud
    access_key, secret_key = storage_crud.plain_minio_credentials(server)
    client = get_minio_client(server.host, server.minio_port, access_key, secret_key)
    objects = client.list_objects(bucket, prefix=object_key, recursive=True)
    if not any(item.object_name == object_key for item in objects):
      return None
    return client.stat_object(bucket, object_key)

  try:
    return await asyncio.wait_for(
      asyncio.to_thread(_do),
      timeout=settings.OBJECT_LOCATE_TIMEOUT,
    )
  except (S3Error, asyncio.TimeoutError, Exception):
    return None


async def find_object_locations(
  app_name: str,
  object_key: str,
  offset: int = 0,
  length: int = 0,
) -> files_schema.ObjectLocateResponse:
  """
  扫描所有已知 MinIO 服务点，返回对象存在的节点及下载指引 URL
  """
  bucket = app_name
  key = object_key.strip()
  servers = await storage_crud.read_minio_server_list()
  local_server = await storage_crud.read_minio_server_by_region_name(settings.REGION)

  local_exists = False
  if local_server:
    local_stat = await _stat_on_server(local_server, bucket, key)
    local_exists = local_stat is not None

  tasks = [_stat_on_server(s, bucket, key) for s in servers]
  results = await asyncio.gather(*tasks)

  available: list[files_schema.ObjectLocationItem] = []
  for server, stat in zip(servers, results):
    if stat is not None:
      available.append(_build_location_item(server, key, offset, length))

  return files_schema.ObjectLocateResponse(
    bucket=bucket,
    object_key=key,
    current_region=settings.REGION,
    local_exists=local_exists,
    available_at=available,
  )


def _not_found_local_payload(
  locate: files_schema.ObjectLocateResponse,
) -> dict:
  """构造「本节点不存在」异常的结构化 data"""
  return {
    "bucket": locate.bucket,
    "object_key": locate.object_key,
    "current_region": locate.current_region,
    "available_at": [item.model_dump() for item in locate.available_at],
  }


async def raise_if_not_found_local(
  app_name: str,
  object_key: str,
  offset: int = 0,
  length: int = 0,
) -> None:
  """
  本节点不存在对象时：
  - 若其他节点存在 → 抛出 OBJECT_NOT_FOUND_LOCAL，data 含多个下载指引
  - 若全集群不存在 → 抛出 OBJECT_NOT_FOUND
  """
  locate = await find_object_locations(app_name, object_key, offset, length)
  if locate.local_exists:
    return
  if locate.available_at:
    raise CustomException(ErrorDesc.OBJECT_NOT_FOUND_LOCAL, _not_found_local_payload(locate))
  raise CustomException(ErrorDesc.OBJECT_NOT_FOUND, {
    "bucket": locate.bucket,
    "object_key": locate.object_key,
    "current_region": locate.current_region,
    "available_at": [],
  })


async def stat_object_local(app_name: str, object_key: str):
  """仅在本节点 MinIO 上 stat 对象，不存在则抛出带指引的异常"""
  bucket = app_name
  key = object_key.strip()
  local_server = await storage_crud.read_minio_server_by_region_name(settings.REGION)
  if not local_server:
    local_server = await storage_crud.read_master_minio_server()
  if not local_server:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")

  stat = await _stat_on_server(local_server, bucket, key)
  if stat is None:
    await raise_if_not_found_local(app_name, key)
    return None

  return stat, local_server
