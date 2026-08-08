from typing import List
from bson import ObjectId
from beanie.operators import In
from src.modules.storage.model import (
  MinioServer,
  ServerFileDetailsCache,
  StorageOperation,
)
from src.modules.public.model import Region
from src.modules.public.model import Application
from src.modules.storage.model import MinioBucket
from src.core.exception import CustomException, ErrorDesc
from src.core.crypto import encrypt_secret, minio_server_plain_credentials
from loguru import logger
from src.utils.helpers import try_to_obj_id
from src.configs.configs import settings
from dataclasses import dataclass
from datetime import datetime
from typing import Any
import json
import zlib
from uuid import uuid4


_CACHE_CHUNK_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class ServerFileDetailsSnapshot:
  data: list[dict[str, Any]]
  fetched_at: datetime
  expires_at: datetime
  generation: str


def _encode_server_file_details(data: list[dict[str, Any]]) -> bytes:
  serialized = json.dumps(
    data,
    ensure_ascii=False,
    separators=(",", ":"),
    default=str,
  ).encode("utf-8")
  return zlib.compress(serialized)


def _decode_server_file_details(payload: bytes) -> list[dict[str, Any]]:
  decoded = json.loads(zlib.decompress(payload).decode("utf-8"))
  if not isinstance(decoded, list) or any(not isinstance(item, dict) for item in decoded):
    raise ValueError("服务器文件详情缓存格式不合法")
  return decoded

async def create_minio_server(
  region: Region,
  name: str,
  domain: str,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int) -> MinioServer:
  """
  创建 Minio 服务器（凭证落库前加密）
  """
  if region.name == settings.REGION:
    master = True
  else:
    master = False
  minio_server = MinioServer(
    region=region,
    name=name,
    domain=domain,
    host=host,
    server_port=server_port,
    minio_port=minio_port,
    master=master,
    access_key=encrypt_secret(access_key),
    secret_key=encrypt_secret(secret_key),
    replicate_weight=replicate_weight
  )
  await minio_server.save()
  return minio_server

async def update_minio_server(
  minio_server: MinioServer,
  domain: str,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int) -> MinioServer:
  """
  更新 Minio 服务器（凭证落库前加密）
  """
  minio_server.domain = domain
  minio_server.host = host
  minio_server.server_port = server_port
  minio_server.minio_port = minio_port
  minio_server.access_key = encrypt_secret(access_key)
  minio_server.secret_key = encrypt_secret(secret_key)
  minio_server.replicate_weight = replicate_weight
  await minio_server.save()
  return minio_server

def plain_minio_credentials(minio_server: MinioServer) -> tuple[str, str]:
  """解密 Mongo 中的 MinIO 凭证。"""
  return minio_server_plain_credentials(minio_server.access_key, minio_server.secret_key)

async def read_master_minio_server() -> MinioServer | None:
  """
  获取主 Minio 服务器
  """
  return await MinioServer.find_one(
    MinioServer.master == True,
    fetch_links=True
  )

async def read_minio_server_by_region(region: Region) -> MinioServer | None:
  """
  根据区域获取 Minio 服务器
  """
  return await MinioServer.find_one(
    MinioServer.region.id == region.id
  )

async def read_minio_server_by_region_name(region_name: str) -> MinioServer | None:
  """
  根据区域名称获取 Minio 服务器
  """
  from src.modules.public import crud as public_crud
  region_obj = await public_crud.read_region_by_name(region_name)
  if not region_obj:
    return None
  return await read_minio_server_by_region(region_obj)

async def read_minio_server_by_fqdn(host: str, minio_port: int) -> MinioServer | None:
  """
  根据 FQDN 获取 Minio 服务器
  """
  return await MinioServer.find_one(
    MinioServer.host == host,
    MinioServer.minio_port == minio_port
  )

async def read_minio_server_by_id(id: str | ObjectId) -> MinioServer | None:
  """
  根据 ID 获取 Minio 服务器
  """
  id = try_to_obj_id(id)
  return await MinioServer.find_one(MinioServer.id == id, fetch_links=True)

async def read_minio_server_list() -> List[MinioServer]:
  return await MinioServer.find_all(fetch_links=True).to_list()

async def read_minio_server_names() -> List[str]:
  """
  获取可供 mc 使用的区域别名列表。
  """
  server_names = []
  server_objs = await read_minio_server_list()
  for server_obj in server_objs:
    # setup_mc_aliases uses Region.name as the stable alias. MinioServer.name
    # is user-facing and may differ from the region identifier.
    server_name = (
      getattr(getattr(server_obj, "region", None), "name", None)
      or server_obj.name
    )
    server_names.append(server_name)
  return server_names


async def delete_expired_server_file_details(now: datetime) -> int:
  result = await ServerFileDetailsCache.get_motor_collection().delete_many({
    "expires_at": {"$lte": now},
  })
  return int(result.deleted_count)


async def read_server_file_details_cache(
  server_id: str,
) -> ServerFileDetailsSnapshot | None:
  latest = await ServerFileDetailsCache.find(
    ServerFileDetailsCache.server_id == server_id
  ).sort("-fetched_at").limit(1).to_list()
  if not latest:
    return None
  head = latest[0]
  chunks = await ServerFileDetailsCache.find(
    ServerFileDetailsCache.server_id == server_id,
    ServerFileDetailsCache.generation == head.generation,
  ).sort("chunk_index").to_list()
  if not chunks or any(item.chunk_index != index for index, item in enumerate(chunks)):
    await ServerFileDetailsCache.get_motor_collection().delete_many({
      "server_id": server_id,
      "generation": head.generation,
    })
    return None
  try:
    data = _decode_server_file_details(b"".join(item.payload for item in chunks))
  except (ValueError, TypeError, zlib.error, json.JSONDecodeError, UnicodeDecodeError):
    await ServerFileDetailsCache.get_motor_collection().delete_many({
      "server_id": server_id,
      "generation": head.generation,
    })
    return None
  return ServerFileDetailsSnapshot(
    data=data,
    fetched_at=head.fetched_at,
    expires_at=head.expires_at,
    generation=head.generation,
  )


async def write_server_file_details_cache(
  server_id: str,
  data: list[dict[str, Any]],
  fetched_at: datetime,
  expires_at: datetime,
) -> ServerFileDetailsSnapshot:
  generation = uuid4().hex
  payload = _encode_server_file_details(data)
  parts = [
    payload[offset:offset + _CACHE_CHUNK_BYTES]
    for offset in range(0, len(payload), _CACHE_CHUNK_BYTES)
  ] or [b""]
  chunks = [
    ServerFileDetailsCache(
      server_id=server_id,
      generation=generation,
      chunk_index=index,
      payload=part,
      fetched_at=fetched_at,
      expires_at=expires_at,
    )
    for index, part in enumerate(parts)
  ]
  await ServerFileDetailsCache.insert_many(chunks)
  # Keep a concurrently written newer generation; remove this server's older snapshots.
  await ServerFileDetailsCache.get_motor_collection().delete_many({
    "server_id": server_id,
    "generation": {"$ne": generation},
    "fetched_at": {"$lte": fetched_at},
  })
  return ServerFileDetailsSnapshot(
    data=data,
    fetched_at=fetched_at,
    expires_at=expires_at,
    generation=generation,
  )


async def delete_server_file_details_cache(server_id: str) -> int:
  result = await ServerFileDetailsCache.get_motor_collection().delete_many({
    "server_id": server_id,
  })
  return int(result.deleted_count)


async def create_storage_operation(
  *,
  kind: str,
  server: str,
  actor: str,
  bucket: str = "",
) -> StorageOperation:
  operation = StorageOperation(
    kind=kind,
    server=server,
    bucket=bucket,
    actor=actor,
  )
  await operation.insert()
  return operation


async def read_active_storage_operation(
  kind: str,
  server: str,
) -> StorageOperation | None:
  return await StorageOperation.find_one(
    StorageOperation.kind == kind,
    StorageOperation.server == server,
    In(StorageOperation.status, ["queued", "running"]),
  )


async def read_latest_storage_operation(
  kind: str,
  server: str,
) -> StorageOperation | None:
  items = await StorageOperation.find(
    StorageOperation.kind == kind,
    StorageOperation.server == server,
  ).sort("-created_at").limit(1).to_list()
  return items[0] if items else None


async def list_storage_operations(limit: int = 20) -> list[StorageOperation]:
  return await StorageOperation.find_all().sort("-created_at").limit(limit).to_list()

async def create_minio_bucket(
  region: Region,
  app: Application,
  server: MinioServer,
  name: str) -> MinioBucket:
  """
  创建 Minio 存储桶
  """
  minio_bucket = MinioBucket(
    region=region,
    app=app,
    server=server,
    name=name
  )
  await minio_bucket.save()
  return minio_bucket

async def bulk_create_minio_bucket(
  app: Application) -> List[MinioBucket]:
  """
  补齐应用在各站点的 MinioBucket 记录。

  授权流程允许失败后重试，因此这里必须能接续一次只写入了部分站点的操作。
  """
  minio_server_objs = await read_minio_server_list()
  existing_buckets = await MinioBucket.find(
    MinioBucket.name == app.name,
    fetch_links=True,
  ).to_list()
  buckets_by_region = {
    str(bucket.region.id): bucket
    for bucket in existing_buckets
  }
  buckets = []
  for minio_server_obj in minio_server_objs:
    region_key = str(minio_server_obj.region.id)
    if region_key in buckets_by_region:
      buckets.append(buckets_by_region[region_key])
      continue
    minio_bucket = MinioBucket(
      region=minio_server_obj.region,
      app=app,
      server=minio_server_obj,
      name=app.name
    )
    try:
      await minio_bucket.save()
      buckets.append(minio_bucket)
    except Exception as e:
      raise CustomException(ErrorDesc.MINIO_CREATE_BUCKET_FAILED, str(e))
  return buckets
