from typing import List
from bson import ObjectId
from src.modules.storage.model import MinioServer
from src.modules.public.model import Region
from src.modules.public.model import Application
from src.modules.storage.model import MinioBucket
from src.core.exception import CustomException, ErrorDesc
from src.core.crypto import encrypt_secret, minio_server_plain_credentials
from loguru import logger
from src.utils.helpers import try_to_obj_id
from src.configs.configs import settings

async def create_minio_server(
  region: Region,
  name: str,
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
    host=host,
    server_port=server_port,
    minio_port=minio_port,
    master=master,
    access_key=encrypt_secret(access_key),
    secret_key=encrypt_secret(secret_key),
    replicate_weight=replicate_weight
  )
  try:
    await minio_server.save()
  except Exception as e:
    logger.error(f"Minio Server Create Failed: {str(e)}")
    # raise CustomException(ErrorDesc.MINIO_CREATE_SERVER_FAILED, str(e))
  return minio_server

async def update_minio_server(
  minio_server: MinioServer,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int) -> MinioServer:
  """
  更新 Minio 服务器（凭证落库前加密）
  """
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
  获取 Minio 服务器名称列表
  """
  server_names = []
  server_objs = await read_minio_server_list()
  for server_obj in server_objs:
    server_name = server_obj.name
    server_names.append(server_name)
  return server_names

async def read_minio_server_priorities() -> List[int]:
  """
  获取 Minio 服务器优先级列表
  """
  server_priorities = {}
  server_objs = await read_minio_server_list()
  for server_obj in server_objs:
    server_name = server_obj.name
    server_priority = server_obj.replicate_weight
    server_priorities[server_name] = server_priority
  return server_priorities

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
  批量创建 Minio 存储桶
  """
  minio_server_objs = await read_minio_server_list()
  for minio_server_obj in minio_server_objs:
    minio_bucket = MinioBucket(
      region=minio_server_obj.region,
      app=app,
      server=minio_server_obj,
      name=app.name
    )
    try:
      await minio_bucket.save()
    except Exception as e:
      raise CustomException(ErrorDesc.MINIO_CREATE_BUCKET_FAILED, str(e))
  