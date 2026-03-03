from typing import List
from src.modules.storage.model import MinioServer
from src.modules.public.model import Region
from src.core.exception import CustomException, ErrorDesc
from loguru import logger

async def create_minio_server(
  region: Region,
  name: str,
  host: str,
  port: int,
  access_key: str,
  secret_key: str) -> MinioServer:
  """
  创建 Minio 服务器

  Args:
    region: 区域
    name: 服务器名称
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    MinioServer: Minio 服务器
  """

  master_minio_server = await read_master_minio_server()
  if master_minio_server:
    master = False
  else:
    master = True
  minio_server = MinioServer(
    region=region,
    name=name,
    host=host,
    port=port,
    master=master,
    access_key=access_key,
    secret_key=secret_key
  )
  await minio_server.save()
  return minio_server

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

async def read_minio_server_by_fqdn(host: str, port: int) -> MinioServer | None:
  """
  根据 FQDN 获取 Minio 服务器
  """
  return await MinioServer.find_one(
    MinioServer.host == host,
    MinioServer.port == port
  )

async def read_minio_server_list() -> List[MinioServer]:
  return await MinioServer.find_all(fetch_links=True).to_list()