from bson import ObjectId
from typing import List

from src.modules.public.model import Region
from src.core.exception import CustomException, ErrorDesc
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.modules.storage.model import MinioServer
from src.core.minio_op import test_minio_server

async def _connect_minio_server(
  host: str,
  port: int,
  access_key: str,
  secret_key: str):
  """
  连接 Minio 服务器

  Args:
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥
  """
  test_minio_server(host, port, access_key, secret_key)
  return None

async def create_minio_server(
  region: str | ObjectId,
  name: str,
  host: str,
  port: int,
  access_key: str,
  secret_key: str) -> MinioServer:
  """
  创建 Minio 服务器

  Args:
    region: 区域ID
    name: 服务器名称
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    MinioServer: Minio 服务器
  """
  await _connect_minio_server(host, port, access_key, secret_key)
  region_obj = await public_crud.read_region_by_id(region)
  if not region_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND)
  return await storage_crud.create_minio_server(
    region=region_obj,
    name=name,
    host=host,
    port=port,
    access_key=access_key,
    secret_key=secret_key
  )

async def get_minio_server_list() -> dict[str, List[MinioServer]]:
  """
  获取 Minio 服务器列表

  Returns:
    List[MinioServer]: Minio 服务器列表
  """
  minio_server_objs = await storage_crud.read_minio_server_list()
  return dict[str, List[MinioServer]](data=minio_server_objs)