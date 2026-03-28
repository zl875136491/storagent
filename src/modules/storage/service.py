from bson import ObjectId
from typing import Any, List

from src.modules.public.model import Region
from src.core.exception import CustomException, ErrorDesc
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.modules.storage.model import MinioServer
from src.core.minio_op import (
  test_minio_server,
  set_site_alias,
  add_new_site,
  remove_site_alias,
  get_buckets_info,
  get_minio_client
)

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
  # 验证区域
  region_obj = await public_crud.read_region_by_id(region)
  if not region_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND)
  exister_minio_region = await storage_crud.read_minio_server_by_region(region_obj)
  if exister_minio_region:
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "MinioServer.region")
  # 验证是否存在
  existed_minio_server = await storage_crud.read_minio_server_by_fqdn(host, port)
  if existed_minio_server:
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "MinioServer.host:port")
  # 验证连接性
  await _connect_minio_server(host, port, access_key, secret_key)
  # 创建别名
  success, res = await set_site_alias(
    site_name=region_obj.name,
    endpoint=f"{host}:{port}",
    admin_user=access_key,
    admin_password=secret_key
  )
  if not success:
    raise CustomException(ErrorDesc.MINIO_ALIAS_FAILED, res)
  master_minio_server_obj = await storage_crud.read_master_minio_server()
  if master_minio_server_obj:
    master_region_obj: Region = master_minio_server_obj.region
    # 已有主节点, 需要执行加入复制集的操作
    set_success, set_res = await add_new_site(
      master_name=master_region_obj.name,
      site_name=region_obj.name
    )
    if not set_success:
      # 加入复制集失败, 需要删除别名
      remove_success, _ = await remove_site_alias(region_obj.name)
      raise CustomException(ErrorDesc.MINIO_REPLICATE_FAILED, set_res)
  # 创建 Minio 服务器数据
  minio_server_obj = await storage_crud.create_minio_server(
    region=region_obj,
    name=name,
    host=host,
    port=port,
    access_key=access_key,
    secret_key=secret_key
  )
  return minio_server_obj

async def get_minio_server_list() -> dict[str, List[MinioServer]]:
  """
  获取 Minio 服务器列表

  Returns:
    List[MinioServer]: Minio 服务器列表
  """
  minio_server_objs = await storage_crud.read_minio_server_list()
  return dict[str, List[MinioServer]](data=minio_server_objs)

async def get_buckets(minio_server: ObjectId) -> List[str]:
  """
  获取存储桶列表
  """
  minio_server_obj = await storage_crud.read_minio_server_by_id(minio_server)
  if not minio_server_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  minio_client = get_minio_client(
    host=minio_server_obj.host,
    port=minio_server_obj.port,
    access_key=minio_server_obj.access_key,
    secret_key=minio_server_obj.secret_key
  )
  buckets = await get_buckets_info(minio_client)
  return dict[str, list](data=buckets)