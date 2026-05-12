from bson import ObjectId
from typing import Any, List

from src.modules.public.model import Region
from src.core.exception import CustomException, ErrorDesc
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.modules.storage.model import MinioServer
from src.modules.graph import crud as graph_crud
from src.core.minio_op import (
  test_minio_server,
  set_site_alias,
  add_new_site,
  remove_site_alias,
  get_buckets_info,
  get_minio_client,
  get_server_buckets,
  get_bucket_replicate_status,
  get_bucket_replicate_info,
  get_site_alias
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
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str) -> MinioServer:
  """
  创建 Minio 服务器

  Args:
    region: 区域ID
    name: 服务器名称
    host: 服务器主机
    server_port: 服务器端口
    minio_port: Minio 端口
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
  existed_minio_server = await storage_crud.read_minio_server_by_fqdn(host, minio_port)
  if existed_minio_server:
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "MinioServer.host:port")
  # 验证连接性
  await _connect_minio_server(host, minio_port, access_key, secret_key)
  # 创建别名
  success, res = await set_site_alias(
    site_name=region_obj.name,
    endpoint=f"{host}:{minio_port}",
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
    server_port=server_port,
    minio_port=minio_port,
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

async def get_buckets() -> List[str]:
  """
  获取存储桶列表
  """
  bucket_data = {}
  server_names = await storage_crud.read_minio_server_names()
  app_infos = {}
  app_objs = await public_crud.read_application_list()
  for app_obj in app_objs:
    app_infos[app_obj.name] = {
      "shown_name": app_obj.shown_name,
      "description": app_obj.description
    }
  for server_name in server_names:
    buckets = await get_server_buckets(server_name)
    for bucket_name in buckets:
      if bucket_name not in bucket_data:
        if bucket_name in app_infos:
          app_info = app_infos[bucket_name]
        else:
          app_info = {}
        bucket_data[bucket_name] = {
          "name": bucket_name,
          "servers": [server_name],
          "app": app_info
        }
      else:
        bucket_data[bucket_name]["servers"].append(server_name)
  return dict[str, List](data=list(bucket_data.values()))

async def get_server_details(minio_server: ObjectId) -> List[str]:
  """
  获取存储桶列表
  """
  minio_server_obj = await storage_crud.read_minio_server_by_id(minio_server)
  if not minio_server_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  minio_client = get_minio_client(
    host=minio_server_obj.host,
    port=minio_server_obj.minio_port,
    access_key=minio_server_obj.access_key,
    secret_key=minio_server_obj.secret_key
  )
  buckets = await get_buckets_info(minio_client)
  return dict[str, list](data=buckets)

async def format_replicate_status(status: dict) -> str:
  """
  格式化复制状态
  """
  data = {}
  if "status" in status:
    data["status"] = status["status"]
  if "rule" in status:
    if "Priority" in status["rule"]:
      data["priority"] = status["rule"]["Priority"]
    if "DeleteMarkerReplication" in status["rule"]:
      data["delete_marker_replication"] = status["rule"]["DeleteMarkerReplication"]["Status"]
    if "ExistingObjectReplication" in status["rule"]:
      data["existing_object_replication"] = status["rule"]["ExistingObjectReplication"]["Status"]
    if "SourceSelectionCriteria" in status["rule"]:
      if "ReplicaModifications" in status["rule"]["SourceSelectionCriteria"]:
        data["source_selection_criteria"] = status["rule"]["SourceSelectionCriteria"]["ReplicaModifications"]["Status"]
  return data

async def get_bucket_replicate_infos(bucket_name) -> List[dict]:
  """
  获取存储桶复制信息
  """
  replicates = []
  alias_datas = await get_site_alias()
  # 获取服务器别名与服务器地址的映射关系
  # 因为 mc replicate ls 命令返回的是服务器地址, 而不是服务器别名
  mappings = {}
  for alias_name, alias_data in alias_datas.items():
    url = alias_data["URL"].split("://")[1]
    mappings[url] = alias_name
  server_names = await storage_crud.read_minio_server_names()
  # 获取拓扑图的边和节点的位置信息
  nodes = {}
  edges = {}
  node_objs = await graph_crud.read_many_bucket_node_positions(bucket_name)
  edge_objs = await graph_crud.read_many_bucket_edge_positions(bucket_name)
  for node_item in node_objs:
    nodes[node_item.server] = {
      "position_x": node_item.position_x,
      "position_y": node_item.position_y
    }
  for server_name in server_names:
    if server_name not in nodes.keys():
      nodes[server_name] = {
        "position_x": 0,
        "position_y": 0
      }
  for edge_item in edge_objs:
    from_server = edge_item.from_server
    to_server = edge_item.to_server
    from_position = edge_item.from_position
    to_position = edge_item.to_position
    temp_id = f"{from_server}-{to_server}"
    edges[temp_id] = {
      "from_position": from_position,
      "to_position": to_position
    } 
  for server_name in server_names:
    to_server_endpoints = await get_bucket_replicate_info(server_name, bucket_name)
    status_infos = await get_bucket_replicate_status(server_name, bucket_name)
    if not to_server_endpoints:
      continue
    for endpoint in to_server_endpoints.keys():
      to_server_name = mappings[endpoint]
      rule_id = to_server_endpoints[endpoint]
      status_info = await format_replicate_status(status_infos[rule_id])
      temp_id = f"{server_name}-{to_server_name}"
      if temp_id in edges:
        from_position = edges[temp_id]["from_position"]
        to_position = edges[temp_id]["to_position"]
      else:
        from_position = "up"
        to_position = "down"
      replicates.append({
        "from": server_name,
        "from_position": from_position,
        "to": to_server_name,
        "to_position": to_position,
        "status": status_info,
        "rule_id": rule_id
      })
  return dict(servers=nodes, replicates=replicates)