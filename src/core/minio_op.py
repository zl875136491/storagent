import json
import subprocess
from minio import Minio
from minio.commonconfig import ENABLED
from minio.versioningconfig import VersioningConfig
from minio.replicationconfig import (
  ReplicationConfig,
  Rule,
  Destination,
  DeleteMarkerReplication
)
from src.core.exception import CustomException, ErrorDesc

def get_minio_client(host: str, port: int, access_key: str, secret_key: str) -> Minio:
  """
  获取 Minio 客户端
  
  Args:
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    Minio: Minio 客户端
  """
  endpoiont = f"{host}:{port}"
  try:
    minio_client = Minio(
      endpoint=endpoiont,
      access_key=access_key,
      secret_key=secret_key,
      secure=False,
    )
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_CONN_FAILED, str(e))
  return minio_client

def test_minio_server(host: str, port: int, access_key: str, secret_key: str):
  """
  连接 Minio 服务器
  
  Args:
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    None: 无返回值
  """
  minio_client = get_minio_client(host, port, access_key, secret_key)
  # 列出所有的存储桶
  bucket_list = []  
  try:
    buckets = minio_client.list_buckets()
    for bucket in buckets:
      bucket_list.append(bucket.name)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

async def create_bucket(server_name: str, bucket_name: str):
  """
  创建存储桶
  
  Args:
    minio_client: Minio 客户端
    bucket_name: 存储桶名称

  Returns:
    None: 无返回值
  """
  # try:
  #   minio_client.make_bucket(bucket_name)
  # except Exception as e:
  #   raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))
  success, err = await _run_cmd(f"mc mb {server_name}/{bucket_name}")
  if not success:
    raise CustomException(ErrorDesc.MINIO_CREATE_BUCKET_FAILED, str(err))

async def _run_cmd(cmd):
  """执行 shell 命令并返回结果"""
  try:
    result = subprocess.run(
      cmd, shell=True, check=True, 
      capture_output=True, text=True
    )
    return True, result.stdout
  except subprocess.CalledProcessError as e:
      return False, e.stderr

async def set_site_alias(site_name, endpoint, admin_user, admin_password):
  """
  设置站点别名
  """
  alias_cmd = f"mc alias set {site_name} http://{endpoint} {admin_user} {admin_password}"
  success, _ = await _run_cmd(alias_cmd)
  if not success: return False, f"设置别名失败:{str(_)}"
  return True, "设置别名成功"

async def remove_site_alias(site_name):
  """
  删除站点别名
  """
  alias_cmd = f"mc alias rm {site_name}"
  success, _ = await _run_cmd(alias_cmd)
  if not success: return False, f"删除别名失败:{str(_)}"
  return True, "删除别名成功"

async def add_new_site(master_name, site_name):
  """
  新增 MinIO 节点加入当前的复制集
  """
  # 将新站点加入复制集
  # 注意：Site Replication 要求所有站点在加入前必须是“空”的（或具有相同的初始状态）
  print(f"[*] 正在将 {site_name} 加入到 {master_name} 的复制集...")
  replicate_cmd = f"mc admin replicate add {master_name} {site_name}"
  success, output = await _run_cmd(replicate_cmd)
  return success, output

async def get_site_replication_status(master):
  """获取当前复制集状态"""
  # 使用 --json 参数便于 Python 解析
  cmd = f"mc admin replicate info {master} --json"
  success, output = await _run_cmd(cmd)
  if success:
      return json.loads(output)
  return None