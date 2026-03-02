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

def create_bucket(minio_client: Minio, bucket_name: str):
  """
  创建存储桶
  
  Args:
    minio_client: Minio 客户端
    bucket_name: 存储桶名称

  Returns:
    None: 无返回值
  """
  try:
    minio_client.make_bucket(bucket_name)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))
  