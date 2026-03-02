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

def connect_minio_server(host: str, port: int, access_key: str, secret_key: str):
  """
  连接 Minio 服务器
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
  # 列出所有的存储桶
  try:
    buckets = minio_client.list_buckets()
    for bucket in buckets:
      print(bucket.name)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))
  