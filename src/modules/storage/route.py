from fastapi import APIRouter, Depends

from src.core.auth import get_current_user, check_permissions
from src.modules.auth.model import User
from src.modules.storage import service as storage_service
from src.modules.storage import schema as storage_schema
from src.modules.public import schema as public_schema

router = APIRouter()

@router.post(
  path="/minio-server",
  response_model=storage_schema.MinioServerResponse,
  summary="创建 Minio 服务器")
async def create_minio_server(
  payload: storage_schema.MinioServerCreateRequest,
  current_user: User = Depends(get_current_user),
) -> storage_schema.MinioServerResponse:
  """
  创建 Minio 服务器（需 region_manage）
  """
  await check_permissions(current_user, ["region_manage"])
  return await storage_service.create_minio_server(
    region=payload.region,
    name=payload.name.strip(),
    host=payload.host.strip(),
    server_port=payload.server_port,
    minio_port=payload.minio_port,
    access_key=payload.access_key.strip(),
    secret_key=payload.secret_key.strip(),
    replicate_weight=payload.replicate_weight
  )

@router.put(
  path="/minio-server/{minio_server_id}",
  response_model=storage_schema.MinioServerResponse,
  summary="更新 Minio 服务器")
async def update_minio_server(
  minio_server_id: public_schema.PydanticObjectId,
  payload: storage_schema.MinioServerUpdateRequest,
  current_user: User = Depends(get_current_user),
) -> storage_schema.MinioServerResponse:
  """
  更新 Minio 服务器（需 region_manage）
  """
  await check_permissions(current_user, ["region_manage"])
  return await storage_service.update_minio_server(
    minio_server_id=minio_server_id,
    payload=payload
  )

@router.get(
  path="/minio-server",
  response_model=storage_schema.MinioServerListResponse,
  summary="获取 Minio 服务器列表")
async def get_minio_server_list(
  current_user: User = Depends(get_current_user),
) -> storage_schema.MinioServerListResponse:
  """
  获取 Minio 服务器列表（需登录）
  """
  return await storage_service.get_minio_server_list()

@router.get(
  path="/{minio_server_id}/details",
  response_model=storage_schema.ServerDetailsResponse,
  summary="获取服务器文件详情")
async def get_server_details(
  minio_server_id: public_schema.PydanticObjectId,
  current_user: User = Depends(get_current_user),
):
  """
  获取服务器文件详情（需登录）
  """
  return await storage_service.get_server_details(minio_server_id)

@router.get(
  path="/buckets",
  response_model=storage_schema.BucketsResponse,
  summary="获取存储桶列表")
async def get_buckets(
  current_user: User = Depends(get_current_user),
) -> storage_schema.BucketsResponse:
  """
  获取存储桶列表（需登录）
  """
  return await storage_service.get_buckets()

@router.get(
  path="/buckets/{bucket_name}/replicates",
  summary="获取存储桶复制信息")
async def get_bucket_replicate_infos(
  bucket_name: str,
  current_user: User = Depends(get_current_user),
):
  """
  获取存储桶复制信息（需登录）
  """
  return await storage_service.get_bucket_replicate_infos(bucket_name)

@router.post(
  path="/buckets/{bucket_name}/replicates",
  response_model=storage_schema.BucketReplicateRuleResponse,
  summary="创建存储桶复制连接")
async def create_bucket_replicate(
  bucket_name: str,
  payload: storage_schema.BucketReplicateCreateRequest,
  current_user: User = Depends(get_current_user),
) -> storage_schema.BucketReplicateRuleResponse:
  """创建一条单向 Bucket 复制规则（需 region_manage）。"""
  await check_permissions(current_user, ["region_manage"])
  return await storage_service.create_bucket_replicate(bucket_name, payload)
