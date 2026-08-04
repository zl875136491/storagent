from fastapi import APIRouter, Depends, Query

from src.core.auth import get_current_user, check_permissions, require_admin
from src.modules.auth.model import User
from src.modules.storage import service as storage_service
from src.modules.storage import operations as storage_operations
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
  refresh: bool = Query(False, description="忽略缓存并重新读取 MinIO"),
  current_user: User = Depends(get_current_user),
):
  """
  获取服务器文件详情（需登录）
  """
  return await storage_service.get_server_details(
    minio_server_id,
    force_refresh=refresh,
  )

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
  response_model=storage_schema.BucketReplicateResponse,
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
  """创建一条单向 Bucket 复制规则（需管理员）。"""
  await require_admin(current_user)
  return await storage_service.create_bucket_replicate(bucket_name, payload)

@router.delete(
  path="/buckets/{bucket_name}/replicates",
  summary="删除存储桶复制连接")
async def delete_bucket_replicate(
  bucket_name: str,
  current_user: User = Depends(get_current_user),
  from_server: str = Query(..., alias="from", min_length=1, max_length=128),
  to_server: str = Query(..., alias="to", min_length=1, max_length=128),
  rule_id: str | None = Query(None, max_length=128),
):
  """删除一条单向 Bucket 复制规则（需管理员）；会执行 mc replicate remove。"""
  await require_admin(current_user)
  return await storage_service.delete_bucket_replicate(
    bucket_name,
    from_server=from_server,
    to_server=to_server,
    rule_id=rule_id,
  )


@router.get(
  path="/operations/replication",
  response_model=storage_schema.ReplicationOperationsResponse,
  summary="获取复制运维总览",
)
async def get_replication_operations(
  bucket: str | None = Query(None, min_length=3, max_length=63),
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.get_replication_overview(bucket)


@router.post(
  path="/operations/replication/{bucket_name}/reconcile",
  response_model=storage_schema.ReplicationOperationResponse,
  summary="校准存储桶复制规则",
)
async def reconcile_bucket_replication(
  bucket_name: str,
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.reconcile_bucket_replication(
    bucket_name,
    current_user.username,
  )


@router.post(
  path="/operations/replication/{bucket_name}/resync",
  response_model=storage_schema.ReplicationOperationResponse,
  summary="启动复制链路对象补传",
)
async def start_bucket_replication_resync(
  bucket_name: str,
  payload: storage_schema.ReplicationResyncRequest,
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.start_replication_resync(
    bucket_name,
    payload.source_server,
    payload.target_server,
    payload.older_than,
    current_user.username,
  )


@router.get(
  path="/operations/clusters",
  response_model=storage_schema.ClusterHealthResponse,
  summary="获取 MinIO 集群健康总览",
)
async def get_cluster_health_operations(
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.get_cluster_health_overview()


@router.get(
  path="/operations/clusters/{server_name}/heal",
  response_model=storage_schema.ClusterHealStatusResponse,
  summary="获取集群自愈状态",
)
async def get_cluster_heal_status(
  server_name: str,
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.get_cluster_heal_status(server_name)


@router.post(
  path="/operations/clusters/{server_name}/heal",
  response_model=storage_schema.StorageOperationItem,
  summary="启动集群深度修复扫描",
)
async def start_cluster_heal(
  server_name: str,
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.start_cluster_heal(
    server_name,
    current_user.username,
  )


@router.get(
  path="/operations/jobs",
  response_model=storage_schema.StorageOperationListResponse,
  summary="获取存储运维任务",
)
async def get_storage_operations(
  limit: int = Query(20, ge=1, le=100),
  current_user: User = Depends(get_current_user),
):
  await require_admin(current_user)
  return await storage_operations.list_storage_operations(limit)
