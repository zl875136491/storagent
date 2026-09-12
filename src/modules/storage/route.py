import secrets
from typing import Literal
from urllib.parse import parse_qs

from fastapi import APIRouter, Depends, Query, Request, Response, status
from fastapi.responses import HTMLResponse

from src.core.auth import get_current_user, check_permissions
from src.core.rate_limit import rate_limit_one_time_download
from src.modules.auth.model import User
from src.modules.storage import service as storage_service
from src.modules.storage import operations as storage_operations
from src.modules.storage import download as storage_download
from src.modules.storage import schema as storage_schema
from src.modules.public import schema as public_schema


_ONE_TIME_DOWNLOAD_BODY_LIMIT = 256


async def _read_one_time_download_token(request: Request) -> str:
  content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
  if content_type != "application/x-www-form-urlencoded":
    raise storage_download.invalid_link_error()

  raw_length = request.headers.get("content-length")
  if raw_length:
    try:
      if int(raw_length) > _ONE_TIME_DOWNLOAD_BODY_LIMIT:
        raise storage_download.invalid_link_error()
    except ValueError:
      raise storage_download.invalid_link_error()

  body = bytearray()
  async for chunk in request.stream():
    if len(body) + len(chunk) > _ONE_TIME_DOWNLOAD_BODY_LIMIT:
      raise storage_download.invalid_link_error()
    body.extend(chunk)

  try:
    fields = parse_qs(
      body.decode("ascii"),
      keep_blank_values=True,
      strict_parsing=True,
      max_num_fields=1,
    )
  except (UnicodeDecodeError, ValueError):
    raise storage_download.invalid_link_error()
  values = fields.get("token")
  if set(fields) != {"token"} or not values or len(values) != 1:
    raise storage_download.invalid_link_error()
  return values[0]

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
    domain=payload.domain.strip().lower(),
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
  path="/{minio_server_id}/inventory/children",
  response_model=storage_schema.InventoryChildrenResponse,
  summary="分页列出服务器文件目录子项",
)
async def list_server_file_children(
  minio_server_id: public_schema.PydanticObjectId,
  bucket: str | None = Query(None, max_length=63, description="存储桶；缺省时列出最外层存储桶"),
  prefix: str | None = Query(None, max_length=1024, description="当前目录前缀，缺省为桶根"),
  offset: int = Query(0, ge=0, description="已加载的子项数量"),
  limit: int = Query(40, ge=1, le=100, description="本次返回的子项数量"),
  sort: Literal["size", "name", "last_modified", "object_key"] = Query("size"),
  order: Literal["asc", "desc"] = Query("desc"),
  current_user: User = Depends(get_current_user),
):
  return await storage_service.list_server_file_children(
    minio_server_id,
    bucket=bucket,
    prefix=prefix,
    offset=offset,
    limit=limit,
    sort=sort,
    order=order,
  )


@router.get(
  path="/{minio_server_id}/inventory/search",
  response_model=storage_schema.InventorySearchResponse,
  summary="在服务器文件索引中分页搜索",
)
async def search_server_files(
  minio_server_id: public_schema.PydanticObjectId,
  q: str | None = Query(None, max_length=128, description="按文件名、对象路径或存储桶全量搜索"),
  bucket: str | None = Query(None, max_length=63),
  page: int = Query(1, ge=1),
  page_size: int = Query(50, ge=1, le=100),
  sort: Literal["size", "name", "last_modified", "object_key"] = Query("object_key"),
  order: Literal["asc", "desc"] = Query("asc"),
  current_user: User = Depends(get_current_user),
):
  return await storage_service.search_server_files(
    minio_server_id,
    query=q,
    bucket=bucket,
    page=page,
    page_size=page_size,
    sort=sort,
    order=order,
  )


@router.post(
  path="/{minio_server_id}/objects/presigned-download",
  response_model=storage_schema.OneTimeDownloadCreateResponse,
  summary="生成管理员一次性对象下载链接",
)
async def create_one_time_object_download(
  minio_server_id: public_schema.PydanticObjectId,
  payload: storage_schema.OneTimeDownloadCreateRequest,
  request: Request,
  response: Response,
  current_user: User = Depends(get_current_user),
) -> storage_schema.OneTimeDownloadCreateResponse:
  await check_permissions(current_user, ["storage_operations_manage"])
  issued = await storage_download.issue_one_time_download(
    minio_server_id,
    payload.bucket,
    payload.object_key,
    current_user.username,
  )
  token = issued.pop("token")
  # Return an origin-independent path. The authenticated console resolves it
  # against the backend it selected, so Host/proxy headers cannot redirect it.
  bootstrap_url = str(request.app.url_path_for("download_one_time_object_bootstrap"))
  # URL fragment is never sent in the HTTP request target or access log.
  download_url = f"{bootstrap_url}#token={token}"
  response.headers["Cache-Control"] = "no-store"
  return storage_schema.OneTimeDownloadCreateResponse(
    download_url=download_url,
    url=download_url,
    single_use=True,
    **issued,
  )


@router.get(
  path="/objects/one-time-download",
  name="download_one_time_object_bootstrap",
  response_model=None,
  summary="打开一次性对象下载引导页",
)
async def download_one_time_object_bootstrap():
  """Read the capability from the fragment, erase it, then submit it in a POST body."""
  nonce = secrets.token_urlsafe(18)
  html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="referrer" content="no-referrer">
  <title>正在准备下载</title>
</head>
<body>
  <p>正在准备下载...</p>
  <noscript>此下载链接需要启用 JavaScript。</noscript>
  <script nonce="{nonce}">
  (() => {{
    const rawHash = window.location.hash;
    window.history.replaceState(null, "", window.location.pathname);
    const token = new URLSearchParams(rawHash.startsWith("#") ? rawHash.slice(1) : rawHash).get("token");
    if (!token) {{
      document.body.textContent = "下载链接无效或已过期";
      return;
    }}
    const form = document.createElement("form");
    form.method = "post";
    form.action = window.location.pathname;
    form.enctype = "application/x-www-form-urlencoded";
    const input = document.createElement("input");
    input.type = "hidden";
    input.name = "token";
    input.value = token;
    form.appendChild(input);
    document.body.appendChild(form);
    form.submit();
  }})();
  </script>
</body>
</html>"""
  return HTMLResponse(
    content=html,
    headers={
      "Cache-Control": "private, no-store, max-age=0",
      "Pragma": "no-cache",
      "Content-Security-Policy": (
        "default-src 'none'; "
        f"script-src 'nonce-{nonce}'; "
        "form-action 'self'; base-uri 'none'; frame-ancestors 'none'"
      ),
      "Referrer-Policy": "no-referrer",
      "X-Content-Type-Options": "nosniff",
    },
  )


@router.post(
  path="/objects/one-time-download",
  name="download_one_time_object",
  response_model=None,
  summary="兑换一次性对象下载链接",
)
async def download_one_time_object(request: Request):
  # Capability 仅出现在受限大小的 POST body；成功兑换后立即从 Etcd 原子删除。
  rate_limit_one_time_download(request)
  token = await _read_one_time_download_token(request)
  return await storage_download.redeem_one_time_download(token)

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
  await check_permissions(current_user, ["storage_operations_manage"])
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
  await check_permissions(current_user, ["storage_operations_manage"])
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
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.get_replication_overview(bucket)


@router.get(
  path="/operations/unmanaged-buckets",
  response_model=storage_schema.UnmanagedBucketOperationsResponse,
  summary="盘点未纳管存储桶",
)
async def get_unmanaged_bucket_operations(
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.get_unmanaged_bucket_overview()


@router.get(
  path="/operations/orphan-buckets",
  response_model=storage_schema.OrphanBucketOperationsResponse,
  include_in_schema=False,
)
async def get_orphan_bucket_operations_compat(
  current_user: User = Depends(get_current_user),
):
  """Compatibility endpoint for older console bundles."""
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.get_orphan_bucket_overview()


@router.post(
  path="/operations/unmanaged-buckets/{bucket_name}/retain",
  response_model=storage_schema.UnmanagedBucketActionResponse,
  summary="登记未纳管存储桶保留",
)
async def retain_unmanaged_bucket(
  bucket_name: str,
  payload: storage_schema.UnmanagedBucketRetainRequest,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.retain_unmanaged_bucket(
    bucket_name,
    payload.reason,
    current_user.username,
  )


@router.delete(
  path="/operations/unmanaged-buckets/{bucket_name}/retain",
  response_model=storage_schema.UnmanagedBucketActionResponse,
  summary="取消未纳管存储桶保留",
)
async def release_unmanaged_bucket_retention(
  bucket_name: str,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.release_unmanaged_bucket_retention(
    bucket_name,
    current_user.username,
  )


@router.post(
  path="/operations/unmanaged-buckets/{bucket_name}/delete",
  response_model=storage_schema.UnmanagedBucketActionResponse,
  status_code=status.HTTP_202_ACCEPTED,
  summary="清理未纳管空存储桶",
)
async def delete_unmanaged_bucket(
  bucket_name: str,
  payload: storage_schema.UnmanagedBucketDeleteRequest,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  operation = await storage_operations.delete_unmanaged_bucket(
    bucket_name,
    payload.confirmation,
    current_user.username,
  )
  return {
    "message": operation.get("message", "清理任务已进入队列"),
    "bucket": bucket_name.strip(),
    "disposition": "deleting" if operation.get("status") in {"queued", "running"} else "delete_failed",
    "accepted": operation.get("status") in {"queued", "running"},
    "operation_id": operation.get("id"),
    "operation_status": operation.get("status"),
    "operation": operation,
  }


@router.post(
  path="/operations/replication/{bucket_name}/reconcile",
  response_model=storage_schema.ReplicationOperationResponse,
  status_code=status.HTTP_200_OK,
  summary="校准存储桶复制规则",
)
async def reconcile_bucket_replication(
  bucket_name: str,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.reconcile_bucket_replication(
    bucket_name,
    current_user.username,
  )


@router.post(
  path="/operations/replication/{bucket_name}/resync",
  response_model=storage_schema.ReplicationOperationResponse,
  status_code=status.HTTP_200_OK,
  summary="启动复制链路对象补传",
)
async def start_bucket_replication_resync(
  bucket_name: str,
  payload: storage_schema.ReplicationResyncRequest,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
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
  await check_permissions(current_user, ["storage_operations_manage"])
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
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.get_cluster_heal_status(server_name)


@router.post(
  path="/operations/clusters/{server_name}/heal",
  response_model=storage_schema.StorageOperationItem,
  summary="启动集群原生自愈巡检",
)
async def start_cluster_heal(
  server_name: str,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
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
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.list_storage_operations(limit)


@router.get(
  path="/operations/jobs/{operation_id}",
  response_model=storage_schema.StorageOperationItem,
  summary="获取单个存储运维任务",
)
async def get_storage_operation(
  operation_id: str,
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await storage_operations.get_storage_operation(operation_id)
