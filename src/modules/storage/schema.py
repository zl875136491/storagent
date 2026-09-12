from pydantic import BaseModel, Field
from typing import Any, List, Literal, Optional
from src.modules.public.schema import PydanticObjectId
from datetime import datetime
class MinioServerCreateRequest(BaseModel):
  region: PydanticObjectId
  name: str
  domain: str = Field(min_length=1, max_length=253, description="对外 Nginx 网关域名，不含协议和路径")
  host: str
  server_port: int
  minio_port: int
  access_key: str
  secret_key: str
  replicate_weight: int = 0

class MinioServerUpdateRequest(BaseModel):
  replicate_weight: int

class SimpleRegionResponse(BaseModel):
  id: PydanticObjectId
  name: str
  shown_name: str

class MinioServerResponse(BaseModel):
  id: PydanticObjectId
  region: SimpleRegionResponse
  name: str
  domain: str
  host: str
  server_port: int
  minio_port: int
  master: bool
  replicate_weight: int

class MinioServerListResponse(BaseModel):
  data: List[MinioServerResponse]

class BucketFileItem(BaseModel):
  name: str = Field(..., description="文件名称")
  size: int = Field(..., description="文件大小")
  last_modified: datetime = Field(..., description="最后修改时间")
  children: List["BucketFileItem"] | None = Field(None, description="子文件列表")

class BucketInfo(BaseModel):
  name: str = Field(..., description="存储桶名称")
  total_size: int = Field(..., description="总大小")
  created_at: datetime = Field(..., description="创建时间")
  object_count: int = Field(default=0, ge=0, description="对象数量")
  files: List[BucketFileItem] = Field(default_factory=list, description="兼容字段：完整树已不再返回")

class ServerDetailsResponse(BaseModel):
  data: List[BucketInfo] = Field(..., description="存储桶占用摘要")
  cache_hit: bool = Field(False, description="是否命中 Mongo 对象索引")
  cached_at: datetime = Field(..., description="缓存生成时间")
  expires_at: datetime = Field(..., description="缓存过期时间")
  ttl_seconds: int = Field(21600, ge=1, description="索引建议刷新间隔（秒），由 Celery 定时任务执行")
  object_count: int = Field(default=0, ge=0, description="对象总数")
  total_size: int = Field(default=0, ge=0, description="对象总大小")
  index_ready: bool = Field(True, description="本区是否已有可用的对象索引")


class InventoryNode(BaseModel):
  name: str
  kind: Literal["dir", "file"]
  bucket: str = ""
  object_key: str = ""
  parent: str = ""
  size: int = Field(default=0, ge=0)
  object_count: int = Field(default=0, ge=0)
  child_count: int = Field(default=0, ge=0)
  last_modified: datetime


class InventoryChildrenResponse(BaseModel):
  bucket: str = ""
  prefix: str = ""
  items: List[InventoryNode] = Field(default_factory=list)
  offset: int = Field(default=0, ge=0)
  limit: int = Field(default=40, ge=1)
  total: int = Field(default=0, ge=0)
  has_more: bool = False
  remaining_count: int = Field(default=0, ge=0)
  page_size_sum: int = Field(default=0, ge=0)
  parent_size: int = Field(default=0, ge=0)
  parent_object_count: int = Field(default=0, ge=0)
  parent_child_count: int = Field(default=0, ge=0)
  cache_hit: bool = False
  cached_at: datetime
  expires_at: datetime
  ttl_seconds: int = Field(21600, ge=1)
  index_ready: bool = True


class InventorySearchResponse(BaseModel):
  q: str = ""
  bucket: str = ""
  items: List[InventoryNode] = Field(default_factory=list)
  page: int = Field(default=1, ge=1)
  page_size: int = Field(default=50, ge=1)
  total: int = Field(default=0, ge=0)
  page_count: int = Field(default=1, ge=1)
  cache_hit: bool = False
  cached_at: datetime
  expires_at: datetime
  ttl_seconds: int = Field(21600, ge=1)
  index_ready: bool = True


class OneTimeDownloadCreateRequest(BaseModel):
  bucket: str = Field(..., min_length=3, max_length=63, description="存储桶名称")
  object_key: str = Field(..., min_length=1, max_length=1024, description="完整对象键")


class OneTimeDownloadCreateResponse(BaseModel):
  download_url: str = Field(..., description="无需 API Key 的一次性下载地址")
  url: str = Field(..., description="download_url 的兼容字段")
  expires_at: datetime = Field(..., description="链接过期时间")
  expires_in_seconds: int = Field(..., ge=1, le=900, description="链接有效秒数")
  single_use: bool = Field(True, description="是否仅允许成功兑换一次")
  filename: str = Field(..., description="下载时使用的文件名")

class SimpleAppInfo(BaseModel):
  shown_name: Optional[str] = Field(None, description="应用显示名称")
  description: Optional[str] = Field(None, description="应用描述")

class BucketListItem(BaseModel):
  name: str = Field(..., description="存储桶名称")
  servers: List[str] = Field(..., description="服务器列表")
  app: SimpleAppInfo = Field(..., description="应用信息")

class BucketsResponse(BaseModel):
  data: List[BucketListItem] = Field(..., description="存储桶列表")
  
class BucketReplicateResponse(BaseModel):
  servers: List[str] | dict = Field(..., description="服务器列表或拓扑坐标")
  server_ids: List[str] = Field(default_factory=list, description="全部服务器 ID")
  replicates: List[dict] = Field(..., description="复制信息")
  policy: dict = Field(default_factory=dict, description="全连接复制策略摘要")

class BucketReplicateRuleStatus(BaseModel):
  status: str = Field(default="pending", max_length=32, description="复制规则状态")
  rule_status: str = Field(default="Enabled", max_length=32, description="MinIO 规则启停状态")
  priority: int = Field(default=0, ge=0, le=2_147_483_647, description="规则优先级")
  delete_marker_replication: Literal["Enabled", "Disabled"] = "Enabled"
  existing_object_replication: Literal["Enabled", "Disabled"] = "Enabled"
  source_selection_criteria: Literal["Enabled", "Disabled"] = "Enabled"

class BucketReplicateCreateRequest(BaseModel):
  from_server: str = Field(alias="from", min_length=1, max_length=128, description="源站点别名")
  to_server: str = Field(alias="to", min_length=1, max_length=128, description="目标站点别名")
  from_side: Literal["top", "right", "bottom", "left"] = "bottom"
  to_side: Literal["top", "right", "bottom", "left"] = "top"
  status: BucketReplicateRuleStatus | None = None

class BucketReplicateRuleResponse(BaseModel):
  from_server: str = Field(alias="from")
  to_server: str = Field(alias="to")
  from_position: Literal["up", "down", "left", "right"]
  to_position: Literal["up", "down", "left", "right"]
  status: dict
  rule_id: str

class BucketReplicateDeleteRequest(BaseModel):
  from_server: str = Field(alias="from", min_length=1, max_length=128, description="源站点别名")
  to_server: str = Field(alias="to", min_length=1, max_length=128, description="目标站点别名")
  rule_id: str | None = Field(None, max_length=128, description="复制规则 ID；缺省时按 from/to 查找")


OperationStatus = Literal["healthy", "syncing", "degraded", "critical", "unreachable"]
StatusReasonSeverity = Literal[
  "info", "syncing", "degraded", "critical", "unreachable"
]
ReplicationResyncStatus = Literal[
  "idle", "running", "completed", "partial", "failed", "unknown"
]


class ReplicationStatusReason(BaseModel):
  code: str
  message: str
  value: Any = None
  severity: StatusReasonSeverity = "info"


class ReplicationTargetMetric(BaseModel):
  source: str
  target: str
  arn: str
  endpoint: str
  status: OperationStatus
  online: bool
  latency_current_ms: float = 0
  latency_average_ms: float = 0
  latency_maximum_ms: float = 0
  total_downtime_seconds: float = 0
  last_online: datetime | None = None
  replication_count: int = 0
  completed_bytes: int = 0
  failed_count: int = 0
  failed_bytes: int = 0
  recent_failed_count: int = 0
  recent_failed_bytes: int = 0
  current_rate_bps: float = 0
  resync_status: ReplicationResyncStatus = "idle"
  resync_reset_id: str = ""
  resync_started_at: datetime | None = None
  resync_updated_at: datetime | None = None
  resync_completed_bytes: int = 0
  resync_object_count: int = 0
  resync_failed_count: int = 0
  resync_failed_bytes: int = 0
  resync_current_object: str = ""
  resync_error: str = ""
  status_reasons: list[ReplicationStatusReason] = Field(default_factory=list)


class ReplicationSourceMetric(BaseModel):
  server: str
  status: OperationStatus
  reachable: bool
  command_latency_ms: float = 0
  error: str = ""
  queued_count: int = 0
  queued_bytes: int = 0
  failed_count: int = 0
  failed_bytes: int = 0
  recent_failed_count: int = 0
  recent_failed_bytes: int = 0
  mrf_failed_last_5m: int = 0
  retries_total: int = 0
  current_rate_bps: float = 0
  expected_target_count: int = 0
  actual_target_count: int = 0
  targets: list[ReplicationTargetMetric] = Field(default_factory=list)
  status_reasons: list[ReplicationStatusReason] = Field(default_factory=list)


class ReplicationBucketMetric(BaseModel):
  bucket: str
  shown_name: str = ""
  status: OperationStatus
  sources: list[ReplicationSourceMetric]
  status_reasons: list[ReplicationStatusReason] = Field(default_factory=list)


class ReplicationOperationsSummary(BaseModel):
  status: OperationStatus
  bucket_count: int = 0
  source_count: int = 0
  reachable_source_count: int = 0
  expected_link_count: int = 0
  actual_link_count: int = 0
  online_link_count: int = 0
  queued_count: int = 0
  queued_bytes: int = 0
  failed_count: int = 0
  failed_bytes: int = 0
  recent_failed_count: int = 0
  recent_failed_bytes: int = 0
  mrf_failed_last_5m: int = 0
  current_rate_bps: float = 0
  status_reasons: list[ReplicationStatusReason] = Field(default_factory=list)


class ReplicationOperationsResponse(BaseModel):
  generated_at: datetime
  servers: list[str]
  summary: ReplicationOperationsSummary
  buckets: list[ReplicationBucketMetric]


class ReplicationResyncRequest(BaseModel):
  source_server: str = Field(..., min_length=1, max_length=128)
  target_server: str = Field(..., min_length=1, max_length=128)
  older_than: str | None = Field(None, max_length=64, description="可选 mc 时长，如 7d12h")


UnmanagedBucketKind = Literal[
  "unmanaged",
  "disabled_application",
  "system",
  "needs_review",
]


# The legacy /operations/orphan-buckets route is intentionally retained for
# older console bundles. New clients must use the unmanaged-buckets contracts
# below; these models freeze the previous response vocabulary.
OrphanBucketKind = Literal["orphan", "disabled_application", "system"]


class OrphanBucketItem(BaseModel):
  name: str
  kind: OrphanBucketKind
  app_name: str = ""
  app_shown_name: str = ""
  servers: list[str] = Field(default_factory=list)
  missing_servers: list[str] = Field(default_factory=list)


class OrphanBucketOperationsSummary(BaseModel):
  orphan_count: int = 0
  disabled_application_count: int = 0
  system_bucket_count: int = 0
  unavailable_server_count: int = 0


class OrphanBucketOperationsResponse(BaseModel):
  generated_at: datetime
  servers: list[str] = Field(default_factory=list)
  summary: OrphanBucketOperationsSummary
  buckets: list[OrphanBucketItem] = Field(default_factory=list)
  errors: dict[str, str] = Field(default_factory=dict)
UnmanagedBucketDisposition = Literal[
  "unreviewed",
  "retained",
  "deleting",
  "delete_failed",
  "needs_review",
  "not_applicable",
]


class UnmanagedBucketItem(BaseModel):
  name: str
  kind: UnmanagedBucketKind
  app_name: str = ""
  app_shown_name: str = ""
  servers: list[str] = Field(default_factory=list)
  missing_servers: list[str] = Field(default_factory=list)
  unreachable_servers: list[str] = Field(default_factory=list)
  coverage_status: Literal["complete", "partial", "unreachable"] = "complete"
  disposition: UnmanagedBucketDisposition = "not_applicable"
  disposition_reason: str = ""
  disposition_updated_at: datetime | None = None
  ownership_source: Literal["authoritative", "system", "unavailable"] = "authoritative"
  ownership_reason: str = ""
  cleanup_remaining_servers: list[str] = Field(default_factory=list)


class UnmanagedBucketOperationsSummary(BaseModel):
  unmanaged_count: int = 0
  disabled_application_count: int = 0
  system_bucket_count: int = 0
  needs_review_count: int = 0
  unavailable_server_count: int = 0


class UnmanagedBucketOperationsResponse(BaseModel):
  generated_at: datetime
  servers: list[str] = Field(default_factory=list)
  summary: UnmanagedBucketOperationsSummary
  buckets: list[UnmanagedBucketItem] = Field(default_factory=list)
  errors: dict[str, str] = Field(default_factory=dict)


class UnmanagedBucketRetainRequest(BaseModel):
  reason: str = Field(default="", max_length=500)


class UnmanagedBucketDeleteRequest(BaseModel):
  confirmation: str = Field(..., min_length=3, max_length=63)


class UnmanagedBucketActionResponse(BaseModel):
  message: str
  bucket: str
  disposition: UnmanagedBucketDisposition
  accepted: bool = False
  operation_id: str | None = None
  operation_status: Literal["queued", "running", "succeeded", "failed"] | None = None
  operation: dict[str, Any] | None = None

class ReplicationOperationResponse(BaseModel):
  message: str
  bucket: str
  accepted: bool = True
  operation_id: str | None = None
  operation_status: Literal["queued", "running", "succeeded", "failed"] | None = None
  source_server: str | None = None
  target_server: str | None = None
  detail: dict[str, Any] = Field(default_factory=dict)


class ClusterDriveHealth(BaseModel):
  endpoint: str
  path: str = ""
  state: str
  health: Literal["healthy", "warning", "critical", "offline", "unknown"] = "unknown"
  health_reasons: list[str] = Field(default_factory=list)
  total_bytes: int = 0
  used_bytes: int = 0
  available_bytes: int = 0
  usage_percent: float = 0
  used_inodes: int = 0
  free_inodes: int = 0
  inode_usage_percent: float = 0
  capacity_skew: bool = False
  waiting_operations: int = 0


class ClusterHealthItem(BaseModel):
  id: str
  server: str
  region: str
  shown_name: str
  endpoint: str
  status: Literal["online", "degraded", "critical", "offline"]
  reachable: bool
  error: str = ""
  checked_at: datetime
  command_latency_ms: float = 0
  version: str = ""
  uptime_seconds: int = 0
  bucket_count: int = 0
  object_count: int = 0
  version_count: int = 0
  delete_marker_count: int = 0
  logical_usage_bytes: int = 0
  raw_capacity_bytes: int = 0
  raw_used_bytes: int = 0
  online_disks: int = 0
  offline_disks: int = 0
  healing_disks: int = 0
  warning_disks: int = 0
  critical_disks: int = 0
  health_reasons: list[str] = Field(default_factory=list)
  drives: list[ClusterDriveHealth] = Field(default_factory=list)


class ClusterHealthSummary(BaseModel):
  status: Literal["online", "degraded", "critical", "offline"]
  cluster_count: int = 0
  online_clusters: int = 0
  degraded_clusters: int = 0
  critical_clusters: int = 0
  offline_clusters: int = 0
  online_disks: int = 0
  offline_disks: int = 0
  healing_disks: int = 0
  warning_disks: int = 0
  critical_disks: int = 0
  raw_capacity_bytes: int = 0
  raw_used_bytes: int = 0
  logical_usage_bytes: int = 0
  object_count: int = 0


class ClusterHealthResponse(BaseModel):
  generated_at: datetime
  auto_heal_enabled: bool
  auto_heal_authority_region: str
  summary: ClusterHealthSummary
  clusters: list[ClusterHealthItem]


class StorageOperationItem(BaseModel):
  id: str
  kind: Literal[
    "cluster_heal",
    "replication_reconcile",
    "replication_resync",
    "unmanaged_bucket_delete",
  ]
  status: Literal["queued", "running", "succeeded", "failed"]
  server: str
  bucket: str = ""
  target: str = ""
  actor: str
  message: str
  result: dict[str, Any] = Field(default_factory=dict)
  origin_region: str = ""
  celery_task_id: str = ""
  dispatch_attempts: int = 0
  dispatched_at: datetime | None = None
  created_at: datetime
  started_at: datetime | None = None
  finished_at: datetime | None = None


class StorageOperationListResponse(BaseModel):
  data: list[StorageOperationItem]


class ClusterHealStatusResponse(BaseModel):
  server: str
  reachable: bool
  status: str
  scanned_items: int = 0
  offline_nodes: list[str] = Field(default_factory=list)
  heal_disks: list[dict[str, Any]] = Field(default_factory=list)
  sets: list[dict[str, Any]] = Field(default_factory=list)
  error: str = ""
  checked_at: datetime
  latest_operation: StorageOperationItem | None = None
