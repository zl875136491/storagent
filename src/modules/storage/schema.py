from pydantic import BaseModel, Field
from typing import List, Literal, Optional
from src.modules.public.schema import PydanticObjectId
from datetime import datetime
class MinioServerCreateRequest(BaseModel):
  region: PydanticObjectId
  name: str
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
  files: List[BucketFileItem] = Field(..., description="文件列表")

class ServerDetailsResponse(BaseModel):
  data: List[BucketInfo] = Field(..., description="文件详情")

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
  servers: List[str] = Field(..., description="服务器列表")
  replicates: List[dict] = Field(..., description="复制信息")

class BucketReplicateRuleStatus(BaseModel):
  status: str = Field(default="pending", max_length=32, description="复制规则状态")
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
