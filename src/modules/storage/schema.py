from pydantic import BaseModel, Field
from typing import List, Optional
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