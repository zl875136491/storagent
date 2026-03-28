from pydantic import BaseModel
from typing import List
from src.modules.public.schema import PydanticObjectId
from datetime import datetime
class MinioServerCreateRequest(BaseModel):
  region: PydanticObjectId
  name: str
  host: str
  port: int
  access_key: str
  secret_key: str

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
  # access_key: str
  secret_key: str

class MinioServerListResponse(BaseModel):
  data: List[MinioServerResponse]

class BucketFileItem(BaseModel):
  name: str
  size: int
  last_modified: datetime
  children: List["BucketFileItem"] | None = None

class BucketInfo(BaseModel):
  name: str
  total_size: int
  created_at: datetime
  files: List[BucketFileItem]

class BucketsResponse(BaseModel):
  data: List[BucketInfo]