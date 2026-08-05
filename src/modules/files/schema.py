from pydantic import BaseModel, Field
from typing import List, Literal, Optional
from datetime import datetime
from fastapi import UploadFile

class MultipartInitRequest(BaseModel):
  """初始化 S3 分片上传"""
  content_type: str = Field(default="application/octet-stream", description="对象 Content-Type")
  size_bytes: int = Field(
    ...,
    gt=0,
    description="上传对象的精确总字节数，用于跨区域配额预留",
  )


class MultipartInitResponse(BaseModel):
  upload_id: str
  bucket: str
  object_key: str

class MultipartUploadPartRequest(BaseModel):
  upload_id: str = Field(..., description="init 返回的 upload_id")
  object_key: str = Field(..., description="init 返回的 object_key")
  part_number: int = Field(..., ge=1, le=10000, description="分片序号，从 1 开始")
  file: UploadFile = Field(..., description="本分片的二进制内容")

class MultipartPartResponse(BaseModel):
  part_number: int
  etag: str


class MultipartPartItem(BaseModel):
  part_number: int = Field(..., ge=1, le=10000)
  etag: str = Field(..., min_length=1, description="UploadPart 返回的 ETag（可带或不带引号）")


class MultipartCompleteRequest(BaseModel):
  upload_id: str = Field(..., description="init 返回的 upload_id")
  object_key: str = Field(..., description="init 返回的 object_key")
  parts: List[MultipartPartItem] = Field(..., min_length=1, description="按 part_number 升序排列的已上传分片")


class MultipartAbortRequest(BaseModel):
  bucket: Optional[str] = None
  object_key: str
  upload_id: str


class MultipartCompleteResponse(BaseModel):
  bucket: str
  object_key: str
  etag: Optional[str] = None
  version_id: Optional[str] = None


class MultipartPartListed(BaseModel):
  part_number: int
  etag: str
  size: Optional[int] = None
  last_modified: Optional[datetime] = None


class MultipartListPartsResponse(BaseModel):
  bucket: Optional[str] = None
  object_key: Optional[str] = None
  upload_id: str
  parts: List[MultipartPartListed]


class ObjectStatResponse(BaseModel):
  bucket: str
  object_key: str
  size: int
  etag: str
  content_type: Optional[str] = None
  last_modified: Optional[datetime] = None
  region: Optional[str] = Field(None, description="对象所在区域标识")
  local: bool = Field(True, description="是否位于当前访问节点")


class ObjectStatRequest(BaseModel):
  object_key: str = Field(..., min_length=1, description="对象键")


class ObjectLocationItem(BaseModel):
  region: str = Field(..., description="区域标识")
  shown_name: str = Field(..., description="区域显示名称")
  master: bool = Field(..., description="是否为该 Region 的本地主节点")
  endpoint: str = Field(..., description="Storagent API 地址")
  stat_url: str = Field(..., description="对象元信息 POST 地址")
  stat_method: Literal["POST"] = "POST"
  stat_body: dict[str, str] = Field(..., description="对象元信息请求体")
  download_url: str = Field(..., description="对象下载地址（需携带相同 x-api-key）")


class ObjectLocateResponse(BaseModel):
  bucket: str
  object_key: str
  current_region: str
  local_exists: bool
  available_at: List[ObjectLocationItem] = Field(default_factory=list)
