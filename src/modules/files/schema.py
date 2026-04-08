from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
from fastapi import UploadFile

class MultipartInitRequest(BaseModel):
  """初始化 S3 分片上传"""
  content_type: str = Field(default="application/octet-stream", description="对象 Content-Type")


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

