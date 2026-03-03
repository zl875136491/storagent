from pydantic import BaseModel
from typing import List
from src.modules.public.schema import PydanticObjectId

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

class MinioServerResponse(BaseModel):
  id: PydanticObjectId
  region: SimpleRegionResponse
  name: str
  host: str
  port: int
  master: bool
  access_key: str
  secret_key: str

class MinioServerListResponse(BaseModel):
  data: List[MinioServerResponse]