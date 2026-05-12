from pydantic import BaseModel
from pydantic import Field

class BucketNodePositionRequest(BaseModel):
  bucket: str = Field(..., description="存储桶名称")
  server: str = Field(..., description="服务器名称")
  position_x: int = Field(..., description="节点位置X")
  position_y: int = Field(..., description="节点位置Y")

class BucketEdgePositionRequest(BaseModel):
  bucket: str = Field(..., description="存储桶名称")
  from_server: str = Field(..., description="起始服务器名称")
  to_server: str = Field(..., description="目标服务器名称")
  from_position: str = Field(..., description="起始节点位置")
  to_position: str = Field(..., description="目标节点位置")

class BucketNodePositionResponse(BaseModel):
  bucket: str = Field(..., description="存储桶名称")
  server: str = Field(..., description="服务器名称")
  position_x: int = Field(..., description="节点位置X")
  position_y: int = Field(..., description="节点位置Y")

class BucketEdgePositionResponse(BaseModel):
  bucket: str = Field(..., description="存储桶名称")
  from_server: str = Field(..., description="起始服务器名称")
  to_server: str = Field(..., description="目标服务器名称")
  from_position: str = Field(..., description="起始节点位置")
  to_position: str = Field(..., description="目标节点位置")