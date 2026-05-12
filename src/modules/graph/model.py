from beanie import Document
from beanie.odm.fields import Link

from pydantic import Field
from pymongo import IndexModel


class BucketNodePosition(Document):
  """
  Bucket下的服务器节点位置
  """
  bucket: str = Field(..., description="存储桶名称")
  server: str = Field(..., description="服务器名称")
  position_x: int = Field(..., description="节点位置X")
  position_y: int = Field(..., description="节点位置Y")
  
  class Settings:
    name = "bucket_node_position"
    indexes = [
      IndexModel(["bucket", "server"], unique=True),
    ]

class BucketEdgePosition(Document):
  """
  Buckets拓扑图的边位置
  """
  bucket: str = Field(..., description="存储桶名称")
  from_server: str = Field(..., description="起始服务器名称")
  to_server: str = Field(...,description="目标服务器名称")
  from_position: str = Field(..., examples=["up", "down", "left", "right"],description="起始节点位置")
  to_position: str = Field(..., examples=["up", "down", "left", "right"],description="目标节点位置")
  
  class Settings:
    name = "bucket_edge_position"
    indexes = [
      IndexModel(["from_server", "to_server"], unique=True),
    ]
  