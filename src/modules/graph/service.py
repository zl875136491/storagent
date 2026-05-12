from src.modules.graph import crud as graph_crud
from src.modules.graph import schema as graph_schema

async def update_bucket_node_position(
  payload: graph_schema.BucketNodePositionRequest):
  """
  更新 Bucket 拓扑节点位置信息
  """
  bucket = payload.bucket
  server = payload.server
  position_x = payload.position_x
  position_y = payload.position_y
  return await graph_crud.update_bucket_node_position(bucket, server, position_x, position_y)

async def update_bucket_edge_position(
  payload: graph_schema.BucketEdgePositionRequest):
  """
  更新 Bucket 拓扑边位置信息
  """
  bucket = payload.bucket
  from_server = payload.from_server
  to_server = payload.to_server
  from_position = payload.from_position
  to_position = payload.to_position
  return await graph_crud.update_bucket_edge_position(bucket, from_server, to_server, from_position, to_position)