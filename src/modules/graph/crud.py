from bson import ObjectId
from typing import List, Dict
from beanie.operators import Set, In

from src.modules.graph.model import BucketNodePosition, BucketEdgePosition
from src.core.exception import CustomException, ErrorDesc

async def read_bucket_node_position(
  bucket: str,
  server: str) -> BucketNodePosition | None:
  """
  读取 Bucket 拓扑节点位置信息
  """
  return await BucketNodePosition.find_one(
    BucketNodePosition.bucket == bucket,
    BucketNodePosition.server == server
  )

async def create_bucket_node_position(
  bucket: str,
  server: str,
  position_x: int,
  position_y: int) -> BucketNodePosition:
  """
  创建 Bucket 拓扑节点位置信息
  """
  bucket_node_position = BucketNodePosition(
    bucket=bucket,
    server=server,
    position_x=position_x,
    position_y=position_y
  )
  await bucket_node_position.save()
  return bucket_node_position

async def read_many_bucket_node_positions(
  bucket: str) -> List[BucketNodePosition]:
  """
  读取多个 Bucket 拓扑节点位置信息
  """
  return await BucketNodePosition.find(
    BucketNodePosition.bucket == bucket
  ).to_list()


async def update_bucket_node_position(
  bucket: str,
  server: str,
  position_x: int,
  position_y: int) -> BucketNodePosition:
  """
  更新 Bucket 拓扑节点位置信息
  """
  bucket_node_position = await BucketNodePosition.find_one(
    BucketNodePosition.bucket == bucket,
    BucketNodePosition.server == server
  )
  if not bucket_node_position:
    return await create_bucket_node_position(bucket, server, position_x, position_y)
  bucket_node_position.position_x = position_x
  bucket_node_position.position_y = position_y
  await bucket_node_position.save()
  return bucket_node_position

async def read_bucket_edge_position(
  bucket: str,
  from_server: str,
  to_server: str) -> BucketEdgePosition | None:
  """
  读取 Bucket 拓扑边位置信息
  """
  return await BucketEdgePosition.find_one(
    BucketEdgePosition.bucket == bucket,
    BucketEdgePosition.from_server == from_server,
    BucketEdgePosition.to_server == to_server
  )

async def read_many_bucket_edge_positions(
  bucket: str) -> List[BucketEdgePosition]:
  """
  读取多个 Bucket 拓扑边位置信息
  """
  return await BucketEdgePosition.find(
    BucketEdgePosition.bucket == bucket
  ).to_list()

async def create_bucket_edge_position(
  bucket: str,
  from_server: str,
  to_server: str,
  from_position: str,
  to_position: str) -> BucketEdgePosition:
  """
  创建 Bucket 拓扑边位置信息
  """
  bucket_edge_position = BucketEdgePosition(
    bucket=bucket,
    from_server=from_server,
    to_server=to_server,
    from_position=from_position,
    to_position=to_position
  )
  await bucket_edge_position.save()
  return bucket_edge_position

async def update_bucket_edge_position(
  bucket: str,
  from_server: str,
  to_server: str,
  from_position: str,
  to_position: str) -> BucketEdgePosition:
  """
  更新 Bucket 拓扑边位置信息
  """
  bucket_edge_position = await BucketEdgePosition.find_one(
    BucketEdgePosition.bucket == bucket,
    BucketEdgePosition.from_server == from_server,
    BucketEdgePosition.to_server == to_server
  )
  if not bucket_edge_position:
    return await create_bucket_edge_position(bucket, from_server, to_server, from_position, to_position)
  bucket_edge_position.from_position = from_position
  bucket_edge_position.to_position = to_position
  await bucket_edge_position.save()
  return bucket_edge_position

async def delete_bucket_edge_position(
  bucket: str,
  from_server: str,
  to_server: str) -> bool:
  """
  删除 Bucket 拓扑边位置信息
  """
  bucket_edge_position = await BucketEdgePosition.find_one(
    BucketEdgePosition.bucket == bucket,
    BucketEdgePosition.from_server == from_server,
    BucketEdgePosition.to_server == to_server
  )
  if not bucket_edge_position:
    return False
  await bucket_edge_position.delete()
  return True