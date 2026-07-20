from fastapi import APIRouter, Depends, File, Query, UploadFile, Form
from src.core.auth import get_current_user
from src.modules.auth.model import User

from src.modules.graph import service as graph_service
from src.modules.graph import schema as graph_schema
router = APIRouter()

@router.post(
  path="/bucket-node-position",
  response_model=graph_schema.BucketNodePositionResponse,
  summary="更新 Bucket 拓扑节点位置信息")
async def update_bucket_node_position(
  payload: graph_schema.BucketNodePositionRequest,
  current_user: User = Depends(get_current_user)):
  """
  更新 Bucket 拓扑节点位置信息
  """
  return await graph_service.update_bucket_node_position(payload)

@router.post(
  path="/bucket-edge-position",
  response_model=graph_schema.BucketEdgePositionResponse,
  summary="更新 Bucket 拓扑边位置信息")
async def update_bucket_edge_position(
  payload: graph_schema.BucketEdgePositionRequest,
  current_user: User = Depends(get_current_user)):
  """
  更新 Bucket 拓扑边位置信息
  """
  return await graph_service.update_bucket_edge_position(payload)

@router.get(
  path="/bucket-node-position",
  response_model=graph_schema.BucketNodePositionListResponse,
  summary="获取 Bucket 拓扑节点位置列表")
async def get_bucket_node_positions(
  bucket: str,
  current_user: User = Depends(get_current_user)):
  """
  获取指定存储桶的拓扑节点位置列表
  """
  return await graph_service.get_bucket_node_positions(bucket)

@router.get(
  path="/bucket-edge-position",
  response_model=graph_schema.BucketEdgePositionListResponse,
  summary="获取 Bucket 拓扑边位置列表")
async def get_bucket_edge_positions(
  bucket: str,
  current_user: User = Depends(get_current_user)):
  """
  获取指定存储桶的拓扑边位置列表
  """
  return await graph_service.get_bucket_edge_positions(bucket)