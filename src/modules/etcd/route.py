"""Etcd operations endpoints."""
from fastapi import APIRouter, Depends, File, Query, UploadFile

from src.core.auth import check_permissions, get_current_user
from src.core.exception import CustomException, ErrorDesc
from src.modules.auth.model import User
from src.modules.etcd import schema, service

router = APIRouter()


@router.get(
  "/operations/etcd",
  response_model=schema.EtcdClusterStatusResponse,
  summary="获取 Etcd 控制面健康状态",
)
async def get_etcd_operations(
  refresh: bool = Query(False),
  current_user: User = Depends(get_current_user),
):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await service.get_status(force_refresh=refresh)


async def _operator(current_user: User):
  await check_permissions(current_user, ["storage_operations_manage"])
  return current_user.username


@router.get("/operations/etcd/trend", response_model=schema.EtcdTrendResponse, summary="获取 Etcd 状态趋势")
async def get_etcd_trend(limit: int = Query(100, ge=1, le=500), current_user: User = Depends(get_current_user)):
  await _operator(current_user)
  return await service.trend(limit)


@router.get("/operations/etcd/keyspace", response_model=schema.EtcdOperationResponse, summary="检查 Etcd Key 空间")
async def get_etcd_keyspace(current_user: User = Depends(get_current_user)):
  actor = await _operator(current_user)
  return await service.keyspace(actor)


@router.get("/operations/etcd/revision-options", response_model=schema.EtcdRevisionOptionsResponse, summary="获取可选压缩 revision")
async def get_etcd_revision_options(current_user: User = Depends(get_current_user)):
  await _operator(current_user)
  return await service.revision_options()


@router.get("/operations/etcd/events", response_model=schema.EtcdEventListResponse, summary="获取 Etcd 运维事件")
async def get_etcd_events(limit: int = Query(50, ge=1, le=200), current_user: User = Depends(get_current_user)):
  await _operator(current_user)
  return await service.events(limit)


@router.post("/operations/etcd/tasks", response_model=schema.EtcdTaskResponse, summary="创建 Etcd 运维任务")
async def create_etcd_task(payload: schema.EtcdTaskRequest, current_user: User = Depends(get_current_user)):
  actor = await _operator(current_user)
  if payload.kind == "compact" and payload.revision is None:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "必须选择压缩 revision")
  return await service.create_task(payload.kind, actor, payload.revision)


@router.get("/operations/etcd/tasks", response_model=schema.EtcdTaskListResponse, summary="获取 Etcd 运维任务历史")
async def list_etcd_tasks(limit: int = Query(50, ge=1, le=200), current_user: User = Depends(get_current_user)):
  await _operator(current_user)
  return await service.tasks(limit)


@router.get("/operations/etcd/tasks/{task_id}", response_model=schema.EtcdTaskResponse, summary="获取 Etcd 运维任务")
async def get_etcd_task(task_id: str, current_user: User = Depends(get_current_user)):
  await _operator(current_user)
  result = await service.get_task(task_id)
  if result is None:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "Etcd 任务不存在")
  return result


@router.post("/operations/etcd/compact", response_model=schema.EtcdOperationResponse, summary="压缩 Etcd 历史")
async def compact_etcd(payload: schema.EtcdOperationRequest, current_user: User = Depends(get_current_user)):
  actor = await _operator(current_user)
  if not payload.confirm or payload.revision is None:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "必须确认操作并提供压缩 revision")
  return await service.compact(payload.revision, actor)


@router.post("/operations/etcd/defrag", response_model=schema.EtcdOperationResponse, summary="整理 Etcd 碎片")
async def defrag_etcd(payload: schema.EtcdOperationRequest, current_user: User = Depends(get_current_user)):
  actor = await _operator(current_user)
  if not payload.confirm:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "必须确认操作")
  return await service.defrag(actor)


@router.post("/operations/etcd/alarm-disarm", response_model=schema.EtcdOperationResponse, summary="解除 Etcd 活动告警")
async def disarm_etcd_alarm(payload: schema.EtcdOperationRequest, current_user: User = Depends(get_current_user)):
  actor = await _operator(current_user)
  if not payload.confirm:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "必须确认操作")
  return await service.disarm_alarm(actor)


@router.get("/operations/etcd/snapshot", summary="下载 Etcd 快照")
async def download_etcd_snapshot(current_user: User = Depends(get_current_user)):
  from fastapi.responses import Response
  actor = await _operator(current_user)
  payload, detail = await service.snapshot(actor)
  return Response(content=payload, media_type="application/octet-stream", headers={
    "Content-Disposition": f'attachment; filename="etcd-{detail["sha256"][:16]}.db"',
    "X-Snapshot-SHA256": detail["sha256"],
    "Cache-Control": "no-store",
  })


@router.post("/operations/etcd/restore", response_model=schema.EtcdOperationResponse, summary="登记 Etcd 快照恢复")
async def stage_etcd_restore(file: UploadFile = File(...), current_user: User = Depends(get_current_user)):
  actor = await _operator(current_user)
  payload = await file.read(max(int(getattr(service.settings, "ETCD_SNAPSHOT_MAX_BYTES", 1024 ** 3)) + 1, 1))
  return await service.stage_restore(payload, file.filename or "snapshot.db", actor)
