from typing import Optional

from fastapi import APIRouter, Depends, File, Query, UploadFile, Form, Request
from src.core.auth import get_current_app_context
from src.core.rate_limit import rate_limit_locate
from src.modules.auth.model import User
from src.modules.files import schema as files_schema
from src.modules.files import service as files_service

router = APIRouter()

@router.post(
  path="/multipart/init",
  response_model=files_schema.MultipartInitResponse,
  summary="初始化分片上传")
async def multipart_init(
  payload: files_schema.MultipartInitRequest,
  app_context: dict = Depends(get_current_app_context),
) -> files_schema.MultipartInitResponse:
  content_type = payload.content_type
  return await files_service.multipart_init(
    app_context,
    content_type,
    size_bytes=payload.size_bytes,
  )


@router.post(
  path="/multipart/part",
  response_model=files_schema.MultipartPartResponse,
  summary="上传单个分片",
)
async def multipart_upload_part(
  upload_id: str = Form(...),
  object_key: str = Form(...),
  part_number: int = Form(...),
  file: UploadFile = File(...),
  app_context: dict = Depends(get_current_app_context),
) -> files_schema.MultipartPartResponse:
  return await files_service.multipart_upload_part(
    app_context=app_context,
    upload_id=upload_id,
    object_key=object_key,
    part_number=part_number,
    file=file,
  )

@router.post(
  path="/multipart/complete",
  response_model=files_schema.MultipartCompleteResponse,
  summary="完成分片上传",
)
async def multipart_complete(
  payload: files_schema.MultipartCompleteRequest,
  app_context: dict = Depends(get_current_app_context),
) -> files_schema.MultipartCompleteResponse:
  upload_id = payload.upload_id
  object_key = payload.object_key
  parts = payload.parts
  return await files_service.multipart_complete(
    app_context=app_context,
    upload_id=upload_id,
    object_key=object_key,
    parts=parts,
  )


@router.post(
  path="/multipart/abort",
  summary="中止分片上传",
)
async def multipart_abort(
  body: files_schema.MultipartAbortRequest,
  app_context: dict = Depends(get_current_app_context),
) -> dict:
  return await files_service.multipart_abort(body, app_context)


@router.get(
  path="/multipart/parts",
  response_model=files_schema.MultipartListPartsResponse,
  summary="列出已上传分片（断点续传）",
)
async def multipart_list_parts(
  upload_id: str = Query(...),
  object_key: str = Query(...),
  part_number_marker: Optional[str] = Query(None),
  app_context: dict = Depends(get_current_app_context)) -> files_schema.MultipartListPartsResponse:
  return await files_service.multipart_list_parts(
    app_context, object_key, upload_id, part_number_marker,
  )


@router.get(
  path="/object/locate",
  response_model=files_schema.ObjectLocateResponse,
  summary="定位对象所在服务点",
)
async def object_locate(
  request: Request,
  object_key: str = Query(..., description="对象键"),
  offset: int = Query(0, ge=0, description="下载起始字节（用于生成 download_url）"),
  length: int = Query(0, ge=0, description="下载长度（用于生成 download_url）"),
  app_context: dict = Depends(get_current_app_context),
) -> files_schema.ObjectLocateResponse:
  """
  扫描所有 MinIO 服务点，返回对象存在的位置及对应 stat/download 指引 URL。
  """
  rate_limit_locate(request)
  return await files_service.locate_object(app_context["app_name"], object_key, offset, length)


@router.post(
  path="/object/stat",
  response_model=files_schema.ObjectStatResponse,
  summary="获取对象元信息（用于规划分片下载）",
)
async def object_stat(
  payload: files_schema.ObjectStatRequest,
  app_context: dict = Depends(get_current_app_context),
) -> files_schema.ObjectStatResponse:
  return await files_service.stat_object(app_context["app_name"], payload.object_key)


@router.get(
  path="/object/download",
  summary="分片下载（Range/偏移）",
  response_model=None,
)
async def download_chunk(
  object_key: str = Query(..., description="对象键"),
  offset: int = Query(0, ge=0, description="起始字节"),
  length: int = Query(
    0,
    ge=0,
    description="读取长度；0 表示从 offset 读到末尾（流式）",
  ),
  app_context: dict = Depends(get_current_app_context),
):
  return await files_service.download_chunk(app_context, object_key, offset, length)
