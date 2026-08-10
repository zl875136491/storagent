"""v2 file API route declarations."""
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, Query, Request, UploadFile

from src.core.auth import get_current_app_context, optional_api_key_header, resolve_data_plane_context
from src.core.rate_limit import rate_limit_locate
from src.configs.consts import CAPABILITY_ACTION_DOWNLOAD, CAPABILITY_ACTION_UPLOAD_PART
from src.modules.files import schema as v1_schema
from src.modules.files.v2 import schema, service

router = APIRouter()


@router.post("/multipart/init")
async def multipart_init(request: Request, payload: v1_schema.MultipartInitRequest, context: dict = Depends(get_current_app_context)):
  return await service.wrap(request, await service.multipart_init(context, payload.content_type, size_bytes=payload.size_bytes))


@router.post("/multipart/complete")
async def multipart_complete(request: Request, payload: v1_schema.MultipartCompleteRequest, context: dict = Depends(get_current_app_context)):
  return await service.wrap(request, await service.multipart_complete(context, payload.upload_id, payload.object_key, payload.parts))


@router.post("/multipart/part")
async def multipart_part(request: Request, upload_id: str = Form(...), object_key: str = Form(...), part_number: int = Form(...), file: UploadFile = File(...), token: Optional[str] = Query(None), api_key: Optional[str] = Depends(optional_api_key_header)):
  context = await resolve_data_plane_context(api_key=api_key, token=token, action=CAPABILITY_ACTION_UPLOAD_PART, object_key=object_key, upload_id=upload_id)
  return await service.wrap(request, await service.multipart_part(app_context=context, upload_id=upload_id, object_key=object_key, part_number=part_number, file=file))


@router.post("/multipart/abort")
async def multipart_abort(request: Request, payload: v1_schema.MultipartAbortRequest, context: dict = Depends(get_current_app_context)):
  return await service.wrap(request, await service.multipart_abort(payload, context))


@router.get("/multipart/parts")
async def multipart_parts(request: Request, upload_id: str = Query(...), object_key: str = Query(...), part_number_marker: str | None = Query(None), context: dict = Depends(get_current_app_context)):
  return await service.wrap(request, await service.multipart_list_parts(context, object_key, upload_id, part_number_marker))


@router.get("/object/locate")
async def locate(request: Request, object_key: str = Query(...), offset: int = Query(0, ge=0), length: int = Query(0, ge=0), context: dict = Depends(get_current_app_context)):
  # v2 delegates the object lookup to v1 Service, but preserves the same
  # anti-scan control as the v1 HTTP boundary before it does so.
  rate_limit_locate(request)
  return await service.wrap(request, await service.locate(context["app_name"], object_key, offset, length))


@router.post("/object/stat")
async def stat(request: Request, payload: v1_schema.ObjectStatRequest, context: dict = Depends(get_current_app_context)):
  return await service.wrap(request, await service.stat(context["app_name"], payload.object_key))


@router.get("/object/download", response_model=None)
async def download(object_key: str = Query(...), offset: int = Query(0, ge=0), length: int = Query(0, ge=0), token: Optional[str] = Query(None), api_key: Optional[str] = Depends(optional_api_key_header)):
  context = await resolve_data_plane_context(api_key=api_key, token=token, action=CAPABILITY_ACTION_DOWNLOAD, object_key=object_key)
  return await service.download(context, object_key, offset, length)


@router.get("/objects", response_model=schema.ObjectListResponse)
async def list_objects(request: Request, prefix: str = Query(""), state: str = Query("active"), limit: int = Query(100, ge=1, le=1000), cursor: str | None = Query(None), context: dict = Depends(get_current_app_context)):
  return await service.list_objects(request, context["app_name"], prefix=prefix, state=state, limit=limit, cursor=cursor)


@router.delete("/objects/{object_id}", response_model=schema.ObjectMutationResponse)
async def delete(request: Request, object_id: str, context: dict = Depends(get_current_app_context)):
  return await service.delete(request, context["app_name"], object_id)


@router.post("/objects/{object_id}/restore", response_model=schema.ObjectMutationResponse)
async def restore(request: Request, object_id: str, context: dict = Depends(get_current_app_context)):
  return await service.restore(request, context["app_name"], object_id)


@router.post("/objects/{object_id}/share", response_model=schema.ShareCreateResponse)
async def share(request: Request, object_id: str, payload: schema.ShareCreateRequest, context: dict = Depends(get_current_app_context)):
  return await service.create_share(request, context["app_name"], object_id, payload)
