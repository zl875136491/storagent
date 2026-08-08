from fastapi import APIRouter, Depends, File, Form, Header, Query, UploadFile

from src.core.auth import get_current_user
from src.core.exception import CustomException, ErrorDesc
from src.modules.auth.model import User
from src.modules.files import schema as files_schema
from src.modules.files import service as files_service
from src.modules.public import crud as public_crud
from src.core.crypto import is_sha256_hex
from src.utils.helpers import before_compare, utc_now

router = APIRouter()


async def _resolve_demo_context(api_key_id: str, current_user: User) -> dict:
  """Resolve the browser's opaque API-key object ID without exposing its secret."""
  # APIKey.key is a SHA256 reference shared by every regional replica.
  # Accepting the Mongo ID as a compatibility fallback keeps older clients safe.
  api_key = await public_crud.read_api_key_by_hash(api_key_id)
  if not api_key:
    api_key = await public_crud.read_api_key_by_id(api_key_id)
  if not api_key or api_key.deleted:
    raise CustomException(ErrorDesc.API_KEY_INVALID, "APIKey 不存在或已吊销")
  if api_key.expired_at and before_compare(api_key.expired_at) < utc_now():
    raise CustomException(ErrorDesc.API_KEY_EXPIRED, "APIKey 已过期")

  application = await public_crud.read_application_by_id(api_key.application.id)
  if not application or application.author.id != current_user.id:
    raise CustomException(ErrorDesc.RES_NOT_BELONG_TO_USER, "APIKey 不属于当前用户")
  if not application.enabled or application.provisioning_status != "ready":
    raise CustomException(ErrorDesc.STATUS_ERR, "APIKey 关联应用尚未就绪")

  # The existing file service only needs the resolved application context.
  # The encrypted key is deliberately never copied into this response/context.
  return {
    "app_name": application.name,
    "app_shown_name": application.shown_name or application.name,
    "api_key_id": api_key.key,
    "api_key_hint": api_key.key_hint or "************",
    "auth_mode": "console_demo",
  }


async def _context(
  api_key_id: str = Header(..., alias="x-demo-api-key-id"),
  current_user: User = Depends(get_current_user),
) -> dict:
  return await _resolve_demo_context(api_key_id, current_user)


@router.get("/api-keys")
async def list_demo_api_keys(current_user: User = Depends(get_current_user)) -> dict:
  """Return only selectable, owned APIKey objects; never return key material."""
  applications = await public_crud.read_users_enabled_application_list(current_user)
  api_keys = await public_crud.read_api_key_by_app(applications, include_admin_destroyed=False)
  data = []
  for item in api_keys:
    if item.deleted or (item.expired_at and before_compare(item.expired_at) < utc_now()):
      continue
    application = await public_crud.read_application_by_id(item.application.id)
    if not application or not application.enabled or application.provisioning_status != "ready":
      continue
    data.append({
      # Use the replicated hash reference when available. Legacy records may
      # still store plaintext in `key`; use their Mongo object ID instead so a
      # secret can never be returned to the browser as a selector value.
      "id": item.key if is_sha256_hex(item.key) else str(item.id),
      "key_hint": item.key_hint or "************",
      "application": {
        "id": str(application.id),
        "name": application.name,
        "shown_name": application.shown_name,
      },
      "expired_at": item.expired_at,
    })
  return {"data": data}


@router.post("/files/multipart/init", response_model=files_schema.MultipartInitResponse)
async def demo_multipart_init(
  payload: files_schema.MultipartInitRequest,
  app_context: dict = Depends(_context),
):
  return await files_service.multipart_init(
    app_context, payload.content_type, size_bytes=payload.size_bytes,
  )


@router.post("/files/multipart/part", response_model=files_schema.MultipartPartResponse)
async def demo_multipart_part(
  upload_id: str = Form(...),
  object_key: str = Form(...),
  part_number: int = Form(...),
  file: UploadFile = File(...),
  app_context: dict = Depends(_context),
):
  return await files_service.multipart_upload_part(
    app_context=app_context, upload_id=upload_id, object_key=object_key,
    part_number=part_number, file=file,
  )


@router.post("/files/multipart/complete", response_model=files_schema.MultipartCompleteResponse)
async def demo_multipart_complete(
  payload: files_schema.MultipartCompleteRequest,
  app_context: dict = Depends(_context),
):
  return await files_service.multipart_complete(app_context, payload.upload_id, payload.object_key, payload.parts)


@router.post("/files/multipart/abort")
async def demo_multipart_abort(
  payload: files_schema.MultipartAbortRequest,
  app_context: dict = Depends(_context),
):
  return await files_service.multipart_abort(payload, app_context)


@router.post("/files/object/stat", response_model=files_schema.ObjectStatResponse)
async def demo_object_stat(
  payload: files_schema.ObjectStatRequest,
  app_context: dict = Depends(_context),
):
  return await files_service.stat_object(app_context["app_name"], payload.object_key)


@router.get("/files/object/locate", response_model=files_schema.ObjectLocateResponse)
async def demo_object_locate(
  object_key: str = Query(...),
  offset: int = Query(0, ge=0),
  length: int = Query(0, ge=0),
  app_context: dict = Depends(_context),
):
  return await files_service.locate_object(app_context["app_name"], object_key, offset, length)


@router.get("/files/object/download")
async def demo_object_download(
  object_key: str = Query(...),
  offset: int = Query(0, ge=0),
  length: int = Query(0, ge=0),
  app_context: dict = Depends(_context),
):
  return await files_service.download_chunk(app_context, object_key, offset, length)
