"""虚拟存储 HTTPS API。管理凭据走 Authorization，租户 Key 走 X-API-Key。"""
from __future__ import annotations

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from src.virtual_storage.core.exception import InvalidCredentialError, VirtualStorageError, to_http_exception
from src.virtual_storage.core.minio_gateway import MinioGateway
from src.virtual_storage.core.security import issue_session, read_session, subject_hash
from src.virtual_storage.services.accounts import account_view, authenticate_management, get_account
from src.virtual_storage.services.lifecycle import (
  cancel_retire,
  freeze_tenant,
  get_operation,
  retire_tenant,
  unfreeze_tenant,
)
from src.virtual_storage.services.objects import list_objects, read_object, restore_object, soft_delete
from src.virtual_storage.services.tenants import (
  adjust_quota,
  authenticate_tenant_key,
  create_tenant,
  get_tenant,
  issue_tenant_key,
  list_tenant_keys,
  list_tenants,
  revoke_tenant_key,
)
from src.virtual_storage.services.uploads import (
  create_upload,
  get_upload,
  request_abort,
  request_complete,
  upload_part,
)
from src.virtual_storage.settings import get_settings

router = APIRouter()
CONSOLE_PATH = __file__.rsplit("/", 2)[0] + "/console/index.html"


def gateway_of(request: Request) -> MinioGateway:
  return request.app.state.gateway


def _bearer(authorization: str | None) -> str:
  if not authorization or not authorization.lower().startswith("bearer "):
    raise InvalidCredentialError("需要 Authorization: Bearer 管理凭据")
  return authorization.split(" ", 1)[1].strip()


def _management_secret(request: Request, authorization: str | None) -> str:
  if authorization:
    return _bearer(authorization)
  token = request.cookies.get("vs_session", "")
  payload = read_session(token)
  if not payload:
    raise InvalidCredentialError("需要管理凭据或控制台会话")
  request.state.session_app = payload.get("app_name")
  request.state.session_subject = payload.get("subject")
  raise InvalidCredentialError("控制台会话不能代替管理凭据明文；请使用 Bearer")


async def _account_from_request(app_name: str, request: Request, authorization: str | None):
  # 控制台只保存签名会话。数据面管理接口仍要求本次请求携带 Bearer。
  # 会话登录后，浏览器把凭据留在内存中的 Bearer，不写入审计。
  if authorization:
    return await authenticate_management(app_name, _bearer(authorization))
  token = request.cookies.get("vs_session", "")
  payload = read_session(token) or {}
  if payload.get("app_name") != app_name:
    raise InvalidCredentialError("控制台会话无效")
  account = await get_account(app_name)
  if subject_hash_matches(account, payload.get("subject", "")):
    return account
  raise InvalidCredentialError("控制台会话无效")


def subject_hash_matches(account, subject: str) -> bool:
  return bool(subject) and subject == account_subject(account)


def account_subject(account) -> str:
  # 会话里只比较登录时写入的 subject 摘要，不能从哈希反推凭据。
  return getattr(account, "_session_subject", "")


async def _guard(app_name: str, request: Request, authorization: str | None, credential: str | None = None):
  if credential:
    return await authenticate_management(app_name, credential)
  if authorization and authorization.lower().startswith("bearer "):
    return await authenticate_management(app_name, _bearer(authorization))
  payload = read_session(request.cookies.get("vs_session", ""))
  if not payload or payload.get("app_name") != app_name:
    raise InvalidCredentialError("需要管理凭据")
  account = await get_account(app_name)
  if payload.get("subject") != _stored_subject(request, account):
    raise InvalidCredentialError("控制台会话无效")
  return account


def _stored_subject(request: Request, account) -> str:
  return request.app.state.session_subjects.get(account.account_id, "")


@router.get("/ready")
async def ready():
  settings = get_settings()
  return {"status": "ok", "region": settings.region, "transactions": True, "service": "virtual-storage"}


@router.get("/healthz")
async def healthz():
  return {"status": "ok"}


def _oauth_error(exc) -> JSONResponse:
  return JSONResponse(
    status_code=exc.status,
    content={"error": exc.error, "error_description": exc.description},
  )


@router.post("/oauth2/token")
async def oauth_token(request: Request):
  from src.virtual_storage.services.machine_auth import (
    OAuthError,
    issue_client_credentials,
    parse_basic,
    reject_secret_in_form,
  )
  try:
    form = {key: value for key, value in (await request.form()).items()}
    reject_secret_in_form(form)
    if form.get("grant_type") != "client_credentials":
      raise OAuthError("unsupported_grant_type", "只接受 client_credentials")
    client_id, secret = parse_basic(request.headers.get("authorization"))
    return await issue_client_credentials(client_id, secret, str(form.get("scope") or ""))
  except OAuthError as exc:
    return _oauth_error(exc)


@router.post("/oauth2/refresh")
async def oauth_refresh(request: Request):
  from src.virtual_storage.services.machine_auth import (
    OAuthError,
    parse_basic,
    refresh_access_token,
    reject_secret_in_form,
  )
  try:
    form = {key: value for key, value in (await request.form()).items()}
    reject_secret_in_form(form)
    if form.get("grant_type") != "refresh_token":
      raise OAuthError("unsupported_grant_type", "只接受 refresh_token")
    client_id, secret = parse_basic(request.headers.get("authorization"))
    return await refresh_access_token(client_id, secret, str(form.get("refresh_token") or ""))
  except OAuthError as exc:
    return _oauth_error(exc)


@router.get("/console", response_class=HTMLResponse)
async def console_page():
  with open(CONSOLE_PATH, "r", encoding="utf-8") as handle:
    return HTMLResponse(handle.read())


@router.post("/console/session")
async def console_login(request: Request):
  body = await request.json()
  from src.virtual_storage.services.accounts import verify_client_secret
  account = await verify_client_secret(body["app_name"], body["credential"])
  subject = subject_hash(body["credential"])
  request.app.state.session_subjects[account.account_id] = subject
  token = issue_session(account.app_name, subject)
  response = JSONResponse({"app_name": account.app_name, "account_id": account.account_id})
  response.set_cookie(
    "vs_session",
    token,
    httponly=True,
    secure=get_settings().require_https,
    samesite="strict",
    path="/",
  )
  return response


@router.get("/api/virtual-storage/v1/accounts/{app_name}")
async def read_account(app_name: str, authorization: str | None = Header(default=None)):
  account = await authenticate_management(app_name, _bearer(authorization))
  return account_view(account)


@router.get("/api/virtual-storage/v1/accounts/{app_name}/tenants")
async def read_tenants(app_name: str, authorization: str | None = Header(default=None)):
  return await list_tenants(app_name, _bearer(authorization))


@router.post("/api/virtual-storage/v1/accounts/{app_name}/tenants")
async def post_tenant(
  app_name: str,
  request: Request,
  authorization: str | None = Header(default=None),
  idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
  if not idempotency_key:
    raise HTTPException(status_code=400, detail={"error_code": "IDEMPOTENCY_REQUIRED", "message": "缺少 Idempotency-Key"})
  credential = _bearer(authorization)
  body = await request.json()
  created = await create_tenant(
    app_name=app_name,
    credential=credential,
    idempotency_key=idempotency_key,
    external_tenant_id=body["external_tenant_id"],
    logical_name=body["logical_name"],
    quota_bytes=int(body["quota_bytes"]),
    quota_objects=int(body["quota_objects"]),
    key_ttl_seconds=body.get("key_ttl_seconds"),
    tenant_credential=body.get("tenant_credential") or None,
  )
  from src.virtual_storage.workers.queue import enqueue_scan
  enqueue_scan()
  return created


@router.get("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}")
async def read_tenant(app_name: str, tenant_id: str, authorization: str | None = Header(default=None)):
  return await get_tenant(app_name, _bearer(authorization), tenant_id)


@router.patch("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}")
async def patch_tenant(
  app_name: str,
  tenant_id: str,
  request: Request,
  authorization: str | None = Header(default=None),
  if_match: str | None = Header(default=None, alias="If-Match"),
):
  body = await request.json()
  return await adjust_quota(
    app_name=app_name,
    credential=_bearer(authorization),
    tenant_id=tenant_id,
    if_match=if_match or "",
    quota_bytes=int(body["quota_bytes"]),
    quota_objects=int(body["quota_objects"]),
  )


@router.get("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/api-keys")
async def read_tenant_keys(app_name: str, tenant_id: str, authorization: str | None = Header(default=None)):
  return await list_tenant_keys(app_name=app_name, credential=_bearer(authorization), tenant_id=tenant_id)


@router.post("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/api-keys")
async def post_tenant_key(
  app_name: str,
  tenant_id: str,
  request: Request,
  authorization: str | None = Header(default=None),
  idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
  if not idempotency_key:
    raise HTTPException(status_code=400, detail={"error_code": "IDEMPOTENCY_REQUIRED", "message": "缺少 Idempotency-Key"})
  body = await request.json()
  return await issue_tenant_key(
    app_name=app_name,
    credential=_bearer(authorization),
    tenant_id=tenant_id,
    idempotency_key=idempotency_key,
    key_ttl_seconds=body.get("key_ttl_seconds"),
  )


@router.delete("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/api-keys/{key_id}")
async def delete_tenant_key(
  app_name: str,
  tenant_id: str,
  key_id: str,
  authorization: str | None = Header(default=None),
):
  return await revoke_tenant_key(
    app_name=app_name,
    credential=_bearer(authorization),
    tenant_id=tenant_id,
    key_id=key_id,
  )


@router.post("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/freeze")
async def post_freeze(app_name: str, tenant_id: str, authorization: str | None = Header(default=None)):
  return await freeze_tenant(app_name, _bearer(authorization), tenant_id)


@router.post("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/unfreeze")
async def post_unfreeze(app_name: str, tenant_id: str, authorization: str | None = Header(default=None)):
  return await unfreeze_tenant(app_name, _bearer(authorization), tenant_id)


@router.post("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/retire")
async def post_retire(app_name: str, tenant_id: str, authorization: str | None = Header(default=None)):
  # 不在请求里立刻扫描。周期任务仍会清除，这段空档里页面可以撤销注销。
  return await retire_tenant(app_name, _bearer(authorization), tenant_id)


@router.post("/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}/cancel-retire")
async def post_cancel(app_name: str, tenant_id: str, authorization: str | None = Header(default=None)):
  return await cancel_retire(app_name, _bearer(authorization), tenant_id)


@router.get("/api/virtual-storage/v1/accounts/{app_name}/operations/{operation_id}")
async def read_operation(app_name: str, operation_id: str, authorization: str | None = Header(default=None)):
  return await get_operation(app_name, _bearer(authorization), operation_id)


async def _tenant_key(tenant_id: str, x_api_key: str | None):
  if not x_api_key:
    raise InvalidCredentialError("需要租户 API Key")
  return await authenticate_tenant_key(tenant_id, x_api_key)


@router.post("/api/virtual-storage/v1/tenants/{tenant_id}/uploads")
async def post_upload(tenant_id: str, request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
  tenant = await _tenant_key(tenant_id, x_api_key)
  account = await get_account_by_id(tenant.account_id)
  body = await request.json()
  return await create_upload(
    tenant=tenant,
    account=account,
    object_key=body["object_key"],
    size_bytes=int(body["size_bytes"]),
    content_type=body.get("content_type") or "application/octet-stream",
    gateway=gateway_of(request),
  )


@router.put("/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload_id}/parts/{part_number}")
async def put_part(
  tenant_id: str,
  upload_id: str,
  part_number: int,
  request: Request,
  x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
  tenant = await _tenant_key(tenant_id, x_api_key)
  data = await request.body()
  return await upload_part(
    tenant=tenant,
    upload_id=upload_id,
    part_number=part_number,
    data=data,
    gateway=gateway_of(request),
    checksum=request.headers.get("x-content-sha256"),
  )


@router.get("/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload_id}")
async def read_upload(tenant_id: str, upload_id: str, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
  tenant = await _tenant_key(tenant_id, x_api_key)
  return await get_upload(tenant.tenant_id, upload_id)


@router.post("/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload_id}/complete")
async def post_complete(tenant_id: str, upload_id: str, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
  tenant = await _tenant_key(tenant_id, x_api_key)
  body = await request_complete(tenant, upload_id)
  from src.virtual_storage.workers.queue import enqueue_scan
  enqueue_scan()
  return JSONResponse(body, status_code=202)


@router.delete("/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload_id}")
async def delete_upload(tenant_id: str, upload_id: str, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
  tenant = await _tenant_key(tenant_id, x_api_key)
  body = await request_abort(tenant, upload_id)
  from src.virtual_storage.workers.queue import enqueue_scan
  enqueue_scan()
  return body


@router.get("/api/virtual-storage/v1/tenants/{tenant_id}/objects")
async def read_objects(
  tenant_id: str,
  prefix: str = "",
  x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
  tenant = await _tenant_key(tenant_id, x_api_key)
  return await list_objects(tenant.tenant_id, prefix)


@router.get("/api/virtual-storage/v1/tenants/{tenant_id}/objects/{object_id}/content")
async def read_content(
  tenant_id: str,
  object_id: str,
  request: Request,
  range_header: str | None = Header(default=None, alias="Range"),
  x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
  tenant = await _tenant_key(tenant_id, x_api_key)
  payload = await read_object(tenant.tenant_id, object_id, gateway_of(request), range_header)
  headers = {
    "Accept-Ranges": "bytes",
    "ETag": payload["etag"],
    "Content-Type": payload["content_type"],
  }
  if payload["status"] == 206:
    headers["Content-Range"] = f"bytes {payload['start']}-{payload['end']}/{payload['size']}"
  return Response(content=payload["data"], status_code=payload["status"], headers=headers, media_type=payload["content_type"])


@router.delete("/api/virtual-storage/v1/tenants/{tenant_id}/objects/{object_id}")
async def delete_object(tenant_id: str, object_id: str, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
  tenant = await _tenant_key(tenant_id, x_api_key)
  return await soft_delete(tenant.tenant_id, object_id)


@router.post("/api/virtual-storage/v1/tenants/{tenant_id}/objects/{object_id}/restore")
async def post_restore(tenant_id: str, object_id: str, x_api_key: str | None = Header(default=None, alias="X-API-Key")):
  tenant = await _tenant_key(tenant_id, x_api_key)
  return await restore_object(tenant.tenant_id, object_id)


async def get_account_by_id(account_id: str):
  from src.virtual_storage.models.documents import VirtualAccount
  account = await VirtualAccount.find_one(VirtualAccount.account_id == account_id)
  if account is None:
    raise InvalidCredentialError("账号不存在")
  return account


@router.post("/internal/drive")
async def internal_drive(request: Request):
  """仅测试进程打开。部署环境不设置 VS_ENABLE_TEST_DRIVE。"""
  import os
  if os.getenv("VS_ENABLE_TEST_DRIVE", "").lower() not in {"1", "true", "yes", "on"}:
    raise HTTPException(status_code=404, detail={"error_code": "NOT_FOUND", "message": "not found"})
  body = await request.json()
  op = body.get("op")
  gateway = gateway_of(request)
  if op == "session":
    from src.virtual_storage.services.uploads import drive_session
    return await drive_session(body["upload_id"], gateway, "test-worker")
  if op == "retire":
    from src.virtual_storage.services.lifecycle import advance_retire
    return await advance_retire(body["tenant_id"], gateway, "test-worker")
  if op == "purge":
    from src.virtual_storage.services.objects import purge_object
    return await purge_object(body["object_id"], gateway, force=bool(body.get("force")))
  if op == "reconcile":
    from src.virtual_storage.services.reconcile import reconcile_tenant
    return await reconcile_tenant(body["tenant_id"])
  if op == "advance-inits":
    from src.virtual_storage.services.applications import advance_pending
    return {"advanced": await advance_pending()}
  if op == "account":
    from src.virtual_storage.services.accounts import account_view, create_account
    account = await create_account(
      app_name=body["app_name"],
      display_name=body.get("display_name") or body["app_name"],
      credential=body["credential"],
      quota_bytes=int(body["quota_bytes"]),
      quota_objects=int(body["quota_objects"]),
    )
    return account_view(account)
  if op == "plant_inflight":
    from src.virtual_storage.models.documents import MinioCall
    from src.virtual_storage.services.support import new_id
    await MinioCall(
      call_id=new_id("call"),
      resource_id=body["resource_id"],
      op="complete",
      fence_epoch=int(body.get("fence_epoch", 1)),
      state="open",
    ).insert()
    return {"ok": True}
  if op == "snapshot":
    from src.virtual_storage.services.reconcile import tenant_snapshot
    return await tenant_snapshot(body["tenant_id"])
  if op == "expire_delivery":
    from datetime import timedelta
    from src.utils.helpers import utc_now
    from src.virtual_storage.models.documents import KeyDelivery
    await KeyDelivery.get_motor_collection().update_one(
      {"key_id": body["key_id"]},
      {"$set": {"window_ends_at": utc_now() - timedelta(seconds=5)}},
    )
    return {"ok": True}
  if op == "audit_contains":
    from src.virtual_storage.models.documents import AuditRecord
    rows = await AuditRecord.find(AuditRecord.account_id == body["account_id"]).to_list()
    blob = " ".join(str(row.detail) for row in rows)
    return {"contains": body["needle"] in blob}
  if op == "inflate_reserved":
    from src.virtual_storage.models.documents import Tenant
    await Tenant.get_motor_collection().update_one(
      {"tenant_id": body["tenant_id"]},
      {"$set": {"quota_bytes_reserved": int(body["reserved"])}},
    )
    return {"ok": True}
  raise HTTPException(status_code=400, detail={"error_code": "BAD_OP", "message": "unknown op"})


def _console_actor(
  token: str | None,
  user_id: str | None,
  is_admin_header: str | None,
) -> tuple[str, bool]:
  from src.virtual_storage.services.console_access import assert_console_token
  assert_console_token(token)
  if not user_id:
    raise InvalidCredentialError("缺少控制台用户")
  return user_id, is_admin_header == "1"


@router.get("/internal/console/session")
async def console_session(
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
):
  from src.virtual_storage.services.console_access import session_overview
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await session_overview(user_id, is_admin)


@router.get("/internal/console/application-links")
async def console_application_links(
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
):
  from src.virtual_storage.services.console_access import application_links, assert_console_token
  assert_console_token(x_console_token)
  return await application_links()


@router.post("/internal/console/grants")
async def console_grant(request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import assert_console_token, grant_account
  assert_console_token(x_console_token)
  body = await request.json()
  return await grant_account(
    actor_is_admin=x_console_admin == "1",
    user_id=str(body["user_id"]),
    username=str(body["username"]),
    account_id=str(body["account_id"]),
  )


@router.delete("/internal/console/grants")
async def console_grant_delete(request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import assert_console_token, revoke_grant
  assert_console_token(x_console_token)
  body = await request.json()
  return await revoke_grant(
    actor_is_admin=x_console_admin == "1",
    user_id=str(body["user_id"]),
    account_id=str(body["account_id"]),
  )


@router.put("/internal/console/accounts/{account_id}/application")
async def console_link_application(account_id: str, request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import assert_console_token, link_application
  assert_console_token(x_console_token)
  body = await request.json()
  return await link_application(
    actor_is_admin=x_console_admin == "1",
    account_id=account_id,
    application_id=str(body.get("application_id") or ""),
  )


@router.get("/internal/console/accounts/{account_id}/tenants")
async def console_list_tenants(account_id: str, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_tenants
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await console_tenants(user_id, is_admin, account_id)


@router.post("/internal/console/accounts/{account_id}/tenants")
async def console_post_tenant(account_id: str, request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_create_tenant
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await console_create_tenant(user_id, is_admin, account_id, await request.json())


@router.patch("/internal/console/accounts/{account_id}/tenants/{tenant_id}")
async def console_patch_quota(account_id: str, tenant_id: str, request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_quota
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await console_quota(user_id, is_admin, account_id, tenant_id, await request.json())


@router.post("/internal/console/accounts/{account_id}/tenants/{tenant_id}/freeze")
@router.post("/internal/console/accounts/{account_id}/tenants/{tenant_id}/unfreeze")
@router.post("/internal/console/accounts/{account_id}/tenants/{tenant_id}/retire")
@router.post("/internal/console/accounts/{account_id}/tenants/{tenant_id}/cancel-retire")
async def console_post_action(account_id: str, tenant_id: str, request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_action
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  action = request.url.path.rstrip("/").rsplit("/", 1)[-1]
  return await console_action(user_id, is_admin, account_id, tenant_id, action)


@router.get("/internal/console/accounts/{account_id}/tenants/{tenant_id}/api-keys")
async def console_get_keys(account_id: str, tenant_id: str, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_keys
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await console_keys(user_id, is_admin, account_id, tenant_id)


@router.post("/internal/console/accounts/{account_id}/tenants/{tenant_id}/api-keys")
async def console_post_key(account_id: str, tenant_id: str, request: Request, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_issue_key
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  body = await request.json()
  return await console_issue_key(user_id, is_admin, account_id, tenant_id, str(body.get("idempotency_key") or ""))


@router.delete("/internal/console/accounts/{account_id}/tenants/{tenant_id}/api-keys/{key_id}")
async def console_delete_key(account_id: str, tenant_id: str, key_id: str, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_revoke_key
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await console_revoke_key(user_id, is_admin, account_id, tenant_id, key_id)


@router.get("/internal/console/accounts/{account_id}/operations/{operation_id}")
async def console_get_operation(account_id: str, operation_id: str, x_console_token: str | None = Header(default=None, alias="X-Console-Token"), x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"), x_console_admin: str | None = Header(default=None, alias="X-Console-Admin")):
  from src.virtual_storage.services.console_access import console_operation
  user_id, is_admin = _console_actor(x_console_token, x_console_user_id, x_console_admin)
  return await console_operation(user_id, is_admin, account_id, operation_id)


def _console_identity(
  token: str | None,
  user_id: str | None,
  username: str | None,
  is_admin_header: str | None,
  reviewer_header: str | None,
) -> tuple[str, str, bool, bool]:
  user_id, is_admin = _console_actor(token, user_id, is_admin_header)
  return user_id, (username or "").strip(), is_admin, reviewer_header == "1" or is_admin


@router.post("/internal/console/applications")
async def console_create_application(
  request: Request,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
  from src.virtual_storage.services.applications import create_application
  user_id, username, _admin, _reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, None,
  )
  body = await request.json()
  created = await create_application(
    user_id=user_id,
    username=username,
    public_account_id=str(body.get("public_account_id") or ""),
    purpose=str(body.get("purpose") or ""),
    quota_bytes=int(body.get("requested_quota_bytes") or 0),
    quota_objects=int(body.get("requested_quota_objects") or 0),
    idempotency_key=(idempotency_key or "").strip(),
  )
  return JSONResponse(status_code=201, content=created)


@router.get("/internal/console/applications")
async def console_list_applications(
  mine: int = 0,
  page: int = 1,
  page_size: int = 20,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  x_console_reviewer: str | None = Header(default=None, alias="X-Console-Reviewer"),
):
  from src.virtual_storage.services.applications import list_applications
  user_id, username, is_admin, reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, x_console_reviewer,
  )
  return await list_applications(
    user_id=user_id,
    username=username,
    reviewer=reviewer or is_admin,
    mine=bool(mine) or not (reviewer or is_admin),
    page=page,
    page_size=page_size,
  )


@router.get("/internal/console/applications/{application_id}")
async def console_get_application(
  application_id: str,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  x_console_reviewer: str | None = Header(default=None, alias="X-Console-Reviewer"),
):
  from src.virtual_storage.services.applications import get_application
  user_id, username, is_admin, reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, x_console_reviewer,
  )
  return await get_application(application_id, user_id, username, reviewer or is_admin)


@router.post("/internal/console/applications/{application_id}/withdraw")
async def console_withdraw(
  application_id: str,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  if_match: str | None = Header(default=None, alias="If-Match"),
):
  from src.virtual_storage.services.applications import withdraw_application
  user_id, username, _admin, _reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, None,
  )
  return await withdraw_application(application_id, user_id, username, if_match or "")


@router.post("/internal/console/applications/{application_id}/approve")
async def console_approve(
  application_id: str,
  request: Request,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  x_console_reviewer: str | None = Header(default=None, alias="X-Console-Reviewer"),
  if_match: str | None = Header(default=None, alias="If-Match"),
):
  from src.virtual_storage.services.applications import _subject, approve_application
  user_id, username, is_admin, reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, x_console_reviewer,
  )
  if not (reviewer or is_admin):
    from src.virtual_storage.core.exception import ConsoleForbiddenError
    raise ConsoleForbiddenError("没有虚拟账号审核权限")
  body = await request.json()
  approved = await approve_application(
    application_id=application_id,
    if_match=if_match or "",
    quota_bytes=int(body.get("quota_bytes") or 0),
    quota_objects=int(body.get("quota_objects") or 0),
    actor_id=_subject(user_id, username),
  )
  return approved


@router.post("/internal/console/applications/{application_id}/reject")
async def console_reject(
  application_id: str,
  request: Request,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  x_console_reviewer: str | None = Header(default=None, alias="X-Console-Reviewer"),
  if_match: str | None = Header(default=None, alias="If-Match"),
):
  from src.virtual_storage.services.applications import reject_application
  _user_id, _username, is_admin, reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, x_console_reviewer,
  )
  if not (reviewer or is_admin):
    from src.virtual_storage.core.exception import ConsoleForbiddenError
    raise ConsoleForbiddenError("没有虚拟账号审核权限")
  body = await request.json()
  return await reject_application(application_id, if_match or "", str(body.get("reason") or ""))


@router.post("/internal/console/applications/{application_id}/advance")
async def console_advance(
  application_id: str,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  x_console_reviewer: str | None = Header(default=None, alias="X-Console-Reviewer"),
):
  from src.virtual_storage.services.applications import advance_application, get_application
  user_id, username, is_admin, reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, x_console_reviewer,
  )
  await get_application(application_id, user_id, username, reviewer or is_admin)
  return await advance_application(application_id)


@router.post("/internal/console/applications/{application_id}/retry-init")
async def console_retry_init(
  application_id: str,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
):
  from src.virtual_storage.services.applications import retry_init
  user_id, username, is_admin, _reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, None,
  )
  return await retry_init(application_id, user_id, username, is_admin)


@router.post("/internal/console/accounts/{public_id}/secret-delivery")
async def console_secret_delivery(
  public_id: str,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
):
  from src.virtual_storage.services.applications import deliver_secret
  user_id, username, is_admin, _reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, None,
  )
  return await deliver_secret(public_id, user_id, username, is_admin)


@router.post("/internal/console/accounts/{public_id}/rotate-secret")
async def console_rotate_secret(
  public_id: str,
  x_console_token: str | None = Header(default=None, alias="X-Console-Token"),
  x_console_user_id: str | None = Header(default=None, alias="X-Console-User-Id"),
  x_console_username: str | None = Header(default=None, alias="X-Console-Username"),
  x_console_admin: str | None = Header(default=None, alias="X-Console-Admin"),
  if_match: str | None = Header(default=None, alias="If-Match"),
):
  from src.virtual_storage.services.applications import rotate_secret
  user_id, username, is_admin, _reviewer = _console_identity(
    x_console_token, x_console_user_id, x_console_username, x_console_admin, None,
  )
  return await rotate_secret(public_id, user_id, username, is_admin, if_match or "")


def install_error_handler(app):
  @app.exception_handler(VirtualStorageError)
  async def _handle(_request, exc: VirtualStorageError):
    return JSONResponse(status_code=exc.status_code, content=to_http_exception(exc).detail)

  from pymongo.errors import OperationFailure

  @app.exception_handler(OperationFailure)
  async def _write_conflict(_request, exc: OperationFailure):
    if exc.code == 112:
      return JSONResponse(status_code=409, content={"error_code": "WRITE_CONFLICT", "message": "并发写入冲突，请重试"})
    return JSONResponse(status_code=500, content={"error_code": "DATABASE_WRITE_FAILED", "message": "数据库写入失败"})
