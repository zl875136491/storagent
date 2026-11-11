"""独立 FastAPI 应用。非归属区域把业务请求转发到归属区域。"""
from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from src.virtual_storage.api.routes import install_error_handler, router
from src.virtual_storage.core.database import close_virtual_storage_db, init_virtual_storage_db
from src.virtual_storage.core.exception import InfrastructureError
from src.virtual_storage.core.minio_gateway import MinioGateway
from src.virtual_storage.services.accounts import create_account
from src.virtual_storage.settings import get_settings
from src.utils.logger import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
  setup_logging()
  app.state.gateway = getattr(app.state, "gateway", None) or MinioGateway()
  app.state.session_subjects = {}
  await init_virtual_storage_db()
  settings = get_settings()
  if settings.minio_endpoint:
    app.state.gateway.ensure_bucket(settings.minio_bucket)
  if settings.bootstrap:
    if not settings.bootstrap_app_name or not settings.bootstrap_credential:
      raise InfrastructureError("引导账号缺少名称或管理凭据")
    await create_account(
      app_name=settings.bootstrap_app_name,
      display_name=settings.bootstrap_display_name,
      credential=settings.bootstrap_credential,
      quota_bytes=settings.bootstrap_quota_bytes,
      quota_objects=settings.bootstrap_quota_objects,
    )
  yield
  await close_virtual_storage_db()


def create_app() -> FastAPI:
  app = FastAPI(
    title="Storagent Virtual Storage",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
    lifespan=lifespan,
  )
  install_error_handler(app)

  @app.middleware("http")
  async def https_and_region(request: Request, call_next):
    settings = get_settings()
    if settings.require_https and request.url.scheme != "https" and request.url.path not in {"/healthz"}:
      return JSONResponse(
        status_code=400,
        content={"error_code": "HTTPS_REQUIRED", "message": "新入口只接受 HTTPS"},
      )
    if _should_forward(request.url.path):
      return await _forward(request)
    from src.virtual_storage.services.console_access import console_username
    username_token = console_username.set(request.headers.get("x-console-username", ""))
    try:
      return await call_next(request)
    finally:
      console_username.reset(username_token)

  app.include_router(router)
  return app


def _should_forward(path: str) -> bool:
  settings = get_settings()
  if not settings.home_base_url or settings.region == settings.home_region:
    return False
  if path in {"/ready", "/healthz"}:
    return False
  if path == "/console":
    return False
  if path in {"/oauth2/token", "/oauth2/refresh"}:
    return True
  return path.startswith("/api/virtual-storage/") or path.startswith("/console/")


async def _forward(request: Request) -> Response:
  settings = get_settings()
  url = settings.home_base_url + request.url.path
  if request.url.query:
    url = url + "?" + request.url.query
  headers = {
    key: value
    for key, value in request.headers.items()
    if key.lower() not in {"host", "content-length"}
  }
  body = await request.body()
  async with httpx.AsyncClient(verify=False, timeout=120.0) as client:
    upstream = await client.request(request.method, url, headers=headers, content=body)
  passthrough = {
    key: value
    for key, value in upstream.headers.items()
    if key.lower() not in {"content-encoding", "transfer-encoding", "content-length", "connection"}
  }
  return Response(content=upstream.content, status_code=upstream.status_code, headers=passthrough)
