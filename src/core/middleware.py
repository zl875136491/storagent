"""
请求中间件：注入 X-Request-Id，累计 HTTP 指标。
"""
from __future__ import annotations

import time
import uuid
import re
from contextvars import ContextVar
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from starlette.responses import JSONResponse

from src.configs.configs import settings
from src.configs.consts import API_V1_PREFIX, API_V2_PREFIX
from src.core import metrics as metrics_mod
from src.core.exception import ErrorDesc


_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
current_request_id: ContextVar[str] = ContextVar("current_request_id", default="")


class UploadBodyLimitMiddleware:
  """Reject oversized multipart parts before Starlette spools the request."""

  _PATHS = {
    f"{API_V1_PREFIX}/files/multipart/part",
    f"{API_V2_PREFIX}/files/multipart/part",
  }
  _MULTIPART_OVERHEAD_BYTES = 1024 ** 2

  def __init__(self, app: Any):
    self.app = app

  @staticmethod
  def _header(scope: dict, name: bytes) -> bytes | None:
    for key, value in scope.get("headers") or []:
      if key.lower() == name:
        return value
    return None

  @staticmethod
  def _response(reason: str, path: str, request_id: str) -> JSONResponse:
    error = ErrorDesc.UPLOAD_PART_TOO_LARGE
    if path.startswith(API_V2_PREFIX):
      content = {
        "error": {
          "code": "upload.part_too_large",
          "message": error.message,
          "retryable": False,
          "details": {},
        },
        "request_id": request_id,
      }
    else:
      content = {"msg": error.message, "data": reason, "code": error.code}
    return JSONResponse(
      status_code=error.code // 1000,
      content=content,
      headers={"Connection": "close", "X-Request-Id": request_id},
    )

  async def __call__(self, scope: dict, receive, send) -> None:
    if scope.get("type") != "http" or scope.get("path") not in self._PATHS:
      await self.app(scope, receive, send)
      return

    part_limit = max(int(settings.APPLICATION_UPLOAD_MAX_PART_BYTES), 1)
    body_limit = part_limit + self._MULTIPART_OVERHEAD_BYTES
    reason = f"单个上传分片不能超过 {part_limit} 字节"
    supplied_request_id = (self._header(scope, b"x-request-id") or b"").decode("ascii", "ignore").strip()
    request_id = supplied_request_id if _REQUEST_ID_PATTERN.fullmatch(supplied_request_id) else uuid.uuid4().hex
    raw_length = self._header(scope, b"content-length")
    if raw_length is not None:
      try:
        if int(raw_length) > body_limit:
          await self._response(reason, scope["path"], request_id)(scope, receive, send)
          return
      except (TypeError, ValueError):
        pass

    received = 0
    exceeded = False

    async def limited_receive():
      nonlocal received, exceeded
      message = await receive()
      if message.get("type") == "http.request":
        received += len(message.get("body") or b"")
        if received > body_limit:
          exceeded = True
          raise RuntimeError("multipart request body limit exceeded")
      return message

    async def guarded_send(message):
      if not exceeded:
        await send(message)

    try:
      await self.app(scope, limited_receive, guarded_send)
    except BaseException:
      if not exceeded:
        raise
    if exceeded:
      await self._response(reason, scope["path"], request_id)(scope, receive, send)


class RequestContextMiddleware(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next) -> Response:
    # The value reaches logs and response headers, so accept only a bounded
    # correlation identifier and regenerate malformed client input.
    supplied_request_id = request.headers.get("x-request-id", "").strip()
    request_id = supplied_request_id if _REQUEST_ID_PATTERN.fullmatch(supplied_request_id) else uuid.uuid4().hex
    request.state.request_id = request_id
    token = current_request_id.set(request_id)
    start = time.perf_counter()
    metrics_mod.incr("http_requests_total")
    try:
      try:
        response = await call_next(request)
      except Exception:
        metrics_mod.incr("http_errors_total")
        raise
    finally:
      current_request_id.reset(token)
    elapsed_ms = (time.perf_counter() - start) * 1000
    metrics_mod.incr(f"http_status_{response.status_code // 100}xx_total")
    if response.status_code >= 500:
      metrics_mod.incr("http_errors_total")
    response.headers["X-Request-Id"] = request_id
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.1f}"
    return response
