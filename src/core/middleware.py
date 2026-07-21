"""
请求中间件：注入 X-Request-Id，累计 HTTP 指标。
"""
from __future__ import annotations

import time
import uuid

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from src.core import metrics as metrics_mod


class RequestContextMiddleware(BaseHTTPMiddleware):
  async def dispatch(self, request: Request, call_next) -> Response:
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    request.state.request_id = request_id
    start = time.perf_counter()
    metrics_mod.incr("http_requests_total")
    try:
      response = await call_next(request)
    except Exception:
      metrics_mod.incr("http_errors_total")
      raise
    elapsed_ms = (time.perf_counter() - start) * 1000
    metrics_mod.incr(f"http_status_{response.status_code // 100}xx_total")
    if response.status_code >= 500:
      metrics_mod.incr("http_errors_total")
    response.headers["X-Request-Id"] = request_id
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.1f}"
    return response
