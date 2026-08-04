"""
进程内滑动窗口限流（按 key）。适合单实例；多实例需后续换 Redis。
"""
from __future__ import annotations

import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import Request

from src.core.exception import CustomException, ErrorDesc
from src.core import metrics as metrics_mod

_lock = Lock()
_hits: dict[str, deque[float]] = defaultdict(deque)


def _client_ip(request: Request) -> str:
  forwarded = request.headers.get("x-forwarded-for")
  if forwarded:
    return forwarded.split(",")[0].strip() or "unknown"
  if request.client and request.client.host:
    return request.client.host
  return "unknown"


def check_rate_limit(key: str, *, limit: int, window_seconds: float) -> None:
  """
  超出窗口内次数则抛出 RATE_LIMITED。
  """
  now = time.monotonic()
  cutoff = now - window_seconds
  with _lock:
    q = _hits[key]
    while q and q[0] < cutoff:
      q.popleft()
    if len(q) >= limit:
      metrics_mod.incr("rate_limit_hits_total")
      raise CustomException(
        ErrorDesc.RATE_LIMITED,
        f"请求过于频繁，请 {int(window_seconds)} 秒后再试",
      )
    q.append(now)


def rate_limit_login(request: Request) -> None:
  ip = _client_ip(request)
  check_rate_limit(f"login:{ip}", limit=10, window_seconds=60.0)


def rate_limit_refresh(request: Request) -> None:
  ip = _client_ip(request)
  check_rate_limit(f"refresh:{ip}", limit=30, window_seconds=60.0)


def rate_limit_locate(request: Request) -> None:
  ip = _client_ip(request)
  check_rate_limit(f"locate:{ip}", limit=60, window_seconds=60.0)


def rate_limit_one_time_download(request: Request) -> None:
  """Bound anonymous capability lookups by both claimed and direct peer IP."""
  ip = _client_ip(request)
  peer = request.client.host if request.client and request.client.host else "unknown"
  check_rate_limit(f"one-time-download:{ip}", limit=60, window_seconds=60.0)
  # A direct caller can forge X-Forwarded-For; the peer-wide guard still caps Etcd work.
  check_rate_limit(f"one-time-download-peer:{peer}", limit=600, window_seconds=60.0)


def rate_limit_ai(request: Request, username: str) -> None:
  ip = _client_ip(request)
  check_rate_limit(f"ai:{username}:{ip}", limit=30, window_seconds=60.0)


def rate_limit_oa_request(request: Request, username: str) -> None:
  ip = _client_ip(request)
  check_rate_limit(f"oa-request:{username}:{ip}", limit=5, window_seconds=600.0)


def rate_limit_oa_verify(request: Request, username: str) -> None:
  ip = _client_ip(request)
  check_rate_limit(f"oa-verify:{username}:{ip}", limit=30, window_seconds=60.0)
