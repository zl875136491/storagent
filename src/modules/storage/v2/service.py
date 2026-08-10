"""v2 storage Service: unchanged operations delegate; share exchange is native v2."""
from __future__ import annotations

import secrets
from urllib.parse import parse_qs

from fastapi import Request
from fastapi.responses import HTMLResponse

from src.core.rate_limit import rate_limit_one_time_download
from src.modules.storage import download as storage_download


async def call(endpoint, *args, **kwargs):
  """Version boundary for inherited storage management operations."""
  return await endpoint(*args, **kwargs)


async def _read_share_token(request: Request) -> str:
  """Accept a compact form body after the browser removes URL fragment data."""
  content_type = request.headers.get("content-type", "").split(";", 1)[0].lower()
  if content_type != "application/x-www-form-urlencoded":
    raise storage_download.invalid_link_error()
  raw = await request.body()
  if len(raw) > 256:
    raise storage_download.invalid_link_error()
  try:
    values = parse_qs(raw.decode("ascii"), strict_parsing=True, max_num_fields=1)
  except (UnicodeDecodeError, ValueError):
    raise storage_download.invalid_link_error()
  token = values.get("token", [])
  if set(values) != {"token"} or len(token) != 1:
    raise storage_download.invalid_link_error()
  return token[0]


async def share_bootstrap() -> HTMLResponse:
  """Render the fragment-to-form bridge without sending capability in a URL."""
  nonce = secrets.token_urlsafe(18)
  return HTMLResponse(
    content=f'''<!doctype html><meta charset="utf-8"><meta name="referrer" content="no-referrer"><script nonce="{nonce}">(()=>{{const t=new URLSearchParams(location.hash.slice(1)).get("token");history.replaceState(null,"",location.pathname);if(!t){{document.body.textContent="下载链接无效或已过期";return}}const f=document.createElement("form");f.method="post";f.action=location.pathname;f.enctype="application/x-www-form-urlencoded";const i=document.createElement("input");i.type="hidden";i.name="token";i.value=t;f.append(i);document.body.append(f);f.submit()}})();</script>''',
    headers={
      "Cache-Control": "private, no-store, max-age=0",
      "Referrer-Policy": "no-referrer",
      "X-Content-Type-Options": "nosniff",
      "Content-Security-Policy": f"default-src 'none'; script-src 'nonce-{nonce}'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'",
    },
  )


async def redeem_share(request: Request):
  rate_limit_one_time_download(request)
  return await storage_download.redeem_one_time_download(await _read_share_token(request))
