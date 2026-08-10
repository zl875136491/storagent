"""v2 anonymous one-time share exchange route."""
from urllib.parse import parse_qs
import secrets

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.core.rate_limit import rate_limit_one_time_download
from src.modules.storage import download as storage_download

router = APIRouter()


async def _token(request: Request) -> str:
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


@router.get("/objects/one-time-download", name="v2_one_time_share_exchange", response_model=None)
async def bootstrap():
  nonce = secrets.token_urlsafe(18)
  return HTMLResponse(
    content=f'''<!doctype html><meta charset="utf-8"><meta name="referrer" content="no-referrer"><script nonce="{nonce}">(()=>{{const t=new URLSearchParams(location.hash.slice(1)).get("token");history.replaceState(null,"",location.pathname);if(!t){{document.body.textContent="下载链接无效或已过期";return}}const f=document.createElement("form");f.method="post";f.action=location.pathname;const i=document.createElement("input");i.type="hidden";i.name="token";i.value=t;f.append(i);document.body.append(f);f.submit()}})();</script>''',
    headers={"Cache-Control": "private, no-store, max-age=0", "Referrer-Policy": "no-referrer", "Content-Security-Policy": f"default-src 'none'; script-src 'nonce-{nonce}'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"},
  )


@router.post("/objects/one-time-download", response_model=None)
async def redeem(request: Request):
  rate_limit_one_time_download(request)
  return await storage_download.redeem_one_time_download(await _token(request))
