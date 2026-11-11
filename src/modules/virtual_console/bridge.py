"""把已登录的控制台用户映射到虚拟存储。管理凭据只留在虚拟存储服务里。"""
from __future__ import annotations

import json
import os
import ssl
import urllib.error
import urllib.request
from urllib.parse import quote

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response

from src.configs.consts import ROLE_SUPERADMIN
from src.modules.auth.crud import get_admin_role, read_user_by_username
from src.modules.auth.model import User
from src.modules.public.model import Application
from src.core.auth import get_current_user

router = APIRouter()


def _config() -> tuple[str, str]:
  base = os.getenv("VS_INTERNAL_BASE_URL", "https://127.0.0.1:8443").rstrip("/")
  token = os.getenv("VS_CONSOLE_TOKEN", "").strip()
  return base, token


def _call(method: str, path: str, *, admin: bool, user: User | None, body: dict | None = None) -> Response:
  base, token = _config()
  if not token:
    return JSONResponse(status_code=503, content={"error_code": "CONSOLE_BRIDGE_DISABLED", "message": "控制台桥接未配置"})
  headers = {
    "X-Console-Token": token,
    "X-Console-Admin": "1" if admin else "0",
    "Content-Type": "application/json",
  }
  if user is not None:
    headers["X-Console-User-Id"] = str(user.id)
    headers["X-Console-Username"] = user.username
  data = None if body is None else json.dumps(body).encode()
  request = urllib.request.Request(base + path, data=data, headers=headers, method=method)
  context = ssl._create_unverified_context()
  try:
    with urllib.request.urlopen(request, context=context, timeout=60) as response:
      payload = response.read()
      status = response.status
  except urllib.error.HTTPError as exc:
    payload = exc.read()
    status = exc.code
  except urllib.error.URLError:
    return JSONResponse(status_code=503, content={"error_code": "CONSOLE_BRIDGE_UNAVAILABLE", "message": "虚拟存储服务不可用"})
  return Response(content=payload, status_code=status, media_type="application/json")


async def user_is_admin(user: User) -> bool:
  admin_role = await get_admin_role()
  for role in user.roles or []:
    if getattr(role, "name", None) == ROLE_SUPERADMIN or getattr(role, "is_admin", False):
      return True
    if admin_role is not None and getattr(role, "id", None) == admin_role.id:
      return True
  return False


async def virtual_account_labels() -> dict[str, str]:
  base, token = _config()
  if not token:
    return {}
  request = urllib.request.Request(
    base + "/internal/console/application-links",
    headers={"X-Console-Token": token},
  )
  context = ssl._create_unverified_context()
  try:
    with urllib.request.urlopen(request, context=context, timeout=8) as response:
      rows = json.loads(response.read().decode() or "[]")
  except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError):
    return {}
  labels = {}
  for row in rows if isinstance(rows, list) else []:
    application_id = str(row.get("application_id") or "")
    if application_id:
      labels[application_id] = str(row.get("display_name") or row.get("app_name") or "")
  return labels


@router.get("/virtual-accounts")
async def list_virtual_accounts(user: User = Depends(get_current_user)):
  return _call("GET", "/internal/console/session", admin=await user_is_admin(user), user=user)


@router.get("/virtual-accounts/{account_id}/tenants")
async def list_account_tenants(account_id: str, user: User = Depends(get_current_user)):
  return _call("GET", f"/internal/console/accounts/{quote(account_id)}/tenants", admin=await user_is_admin(user), user=user)


@router.post("/virtual-accounts/{account_id}/tenants")
async def create_account_tenant(account_id: str, body: dict, user: User = Depends(get_current_user)):
  return _call("POST", f"/internal/console/accounts/{quote(account_id)}/tenants", admin=await user_is_admin(user), user=user, body=body)


@router.patch("/virtual-accounts/{account_id}/tenants/{tenant_id}")
async def update_account_tenant(account_id: str, tenant_id: str, body: dict, user: User = Depends(get_current_user)):
  path = f"/internal/console/accounts/{quote(account_id)}/tenants/{quote(tenant_id)}"
  return _call("PATCH", path, admin=await user_is_admin(user), user=user, body=body)


@router.post("/virtual-accounts/{account_id}/tenants/{tenant_id}/{action}")
async def act_on_tenant(account_id: str, tenant_id: str, action: str, user: User = Depends(get_current_user)):
  path = f"/internal/console/accounts/{quote(account_id)}/tenants/{quote(tenant_id)}/{quote(action)}"
  return _call("POST", path, admin=await user_is_admin(user), user=user, body={})


@router.get("/virtual-accounts/{account_id}/tenants/{tenant_id}/api-keys")
async def list_keys(account_id: str, tenant_id: str, user: User = Depends(get_current_user)):
  path = f"/internal/console/accounts/{quote(account_id)}/tenants/{quote(tenant_id)}/api-keys"
  return _call("GET", path, admin=await user_is_admin(user), user=user)


@router.post("/virtual-accounts/{account_id}/tenants/{tenant_id}/api-keys")
async def issue_key(account_id: str, tenant_id: str, body: dict, user: User = Depends(get_current_user)):
  path = f"/internal/console/accounts/{quote(account_id)}/tenants/{quote(tenant_id)}/api-keys"
  return _call("POST", path, admin=await user_is_admin(user), user=user, body=body)


@router.delete("/virtual-accounts/{account_id}/tenants/{tenant_id}/api-keys/{key_id}")
async def revoke_key(account_id: str, tenant_id: str, key_id: str, user: User = Depends(get_current_user)):
  path = f"/internal/console/accounts/{quote(account_id)}/tenants/{quote(tenant_id)}/api-keys/{quote(key_id)}"
  return _call("DELETE", path, admin=await user_is_admin(user), user=user)


@router.get("/virtual-accounts/{account_id}/operations/{operation_id}")
async def read_operation(account_id: str, operation_id: str, user: User = Depends(get_current_user)):
  path = f"/internal/console/accounts/{quote(account_id)}/operations/{quote(operation_id)}"
  return _call("GET", path, admin=await user_is_admin(user), user=user)


@router.post("/virtual-accounts/grants")
async def create_grant(body: dict, user: User = Depends(get_current_user)):
  if not await user_is_admin(user):
    return JSONResponse(status_code=403, content={"error_code": "CONSOLE_FORBIDDEN", "message": "只有管理员可以授权虚拟账号"})
  target = await read_user_by_username(str(body.get("username") or "").strip())
  if target is None:
    return JSONResponse(status_code=404, content={"error_code": "USER_NOT_FOUND", "message": "用户不存在"})
  return _call("POST", "/internal/console/grants", admin=True, user=user, body={
    "user_id": str(target.id),
    "username": target.username,
    "account_id": str(body.get("account_id") or ""),
  })


@router.delete("/virtual-accounts/grants")
async def delete_grant(body: dict, user: User = Depends(get_current_user)):
  if not await user_is_admin(user):
    return JSONResponse(status_code=403, content={"error_code": "CONSOLE_FORBIDDEN", "message": "只有管理员可以取消授权"})
  target = await read_user_by_username(str(body.get("username") or "").strip())
  if target is None:
    return JSONResponse(status_code=404, content={"error_code": "USER_NOT_FOUND", "message": "用户不存在"})
  return _call("DELETE", "/internal/console/grants", admin=True, user=user, body={
    "user_id": str(target.id),
    "account_id": str(body.get("account_id") or ""),
  })


@router.put("/virtual-accounts/{account_id}/application")
async def link_application(account_id: str, body: dict, user: User = Depends(get_current_user)):
  if not await user_is_admin(user):
    return JSONResponse(status_code=403, content={"error_code": "CONSOLE_FORBIDDEN", "message": "只有管理员可以关联应用"})
  application_id = str(body.get("application_id") or "").strip()
  if application_id:
    try:
      application = await Application.get(application_id)
    except Exception:
      application = None
    if application is None:
      return JSONResponse(status_code=404, content={"error_code": "APPLICATION_NOT_FOUND", "message": "应用不存在"})
  return _call(
    "PUT",
    f"/internal/console/accounts/{quote(account_id)}/application",
    admin=True,
    user=user,
    body={"application_id": application_id},
  )
