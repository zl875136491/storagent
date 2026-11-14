"""控制台登录用户到虚拟账号的显式授权。不接受浏览器传来的管理凭据。"""
from __future__ import annotations

import contextvars
import hmac

from src.virtual_storage.core.exception import ConsoleForbiddenError, InvalidCredentialError
from src.virtual_storage.core.security import subject_hash
from src.virtual_storage.models.documents import VirtualAccount, VirtualAccountGrant
from src.virtual_storage.services.accounts import account_view
from src.virtual_storage.services.lifecycle import (
  cancel_retire,
  freeze_tenant,
  get_operation,
  retire_tenant,
  unfreeze_tenant,
)
from src.virtual_storage.services.support import new_id, stamp
from src.virtual_storage.services.tenants import (
  adjust_quota,
  create_tenant,
  issue_tenant_key,
  list_tenant_keys,
  list_tenants,
  revoke_tenant_key,
)
from src.virtual_storage.settings import get_settings


def assert_console_token(presented: str | None) -> None:
  expected = get_settings().console_token
  if not expected or not presented or not hmac.compare_digest(presented, expected):
    raise InvalidCredentialError("控制台桥接未授权")


console_username: contextvars.ContextVar[str] = contextvars.ContextVar("vs_console_username", default="")


def actor_subject(user_id: str) -> str:
  # 用户名在各区域稳定；Mongo 文档 id 会因同步而不同。
  identity = console_username.get().strip() or user_id
  return subject_hash(f"console-user:{identity}")


async def visible_accounts(user_id: str, is_admin: bool) -> tuple[str, list[VirtualAccount]]:
  if is_admin:
    accounts = await VirtualAccount.find_all().to_list()
    return "admin", accounts
  clauses = []
  if user_id:
    clauses.append({"user_id": user_id})
  username = console_username.get().strip()
  if username:
    clauses.append({"username": username})
  grants = await VirtualAccountGrant.get_motor_collection().find({"$or": clauses or [{"user_id": user_id}]}).to_list(100)
  accounts = []
  for grant in grants:
    account = await VirtualAccount.find_one(VirtualAccount.account_id == grant["account_id"])
    if account is not None:
      accounts.append(account)
  if not accounts:
    return "none", []
  return "virtual", accounts


async def require_account(user_id: str, is_admin: bool, account_id: str) -> VirtualAccount:
  role, accounts = await visible_accounts(user_id, is_admin)
  for account in accounts:
    if account.account_id == account_id:
      return account
  if role == "none":
    raise ConsoleForbiddenError("虚拟账号才可使用租户功能")
  raise ConsoleForbiddenError("无权访问该虚拟账号")


async def grant_account(*, actor_is_admin: bool, user_id: str, username: str, account_id: str) -> dict:
  if not actor_is_admin:
    raise ConsoleForbiddenError("只有管理员可以授权虚拟账号")
  account = await VirtualAccount.find_one(VirtualAccount.account_id == account_id)
  if account is None:
    raise ConsoleForbiddenError("虚拟账号不存在")
  existing = await VirtualAccountGrant.find_one(
    VirtualAccountGrant.user_id == user_id,
    VirtualAccountGrant.account_id == account_id,
  )
  if existing is None:
    existing = VirtualAccountGrant(
      grant_id=new_id("grant"),
      account_id=account_id,
      user_id=user_id,
      username=username,
      created_at=stamp(),
    )
    await existing.insert()
  return {"grant_id": existing.grant_id, "account_id": account_id, "user_id": user_id, "username": username}


async def revoke_grant(*, actor_is_admin: bool, user_id: str, account_id: str) -> dict:
  if not actor_is_admin:
    raise ConsoleForbiddenError("只有管理员可以取消授权")
  existing = await VirtualAccountGrant.find_one(
    VirtualAccountGrant.user_id == user_id,
    VirtualAccountGrant.account_id == account_id,
  )
  if existing is not None:
    await existing.delete()
  return {"account_id": account_id, "user_id": user_id, "granted": False}


async def link_application(*, actor_is_admin: bool, account_id: str, application_id: str) -> dict:
  if not actor_is_admin:
    raise ConsoleForbiddenError("只有管理员可以关联应用")
  account = await VirtualAccount.find_one(VirtualAccount.account_id == account_id)
  if account is None:
    raise ConsoleForbiddenError("虚拟账号不存在")
  application_id = application_id.strip()
  if application_id:
    other = await VirtualAccount.find_one(VirtualAccount.application_id == application_id)
    if other is not None and other.account_id != account.account_id:
      raise ConsoleForbiddenError("该应用已经关联到另一个虚拟账号")
  account.application_id = application_id
  account.updated_at = stamp()
  await account.save()
  return account_view(account)


async def application_links() -> list[dict]:
  linked = [item for item in await VirtualAccount.find_all().to_list() if item.application_id]
  return [
    {
      "application_id": item.application_id,
      "account_id": item.account_id,
      "app_name": item.app_name,
      "display_name": item.display_name,
    }
    for item in linked
  ]


def _view(account: VirtualAccount) -> dict:
  body = account_view(account)
  body["occupied_bytes"] = account.quota_bytes_used + account.quota_bytes_reserved + account.quota_bytes_trashed
  return body


async def session_overview(user_id: str, is_admin: bool) -> dict:
  role, accounts = await visible_accounts(user_id, is_admin)
  return {"role": role, "accounts": [_view(item) for item in accounts]}


async def console_tenants(user_id: str, is_admin: bool, account_id: str) -> list[dict]:
  account = await require_account(user_id, is_admin, account_id)
  return await list_tenants("", "", account=account)


async def console_create_tenant(user_id: str, is_admin: bool, account_id: str, body: dict) -> dict:
  account = await require_account(user_id, is_admin, account_id)
  logical_name = str(body.get("logical_name") or "").strip()
  if not logical_name:
    raise ConsoleForbiddenError("需要租户名")
  return await create_tenant(
    app_name=account.app_name,
    credential="",
    idempotency_key=str(body.get("idempotency_key") or ""),
    external_tenant_id=str(body.get("external_tenant_id") or logical_name).strip(),
    logical_name=logical_name,
    quota_bytes=int(body.get("quota_bytes") or 0),
    quota_objects=int(body.get("quota_objects") or 0),
    tenant_credential=body.get("tenant_credential") or None,
    account=account,
    actor_subject=actor_subject(user_id),
  )


async def console_quota(user_id: str, is_admin: bool, account_id: str, tenant_id: str, body: dict) -> dict:
  account = await require_account(user_id, is_admin, account_id)
  return await adjust_quota(
    app_name=account.app_name,
    credential="",
    tenant_id=tenant_id,
    if_match=str(body.get("revision") or ""),
    quota_bytes=int(body["quota_bytes"]),
    quota_objects=int(body["quota_objects"]),
    account=account,
  )


async def console_action(user_id: str, is_admin: bool, account_id: str, tenant_id: str, action: str) -> dict:
  account = await require_account(user_id, is_admin, account_id)
  operations = {
    "freeze": freeze_tenant,
    "unfreeze": unfreeze_tenant,
    "retire": retire_tenant,
    "cancel-retire": cancel_retire,
  }
  handler = operations.get(action)
  if handler is None:
    raise ConsoleForbiddenError("不支持的操作")
  return await handler(account.app_name, "", tenant_id, account=account)


async def console_keys(user_id: str, is_admin: bool, account_id: str, tenant_id: str) -> list[dict]:
  account = await require_account(user_id, is_admin, account_id)
  return await list_tenant_keys(app_name=account.app_name, credential="", tenant_id=tenant_id, account=account)


async def console_issue_key(user_id: str, is_admin: bool, account_id: str, tenant_id: str, idempotency_key: str) -> dict:
  account = await require_account(user_id, is_admin, account_id)
  scoped_key = idempotency_key.strip() or new_id("idem")
  return await issue_tenant_key(
    app_name=account.app_name,
    credential="",
    tenant_id=tenant_id,
    idempotency_key=f"issue:{scoped_key}",
    account=account,
    actor_subject=actor_subject(user_id),
  )


async def console_revoke_key(user_id: str, is_admin: bool, account_id: str, tenant_id: str, key_id: str) -> dict:
  account = await require_account(user_id, is_admin, account_id)
  return await revoke_tenant_key(
    app_name=account.app_name, credential="", tenant_id=tenant_id, key_id=key_id, account=account,
  )


async def console_operation(user_id: str, is_admin: bool, account_id: str, operation_id: str) -> dict:
  account = await require_account(user_id, is_admin, account_id)
  return await get_operation(account.app_name, "", operation_id, account=account)
