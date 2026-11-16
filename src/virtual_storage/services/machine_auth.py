"""机器身份。client_secret 只出现在换票和刷新，管理接口只接受 access token。"""
from __future__ import annotations

import hashlib
import secrets
from base64 import b64decode
from datetime import timedelta
from urllib.parse import unquote

from jose import JWTError, jwt

from src.utils.helpers import utc_now
from src.virtual_storage.core.exception import (
  AccountDisabledError,
  AccountNotFoundError,
  AccountScopeError,
  InvalidCredentialError,
)
from src.virtual_storage.core.security import hash_management_credential, hint
from src.virtual_storage.models.documents import OAuthRefreshToken, VirtualAccount
from src.virtual_storage.settings import get_settings

ACCESS_SCOPE = "vs.account.manage"
TOKEN_TYPE = "vs_account_access"
AUDIENCE = "virtual-storage"


class OAuthError(Exception):
  def __init__(self, error: str, description: str, status: int = 400):
    self.error = error
    self.description = description
    self.status = status
    super().__init__(description)


def parse_basic(authorization: str | None) -> tuple[str, str]:
  if not authorization or not authorization.lower().startswith("basic "):
    raise OAuthError("invalid_client", "需要 Basic 客户端认证", 401)
  encoded = authorization.split(" ", 1)[1].strip()
  try:
    decoded = b64decode(encoded).decode("utf-8")
  except (ValueError, UnicodeError) as exc:
    raise OAuthError("invalid_client", "Basic 认证无法解析", 401) from exc
  if ":" not in decoded:
    raise OAuthError("invalid_client", "Basic 认证无法解析", 401)
  client_id, secret = decoded.split(":", 1)
  return unquote(client_id), unquote(secret)


def reject_secret_in_form(form: dict) -> None:
  if form.get("client_id") or form.get("client_secret"):
    raise OAuthError("invalid_request", "不接受把 client_secret 放在表单里")


def _issuer() -> str:
  settings = get_settings()
  if settings.oauth_issuer:
    return settings.oauth_issuer
  return f"virtual-storage:{settings.home_region}"


def _secret() -> str:
  secret = get_settings().oauth_secret
  if not secret:
    raise OAuthError("server_error", "机器令牌签名密钥未配置", 500)
  return secret


def _hash_refresh(token: str) -> str:
  return hashlib.sha256(b"vs-refresh:" + token.encode("utf-8")).hexdigest()


def decode_access_token(token: str) -> dict:
  if not token or token.count(".") != 2:
    raise InvalidCredentialError("需要 oauth2 access token")
  try:
    payload = jwt.decode(
      token,
      _secret(),
      algorithms=["HS256"],
      audience=AUDIENCE,
      issuer=_issuer(),
      options={"leeway": 60, "require_aud": True, "require_exp": True, "require_sub": True},
    )
  except JWTError as exc:
    raise InvalidCredentialError("access token 无效") from exc
  except OAuthError as exc:
    raise InvalidCredentialError(exc.description) from exc
  if payload.get("typ") != TOKEN_TYPE:
    raise InvalidCredentialError("access token 无效")
  scope = str(payload.get("scope") or "")
  if ACCESS_SCOPE not in scope.split():
    raise InvalidCredentialError("access token 无效")
  return payload


async def authenticate_access_token(public_id: str, token: str) -> VirtualAccount:
  payload = decode_access_token(token)
  if payload.get("sub") != public_id:
    raise AccountScopeError()
  from src.virtual_storage.services.accounts import find_account
  try:
    account = await find_account(public_id)
  except AccountNotFoundError as exc:
    raise InvalidCredentialError("access token 无效") from exc
  if (account.public_account_id or account.app_name) != public_id:
    raise AccountScopeError()
  if not account.enabled:
    raise AccountDisabledError(public_id)
  if int(payload.get("credential_version") or -1) != int(account.credential_version or 1):
    raise InvalidCredentialError("access token 已失效")
  return account


def _scope_from_request(raw: str | None) -> str:
  requested = (raw or "").strip()
  if not requested:
    return ACCESS_SCOPE
  parts = requested.split()
  if any(part != ACCESS_SCOPE for part in parts):
    raise OAuthError("invalid_scope", "scope 不能超出 vs.account.manage")
  return ACCESS_SCOPE


async def _account_for_client(client_id: str, secret: str) -> VirtualAccount:
  from src.virtual_storage.core.security import verify_management_credential
  from src.virtual_storage.services.accounts import find_account
  try:
    account = await find_account(client_id)
  except AccountNotFoundError as exc:
    raise OAuthError("invalid_client", "client 认证失败", 401) from exc
  if not account.enabled:
    raise OAuthError("unauthorized_client", "账号已停用", 401)
  if not verify_management_credential(secret, account.management_credential_hash):
    raise OAuthError("invalid_client", "client 认证失败", 401)
  return account


def _access_token(account: VirtualAccount, scope: str) -> tuple[str, int]:
  settings = get_settings()
  expires = int(settings.access_token_seconds)
  now = utc_now()
  claims = {
    "iss": _issuer(),
    "aud": AUDIENCE,
    "sub": account.public_account_id or account.app_name,
    "typ": TOKEN_TYPE,
    "scope": scope,
    "credential_version": int(account.credential_version or 1),
    "iat": int(now.timestamp()),
    "nbf": int(now.timestamp()),
    "exp": int((now + timedelta(seconds=expires)).timestamp()),
  }
  return jwt.encode(claims, _secret(), algorithm="HS256"), expires


async def _store_refresh(account: VirtualAccount) -> str:
  raw = secrets.token_urlsafe(48)
  settings = get_settings()
  await OAuthRefreshToken(
    token_id=secrets.token_hex(16),
    public_account_id=account.public_account_id or account.app_name,
    credential_version=int(account.credential_version or 1),
    token_hash=_hash_refresh(raw),
    expires_at=utc_now() + timedelta(seconds=int(settings.refresh_token_seconds)),
  ).insert()
  return raw


def _token_response(access: str, expires: int, refresh: str, scope: str) -> dict:
  return {
    "access_token": access,
    "refresh_token": refresh,
    "token_type": "Bearer",
    "expires_in": expires,
    "scope": scope,
  }


async def issue_client_credentials(client_id: str, secret: str, scope: str | None) -> dict:
  granted = _scope_from_request(scope)
  account = await _account_for_client(client_id, secret)
  access, expires = _access_token(account, granted)
  refresh = await _store_refresh(account)
  return _token_response(access, expires, refresh, granted)


async def refresh_access_token(client_id: str, secret: str, refresh_token: str) -> dict:
  if not refresh_token:
    raise OAuthError("invalid_request", "缺少 refresh_token")
  account = await _account_for_client(client_id, secret)
  public_id = account.public_account_id or account.app_name
  if client_id != public_id and client_id != account.app_name:
    raise OAuthError("invalid_client", "client 认证失败", 401)
  stored = await OAuthRefreshToken.find_one(OAuthRefreshToken.token_hash == _hash_refresh(refresh_token))
  if stored is None or stored.revoked or stored.public_account_id != public_id:
    raise OAuthError("invalid_grant", "refresh token 无效")
  expires_at = stored.expires_at
  if expires_at.tzinfo is None:
    from datetime import timezone
    expires_at = expires_at.replace(tzinfo=timezone.utc)
  if expires_at <= utc_now():
    raise OAuthError("invalid_grant", "refresh token 已过期")
  if int(stored.credential_version) != int(account.credential_version or 1):
    raise OAuthError("invalid_grant", "refresh token 已失效")
  stored.revoked = True
  await stored.save()
  access, expires = _access_token(account, ACCESS_SCOPE)
  refresh = await _store_refresh(account)
  return _token_response(access, expires, refresh, ACCESS_SCOPE)


async def revoke_refresh_tokens(public_id: str) -> None:
  await OAuthRefreshToken.get_motor_collection().update_many(
    {"public_account_id": public_id, "revoked": False},
    {"$set": {"revoked": True}},
  )


async def replace_client_secret(account: VirtualAccount, secret: str) -> None:
  account.management_credential_hash = hash_management_credential(secret)
  account.management_credential_hint = hint(secret)
  account.credential_version = int(account.credential_version or 1) + 1
  account.updated_at = utc_now()
  await account.save()
  await revoke_refresh_tokens(account.public_account_id or account.app_name)
