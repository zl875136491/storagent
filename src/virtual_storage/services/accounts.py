"""虚拟账号与专用池。池记录不会写入旧的 minio_server 集合。"""
from __future__ import annotations

from src.core.crypto import encrypt_secret
from src.virtual_storage.core.database import transaction
from src.virtual_storage.core.exception import AccountDisabledError, AccountNotFoundError, InvalidCredentialError
from src.virtual_storage.core.security import hash_management_credential, hint, verify_management_credential
from src.virtual_storage.models.documents import BucketPool, VirtualAccount
from src.virtual_storage.services.support import new_id, stamp, write_audit
from src.virtual_storage.settings import get_settings


def account_view(account: VirtualAccount) -> dict:
  return {
    "account_id": account.account_id,
    "app_name": account.app_name,
    "public_account_id": account.public_account_id or account.app_name,
    "credential_version": int(account.credential_version or 1),
    "display_name": account.display_name,
    "home_region": account.home_region,
    "pool_id": account.pool_id,
    "enabled": account.enabled,
    "management_credential_hint": account.management_credential_hint,
    "quota_bytes_total": account.quota_bytes_total,
    "quota_bytes_allocated": account.quota_bytes_allocated,
    "quota_bytes_used": account.quota_bytes_used,
    "quota_bytes_reserved": account.quota_bytes_reserved,
    "quota_bytes_trashed": account.quota_bytes_trashed,
    "quota_objects_total": account.quota_objects_total,
    "quota_objects_allocated": account.quota_objects_allocated,
    "quota_objects_used": account.quota_objects_used,
    "quota_objects_reserved": account.quota_objects_reserved,
    "quota_objects_trashed": account.quota_objects_trashed,
    "revision": account.revision,
  }


async def create_account(
  *,
  app_name: str,
  display_name: str,
  credential: str,
  quota_bytes: int,
  quota_objects: int,
  home_region: str | None = None,
) -> VirtualAccount:
  existing = await VirtualAccount.find_one(VirtualAccount.app_name == app_name)
  if existing:
    if not existing.public_account_id:
      existing.public_account_id = existing.app_name
      existing.updated_at = stamp()
      await existing.save()
    return existing
  settings = get_settings()
  region = home_region or settings.home_region
  pool_id = f"vs-pool-{region}"
  async with transaction() as session:
    pool = await BucketPool.find_one(BucketPool.pool_id == pool_id, session=session)
    if pool is None:
      pool = BucketPool(
        pool_id=pool_id,
        endpoint=settings.minio_endpoint,
        access_key_enc=encrypt_secret(settings.minio_access_key) if settings.minio_access_key else "",
        secret_key_enc=encrypt_secret(settings.minio_secret_key) if settings.minio_secret_key else "",
        secure=settings.minio_secure,
        region=region,
        bucket=settings.minio_bucket,
        registered_in_legacy_scan=False,
      )
      await pool.insert(session=session)
    account = VirtualAccount(
      account_id=new_id("acct"),
      app_name=app_name,
      public_account_id=app_name,
      credential_version=1,
      display_name=display_name,
      home_region=region,
      pool_id=pool.pool_id,
      management_credential_hash=hash_management_credential(credential),
      management_credential_hint=hint(credential),
      quota_bytes_total=quota_bytes,
      quota_objects_total=quota_objects,
      created_at=stamp(),
      updated_at=stamp(),
    )
    await account.insert(session=session)
    await write_audit(
      account_id=account.account_id,
      tenant_id="",
      actor_type="system",
      actor_id="bootstrap",
      action="account.create",
      resource_id=account.account_id,
      detail={"app_name": app_name, "home_region": region, "pool_id": pool.pool_id},
      session=session,
    )
  return account


async def verify_client_secret(app_name: str, credential: str) -> VirtualAccount:
  """只给换票和旧控制台会话使用。管理接口不接受这段明文。"""
  account = await find_account(app_name)
  if not account.enabled:
    raise AccountDisabledError(app_name)
  if not verify_management_credential(credential, account.management_credential_hash):
    raise InvalidCredentialError("管理凭据无效")
  return account


async def authenticate_management(app_name: str, credential: str) -> VirtualAccount:
  from src.virtual_storage.services.machine_auth import authenticate_access_token
  return await authenticate_access_token(app_name, credential)


async def find_account(name: str) -> VirtualAccount:
  account = await VirtualAccount.find_one(VirtualAccount.public_account_id == name)
  if account is None:
    account = await VirtualAccount.find_one(VirtualAccount.app_name == name)
  if account is None:
    raise AccountNotFoundError(name)
  return account


async def get_account(app_name: str) -> VirtualAccount:
  return await find_account(app_name)
