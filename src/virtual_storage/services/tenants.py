"""租户、代际和管理凭据与租户 Key 的隔离交付。"""
from __future__ import annotations

from datetime import timedelta

from pymongo.errors import DuplicateKeyError

from src.utils.helpers import utc_now
from src.virtual_storage.core.database import transaction
from src.virtual_storage.core.exception import (
  ConflictError,
  InvalidCredentialError,
  PreconditionError,
  QuotaExceededError,
  TenantFrozenError,
  TenantNotFoundError,
  TenantRetiredError,
)
from src.virtual_storage.core.security import (
  decrypt_delivery,
  encrypt_delivery,
  hash_tenant_key,
  hint,
  request_fingerprint,
  subject_hash,
)
from src.virtual_storage.models.documents import ACTIVE_TENANT_STATES, KeyDelivery, Tenant, TenantAPIKey
from src.virtual_storage.services.accounts import authenticate_management
from src.virtual_storage.services.quota import allocate_tenant_quota
from src.virtual_storage.services.support import as_utc, new_id, stamp, tenant_key_secret, write_audit
from src.virtual_storage.settings import get_settings


def tenant_view(tenant: Tenant) -> dict:
  return {
    "tenant_id": tenant.tenant_id,
    "account_id": tenant.account_id,
    "external_tenant_id": tenant.external_tenant_id,
    "logical_name": tenant.logical_name,
    "generation": tenant.generation,
    "state": tenant.state,
    "purge_started": tenant.purge_started,
    "quota_bytes_total": tenant.quota_bytes_total,
    "quota_bytes_used": tenant.quota_bytes_used,
    "quota_bytes_reserved": tenant.quota_bytes_reserved,
    "quota_bytes_trashed": tenant.quota_bytes_trashed,
    "quota_objects_total": tenant.quota_objects_total,
    "quota_objects_used": tenant.quota_objects_used,
    "quota_objects_reserved": tenant.quota_objects_reserved,
    "quota_objects_trashed": tenant.quota_objects_trashed,
    "revision": tenant.revision,
  }


def _fingerprint(payload: dict) -> str:
  body = {
    "external_tenant_id": payload["external_tenant_id"],
    "logical_name": payload["logical_name"],
    "quota_bytes": int(payload["quota_bytes"]),
    "quota_objects": int(payload["quota_objects"]),
    "key_ttl_seconds": int(payload["key_ttl_seconds"]),
  }
  if payload.get("credential_sha256"):
    body["credential_sha256"] = payload["credential_sha256"]
  return request_fingerprint(body)


async def _find_delivery(account_id: str, subject: str, idempotency_key: str, session=None):
  # 直接按三个字段查询。Beanie 的多表达式 find_one 会丢掉后面的幂等键条件。
  document = await KeyDelivery.get_motor_collection().find_one(
    {"account_id": account_id, "subject_hash": subject, "idempotency_key": idempotency_key},
    session=session,
  )
  if document is None:
    return None
  return await KeyDelivery.find_one(KeyDelivery.delivery_id == document["delivery_id"])


def _custom_tenant_credential(value: str | None) -> str:
  supplied = (value or "").strip()
  if not supplied:
    return ""
  if len(supplied) < 20 or len(supplied) > 128 or any(character.isspace() for character in supplied):
    raise ConflictError("租户凭证需为 20 到 128 位且不能包含空白", "CREDENTIAL_FORMAT")
  return supplied


async def _replay(delivery: KeyDelivery, fingerprint: str) -> dict:
  if delivery.request_fingerprint != fingerprint:
    raise ConflictError("同一幂等键的参数不一致", "IDEMPOTENCY_MISMATCH")
  tenant = await Tenant.find_one(Tenant.tenant_id == delivery.tenant_id)
  api_key = await TenantAPIKey.find_one(TenantAPIKey.key_id == delivery.key_id)
  body = {
    "tenant_id": delivery.tenant_id,
    "generation": tenant.generation if tenant else None,
    "key_id": delivery.key_id,
    "key_hint": api_key.key_hint if api_key else "",
    "key_expires_at": api_key.expires_at.isoformat() if api_key else None,
    "delivery_expires_at": delivery.window_ends_at.isoformat(),
    "logical_name": tenant.logical_name if tenant else "",
    "external_tenant_id": tenant.external_tenant_id if tenant else "",
  }
  if utc_now() <= as_utc(delivery.window_ends_at):
    body["api_key"] = decrypt_delivery(delivery.encrypted_key)
    body["plaintext_available"] = True
    body["delivery_expired"] = False
  else:
    body["plaintext_available"] = False
    body["delivery_expired"] = True
  return body


async def create_tenant(
  *,
  app_name: str,
  credential: str,
  idempotency_key: str,
  external_tenant_id: str,
  logical_name: str,
  quota_bytes: int,
  quota_objects: int,
  key_ttl_seconds: int | None = None,
  tenant_credential: str | None = None,
  account=None,
  actor_subject: str = "",
) -> dict:
  if quota_bytes < 0 or quota_objects < 0:
    raise ConflictError("配额不能为负")
  if account is None:
    account = await authenticate_management(app_name, credential)
    subject = subject_hash(credential)
  else:
    subject = actor_subject or subject_hash(f"console:{account.account_id}")
  settings = get_settings()
  ttl = key_ttl_seconds or settings.key_ttl_seconds
  supplied = _custom_tenant_credential(tenant_credential)
  payload = {
    "external_tenant_id": external_tenant_id,
    "logical_name": logical_name,
    "quota_bytes": quota_bytes,
    "quota_objects": quota_objects,
    "key_ttl_seconds": ttl,
  }
  if supplied:
    payload["credential_sha256"] = hash_tenant_key(supplied)
  fingerprint = _fingerprint(payload)
  existing = await _find_delivery(account.account_id, subject, idempotency_key)
  if existing:
    return await _replay(existing, fingerprint)

  raw_key = supplied or tenant_key_secret()
  tenant_id = new_id("tn")
  key_id = new_id("key")
  now = stamp()
  try:
    async with transaction() as session:
      again = await _find_delivery(account.account_id, subject, idempotency_key, session)
      if again:
        raw_key = ""
      else:
        previous_rows = await Tenant.find(
          Tenant.account_id == account.account_id,
          Tenant.external_tenant_id == external_tenant_id,
          session=session,
        ).sort(-Tenant.generation).limit(1).to_list()
        previous = previous_rows[0] if previous_rows else None
        generation = (previous.generation if previous else 0) + 1
        if previous and previous.state in ACTIVE_TENANT_STATES:
          raise ConflictError("外部租户标识仍被未注销租户占用", "TENANT_ACTIVE")
        await allocate_tenant_quota(
          account_id=account.account_id,
          tenant_id=tenant_id,
          quota_bytes=quota_bytes,
          quota_objects=quota_objects,
          session=session,
        )
        tenant = Tenant(
          tenant_id=tenant_id,
          account_id=account.account_id,
          external_tenant_id=external_tenant_id,
          logical_name=logical_name,
          generation=generation,
          state="active",
          quota_bytes_total=quota_bytes,
          quota_objects_total=quota_objects,
          created_at=now,
          updated_at=now,
        )
        await tenant.insert(session=session)
        await TenantAPIKey(
          key_id=key_id,
          tenant_id=tenant_id,
          account_id=account.account_id,
          purpose="tenant_data",
          key_hash=hash_tenant_key(raw_key),
          key_hint=hint(raw_key),
          expires_at=now + timedelta(seconds=ttl),
          created_at=now,
        ).insert(session=session)
        await KeyDelivery(
          delivery_id=new_id("dlv"),
          account_id=account.account_id,
          tenant_id=tenant_id,
          key_id=key_id,
          subject_hash=subject,
          idempotency_key=idempotency_key,
          request_fingerprint=fingerprint,
          encrypted_key=encrypt_delivery(raw_key),
          window_starts_at=now,
          window_ends_at=now + timedelta(seconds=settings.delivery_window_seconds),
          created_at=now,
        ).insert(session=session)
        await write_audit(
          account_id=account.account_id,
          tenant_id=tenant_id,
          actor_type="management",
          actor_id=account.management_credential_hint,
          action="tenant.create",
          resource_id=tenant_id,
          detail={"generation": generation, "key_id": key_id, "logical_name": logical_name},
          session=session,
        )
  except DuplicateKeyError as exc:
    existing = await _find_delivery(account.account_id, subject, idempotency_key)
    if existing:
      return await _replay(existing, fingerprint)
    raise ConflictError("租户逻辑名或外部标识冲突") from exc
  except QuotaExceededError:
    raise

  if raw_key == "":
    existing = await _find_delivery(account.account_id, subject, idempotency_key)
    return await _replay(existing, fingerprint)
  delivery = await KeyDelivery.find_one(KeyDelivery.key_id == key_id)
  return await _replay(delivery, fingerprint)


async def list_tenants(app_name: str, credential: str, account=None) -> list[dict]:
  if account is None:
    account = await authenticate_management(app_name, credential)
  tenants = await Tenant.find(Tenant.account_id == account.account_id).to_list()
  return [tenant_view(tenant) for tenant in tenants]


async def get_tenant(app_name: str, credential: str, tenant_id: str) -> dict:
  account = await authenticate_management(app_name, credential)
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id, Tenant.account_id == account.account_id)
  if tenant is None:
    raise TenantNotFoundError(tenant_id)
  return tenant_view(tenant)


async def authenticate_tenant_key(tenant_id: str, raw_key: str) -> Tenant:
  matched = await TenantAPIKey.find_one(
    TenantAPIKey.key_hash == hash_tenant_key(raw_key),
    TenantAPIKey.enabled == True,
  )
  if matched is None or matched.purpose != "tenant_data":
    raise InvalidCredentialError("租户 Key 无效")
  if matched.tenant_id != tenant_id:
    raise InvalidCredentialError("租户 Key 与路径不一致")
  if as_utc(matched.expires_at) <= utc_now():
    raise InvalidCredentialError("租户 Key 已过有效期")
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  if tenant is None:
    raise TenantNotFoundError(tenant_id)
  if tenant.state == "frozen":
    raise TenantFrozenError(tenant_id)
  if tenant.state == "retired":
    raise TenantRetiredError(tenant_id)
  if tenant.state != "active":
    raise TenantFrozenError(tenant_id)
  return tenant


async def issue_tenant_key(
  *,
  app_name: str,
  credential: str,
  tenant_id: str,
  idempotency_key: str,
  key_ttl_seconds: int | None = None,
  account=None,
  actor_subject: str = "",
) -> dict:
  if account is None:
    account = await authenticate_management(app_name, credential)
    subject = subject_hash(credential)
  else:
    subject = actor_subject or subject_hash(f"console:{account.account_id}")
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id, Tenant.account_id == account.account_id)
  if tenant is None:
    raise TenantNotFoundError(tenant_id)
  if tenant.state != "active":
    raise ConflictError("只有活跃租户可以签发 Key")
  settings = get_settings()
  ttl = key_ttl_seconds or settings.key_ttl_seconds
  fingerprint = request_fingerprint({"tenant_id": tenant_id, "key_ttl_seconds": ttl, "op": "issue"})
  existing = await _find_delivery(account.account_id, subject, idempotency_key)
  if existing:
    return await _replay(existing, fingerprint)
  raw_key = tenant_key_secret()
  key_id = new_id("key")
  now = stamp()
  async with transaction() as session:
    await TenantAPIKey(
      key_id=key_id,
      tenant_id=tenant_id,
      account_id=account.account_id,
      purpose="tenant_data",
      key_hash=hash_tenant_key(raw_key),
      key_hint=hint(raw_key),
      expires_at=now + timedelta(seconds=ttl),
      created_at=now,
    ).insert(session=session)
    await KeyDelivery(
      delivery_id=new_id("dlv"),
      account_id=account.account_id,
      tenant_id=tenant_id,
      key_id=key_id,
      subject_hash=subject,
      idempotency_key=idempotency_key,
      request_fingerprint=fingerprint,
      encrypted_key=encrypt_delivery(raw_key),
      window_starts_at=now,
      window_ends_at=now + timedelta(seconds=settings.delivery_window_seconds),
      created_at=now,
    ).insert(session=session)
    await write_audit(
      account_id=account.account_id, tenant_id=tenant_id, actor_type="management",
      actor_id=account.management_credential_hint, action="tenant.key.issue",
      resource_id=key_id, session=session,
    )
  delivery = await KeyDelivery.find_one(KeyDelivery.key_id == key_id)
  return await _replay(delivery, fingerprint)


async def list_tenant_keys(*, app_name: str, credential: str, tenant_id: str, account=None) -> list[dict]:
  if account is None:
    account = await authenticate_management(app_name, credential)
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id, Tenant.account_id == account.account_id)
  if tenant is None:
    raise TenantNotFoundError(tenant_id)
  keys = await TenantAPIKey.find(TenantAPIKey.tenant_id == tenant_id).to_list()
  return [
    {
      "key_id": item.key_id,
      "key_hint": item.key_hint,
      "purpose": item.purpose,
      "enabled": item.enabled,
      "expires_at": item.expires_at.isoformat(),
    }
    for item in keys
  ]


async def revoke_tenant_key(*, app_name: str, credential: str, tenant_id: str, key_id: str, account=None) -> dict:
  if account is None:
    account = await authenticate_management(app_name, credential)
  matched = await TenantAPIKey.find_one(TenantAPIKey.key_id == key_id, TenantAPIKey.account_id == account.account_id)
  if matched is None or matched.tenant_id != tenant_id:
    raise TenantNotFoundError(key_id)
  if matched.enabled:
    matched.enabled = False
    matched.revoked_at = stamp()
    await matched.save()
    await write_audit(
      account_id=account.account_id, tenant_id=tenant_id, actor_type="management",
      actor_id=account.management_credential_hint, action="tenant.key.revoke", resource_id=key_id,
    )
  return {"key_id": key_id, "enabled": False}


async def adjust_quota(
  *,
  app_name: str,
  credential: str,
  tenant_id: str,
  if_match: str,
  quota_bytes: int,
  quota_objects: int,
  account=None,
) -> dict:
  if account is None:
    account = await authenticate_management(app_name, credential)
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id, Tenant.account_id == account.account_id)
  if tenant is None:
    raise TenantNotFoundError(tenant_id)
  if str(tenant.revision) != if_match:
    raise PreconditionError("租户版本已变化")
  if tenant.state != "active":
    raise ConflictError("只有活跃租户可以调整配额")
  occupied_bytes = tenant.quota_bytes_used + tenant.quota_bytes_reserved + tenant.quota_bytes_trashed
  occupied_objects = tenant.quota_objects_used + tenant.quota_objects_reserved + tenant.quota_objects_trashed
  if quota_bytes < occupied_bytes or quota_objects < occupied_objects:
    raise QuotaExceededError("新配额小于已占用额度，软删除仍计入占用")
  delta_bytes = quota_bytes - tenant.quota_bytes_total
  delta_objects = quota_objects - tenant.quota_objects_total
  from src.virtual_storage.models.documents import VirtualAccount
  async with transaction() as session:
    account_result = await VirtualAccount.get_motor_collection().update_one(
      {
        "account_id": account.account_id,
        "quota_bytes_allocated": {"$gte": -delta_bytes if delta_bytes < 0 else 0},
        "$expr": {
          "$and": [
            {"$lte": [{"$add": ["$quota_bytes_allocated", delta_bytes]}, "$quota_bytes_total"]},
            {"$lte": [{"$add": ["$quota_objects_allocated", delta_objects]}, "$quota_objects_total"]},
            {"$gte": [{"$add": ["$quota_bytes_allocated", delta_bytes]}, 0]},
            {"$gte": [{"$add": ["$quota_objects_allocated", delta_objects]}, 0]},
          ]
        },
      },
      {"$inc": {
        "quota_bytes_allocated": delta_bytes,
        "quota_objects_allocated": delta_objects,
        "revision": 1,
      }, "$set": {"updated_at": stamp()}},
      session=session,
    )
    if account_result.modified_count != 1:
      raise QuotaExceededError("账号可分配额度不足")
    tenant_result = await Tenant.get_motor_collection().update_one(
      {"tenant_id": tenant_id, "revision": tenant.revision, "state": "active"},
      {"$set": {
        "quota_bytes_total": quota_bytes,
        "quota_objects_total": quota_objects,
        "updated_at": stamp(),
      }, "$inc": {"revision": 1}},
      session=session,
    )
    if tenant_result.modified_count != 1:
      raise PreconditionError("租户版本已变化")
  refreshed = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  return tenant_view(refreshed)
