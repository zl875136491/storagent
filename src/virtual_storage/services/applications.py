"""虚拟账号申请。入口先占名，审核不负责发现重名。"""
from __future__ import annotations

import re
import secrets
from datetime import timedelta

from pymongo.errors import DuplicateKeyError

from src.core.crypto import encrypt_secret
from src.utils.helpers import utc_now
from src.virtual_storage.core.database import run_transaction
from src.virtual_storage.core.exception import ConflictError, ConsoleForbiddenError, NotReadyError, PreconditionError, VirtualStorageError
from src.virtual_storage.core.security import decrypt_delivery, encrypt_delivery, hash_management_credential, hint, request_fingerprint
from src.virtual_storage.models.documents import (
  BucketPool,
  KeyDelivery,
  VirtualAccount,
  VirtualAccountApplication,
  VirtualAccountGrant,
)
from src.virtual_storage.services.accounts import find_account
from src.virtual_storage.services.machine_auth import replace_client_secret
from src.virtual_storage.services.support import new_id, stamp, write_audit
from src.virtual_storage.settings import get_settings

PUBLIC_ID = re.compile(r"^[a-z0-9](?:[a-z0-9-]{2,30})[a-z0-9]$")
RESERVED_IDS = {"healthz", "ready", "oauth2", "internal", "console", "api"}
HOLDING_INIT = ("pending", "initializing", "active", "failed")


def validate_public_id(value: str) -> str:
  public_id = (value or "").strip().lower()
  if not PUBLIC_ID.fullmatch(public_id) or not 4 <= len(public_id) <= 32:
    raise VirtualStorageError(
      "account id 需要 4 到 32 位，只能使用小写字母、数字和连字符，并且不能以连字符开头或结尾",
      "INVALID_ACCOUNT_ID",
      400,
    )
  if public_id in RESERVED_IDS:
    raise VirtualStorageError("该 account id 不能使用", "RESERVED_ACCOUNT_ID", 400)
  return public_id


def _subject(user_id: str, username: str) -> str:
  name = (username or "").strip().lower()
  return name or (user_id or "").strip()


def application_view(item: VirtualAccountApplication) -> dict:
  return {
    "application_id": item.application_id,
    "public_account_id": item.public_account_id,
    "purpose": item.purpose,
    "requested_quota_bytes": item.requested_quota_bytes,
    "requested_quota_objects": item.requested_quota_objects,
    "approved_quota_bytes": item.approved_quota_bytes,
    "approved_quota_objects": item.approved_quota_objects,
    "status": item.status,
    "init_status": item.init_status,
    "owner_user_id": item.owner_user_id,
    "owner_username": item.owner_username,
    "revision": item.revision,
    "reject_reason": item.reject_reason,
    "account_id": item.account_id,
    "init_error": item.init_error,
    "created_at": item.created_at.isoformat(),
    "updated_at": item.updated_at.isoformat(),
  }


def _require_revision(item: VirtualAccountApplication, if_match: str) -> None:
  if str(if_match or "").strip() != str(item.revision):
    raise PreconditionError("申请已被他人更新")


async def _holding(public_id: str) -> VirtualAccountApplication | None:
  return await VirtualAccountApplication.find_one(
    {
      "public_account_id": public_id,
      "$or": [
        {"status": "submitted"},
        {"status": "approved", "init_status": {"$in": list(HOLDING_INIT)}},
      ],
    }
  )


async def _name_taken(public_id: str) -> bool:
  account = await VirtualAccount.find_one(
    {"$or": [{"public_account_id": public_id}, {"app_name": public_id}]}
  )
  if account is not None:
    return True
  return await _holding(public_id) is not None


def _fingerprint(public_id: str, purpose: str, quota_bytes: int, quota_objects: int) -> str:
  return request_fingerprint({
    "public_account_id": public_id,
    "purpose": purpose,
    "requested_quota_bytes": quota_bytes,
    "requested_quota_objects": quota_objects,
  })


async def create_application(
  *,
  user_id: str,
  username: str,
  public_account_id: str,
  purpose: str,
  quota_bytes: int,
  quota_objects: int,
  idempotency_key: str,
) -> dict:
  if quota_bytes < 0 or quota_objects < 0:
    raise VirtualStorageError("配额不能为负数", "INVALID_QUOTA", 400)
  public_id = validate_public_id(public_account_id)
  subject = _subject(user_id, username)
  fingerprint = _fingerprint(public_id, purpose.strip(), int(quota_bytes), int(quota_objects))
  if idempotency_key:
    existing = await VirtualAccountApplication.find_one({
      "owner_subject": subject,
      "idempotency_key": idempotency_key,
    })
    if existing is not None:
      if existing.request_fingerprint != fingerprint:
        raise ConflictError("同一个 Idempotency-Key 对应了不同的申请内容", "IDEMPOTENCY_CONFLICT")
      return application_view(existing)
  own = await VirtualAccountApplication.find_one({
    "owner_subject": subject,
    "public_account_id": public_id,
    "status": "submitted",
  })
  if own is not None:
    return application_view(own)
  if await _name_taken(public_id):
    raise ConflictError("account id 已被占用", "ACCOUNT_ID_TAKEN")
  item = VirtualAccountApplication(
    application_id=new_id("vsa"),
    public_account_id=public_id,
    purpose=purpose.strip(),
    requested_quota_bytes=int(quota_bytes),
    requested_quota_objects=int(quota_objects),
    status="submitted",
    owner_user_id=user_id,
    owner_username=(username or "").strip(),
    owner_subject=subject,
    revision=1,
    idempotency_key=idempotency_key or "",
    request_fingerprint=fingerprint,
    created_at=stamp(),
    updated_at=stamp(),
  )
  try:
    await item.insert()
  except DuplicateKeyError as exc:
    raise ConflictError("account id 已被占用", "ACCOUNT_ID_TAKEN") from exc
  return application_view(item)


async def _visible(application_id: str, user_id: str, username: str, reviewer: bool) -> VirtualAccountApplication:
  item = await VirtualAccountApplication.find_one(VirtualAccountApplication.application_id == application_id)
  if item is None:
    raise VirtualStorageError("申请不存在", "APPLICATION_NOT_FOUND", 404)
  subject = _subject(user_id, username)
  if reviewer or item.owner_subject == subject or item.owner_user_id == user_id:
    return item
  raise VirtualStorageError("申请不存在", "APPLICATION_NOT_FOUND", 404)


async def list_applications(
  *,
  user_id: str,
  username: str,
  reviewer: bool,
  mine: bool,
  page: int,
  page_size: int,
) -> dict:
  page = max(page, 1)
  page_size = min(max(page_size, 1), 100)
  query: dict = {}
  if mine or not reviewer:
    subject = _subject(user_id, username)
    query = {"$or": [{"owner_subject": subject}, {"owner_user_id": user_id}]}
  else:
    query = {"status": "submitted"}
  total = await VirtualAccountApplication.find(query).count()
  rows = await VirtualAccountApplication.find(query).sort("-created_at").skip((page - 1) * page_size).limit(page_size).to_list()
  return {"items": [application_view(row) for row in rows], "page": page, "page_size": page_size, "total": total}


async def get_application(application_id: str, user_id: str, username: str, reviewer: bool) -> dict:
  return application_view(await _visible(application_id, user_id, username, reviewer))


async def withdraw_application(application_id: str, user_id: str, username: str, if_match: str) -> dict:
  item = await _visible(application_id, user_id, username, False)
  subject = _subject(user_id, username)
  if item.owner_subject != subject and item.owner_user_id != user_id:
    raise VirtualStorageError("申请不存在", "APPLICATION_NOT_FOUND", 404)
  _require_revision(item, if_match)
  if item.status != "submitted":
    raise ConflictError("这张申请已经结束，不能再次提交或撤回", "APPLICATION_TERMINAL")
  item.status = "withdrawn"
  item.revision += 1
  item.updated_at = stamp()
  await item.save()
  return application_view(item)


async def approve_application(
  *,
  application_id: str,
  if_match: str,
  quota_bytes: int,
  quota_objects: int,
  actor_id: str,
) -> dict:
  if quota_bytes < 0 or quota_objects < 0:
    raise VirtualStorageError("配额不能为负数", "INVALID_QUOTA", 400)
  item = await VirtualAccountApplication.find_one(VirtualAccountApplication.application_id == application_id)
  if item is None:
    raise VirtualStorageError("申请不存在", "APPLICATION_NOT_FOUND", 404)
  _require_revision(item, if_match)
  if item.status != "submitted":
    raise ConflictError("这张申请已经结束", "APPLICATION_TERMINAL")
  item.status = "approved"
  item.init_status = "pending"
  item.approved_quota_bytes = int(quota_bytes)
  item.approved_quota_objects = int(quota_objects)
  item.revision += 1
  item.updated_at = stamp()
  await item.save()
  await write_audit(
    account_id="",
    tenant_id="",
    actor_type="console",
    actor_id=actor_id,
    action="application.approve",
    resource_id=item.application_id,
    detail={
      "public_account_id": item.public_account_id,
      "self_review": actor_id == item.owner_subject or actor_id == item.owner_user_id,
    },
  )
  return application_view(item)


async def reject_application(application_id: str, if_match: str, reason: str) -> dict:
  text = (reason or "").strip()
  if not text:
    raise VirtualStorageError("拒绝时需要填写原因", "REASON_REQUIRED", 400)
  item = await VirtualAccountApplication.find_one(VirtualAccountApplication.application_id == application_id)
  if item is None:
    raise VirtualStorageError("申请不存在", "APPLICATION_NOT_FOUND", 404)
  _require_revision(item, if_match)
  if item.status != "submitted":
    raise ConflictError("这张申请已经结束", "APPLICATION_TERMINAL")
  item.status = "rejected"
  item.reject_reason = text
  item.revision += 1
  item.updated_at = stamp()
  await item.save()
  return application_view(item)


async def _open_delivery(account: VirtualAccount, secret: str, session=None) -> None:
  settings = get_settings()
  now = stamp()
  delivery = KeyDelivery(
    delivery_id=new_id("delivery"),
    account_id=account.account_id,
    tenant_id="",
    key_id="client-secret",
    subject_hash=f"client-secret:{account.credential_version}",
    idempotency_key=f"v{account.credential_version}",
    request_fingerprint=str(account.credential_version),
    encrypted_key=encrypt_delivery(secret),
    window_starts_at=now,
    window_ends_at=now + timedelta(seconds=settings.delivery_window_seconds),
    created_at=now,
  )
  await delivery.insert(session=session)


async def _materialize(item: VirtualAccountApplication, secret: str, session) -> None:
  current = await VirtualAccountApplication.find_one(
    VirtualAccountApplication.application_id == item.application_id,
    session=session,
  )
  if current is None or current.status != "approved":
    return
  if current.init_status == "active":
    return
  settings = get_settings()
  public_id = current.public_account_id
  account = await VirtualAccount.find_one(VirtualAccount.public_account_id == public_id, session=session)
  if account is None:
    pool_id = f"vs-pool-{settings.home_region}"
    pool = await BucketPool.find_one(BucketPool.pool_id == pool_id, session=session)
    if pool is None:
      pool = BucketPool(
        pool_id=pool_id,
        endpoint=settings.minio_endpoint,
        access_key_enc=encrypt_secret(settings.minio_access_key) if settings.minio_access_key else "",
        secret_key_enc=encrypt_secret(settings.minio_secret_key) if settings.minio_secret_key else "",
        secure=settings.minio_secure,
        region=settings.home_region,
        bucket=settings.minio_bucket,
        registered_in_legacy_scan=False,
      )
      await pool.insert(session=session)
    account = VirtualAccount(
      account_id=new_id("acct"),
      app_name=public_id,
      public_account_id=public_id,
      credential_version=1,
      display_name=public_id,
      home_region=settings.home_region,
      pool_id=pool.pool_id,
      management_credential_hash=hash_management_credential(secret),
      management_credential_hint=hint(secret),
      quota_bytes_total=current.approved_quota_bytes,
      quota_objects_total=current.approved_quota_objects,
      created_at=stamp(),
      updated_at=stamp(),
    )
    await account.insert(session=session)
    if current.owner_user_id:
      await VirtualAccountGrant(
        grant_id=new_id("grant"),
        account_id=account.account_id,
        user_id=current.owner_user_id,
        username=current.owner_username,
        created_at=stamp(),
      ).insert(session=session)
    await _open_delivery(account, secret, session)
  current.account_id = account.account_id
  current.init_status = "active"
  current.init_error = ""
  current.revision += 1
  current.updated_at = stamp()
  await current.save(session=session)


async def advance_application(application_id: str) -> dict:
  item = await VirtualAccountApplication.find_one(VirtualAccountApplication.application_id == application_id)
  if item is None:
    raise VirtualStorageError("申请不存在", "APPLICATION_NOT_FOUND", 404)
  if item.status != "approved":
    raise ConflictError("只有已批准的申请可以初始化", "APPLICATION_NOT_APPROVED")
  if item.init_status == "active":
    return application_view(item)
  if item.init_status not in {"", "pending", "failed", "initializing"}:
    raise ConflictError("当前不能初始化", "INIT_NOT_ALLOWED")
  item.init_status = "initializing"
  item.init_attempt = item.init_attempt or new_id("init")
  item.updated_at = stamp()
  await item.save()
  secret = secrets.token_urlsafe(32)
  try:
    await run_transaction(lambda session: _materialize(item, secret, session))
  except Exception:
    fresh = await VirtualAccountApplication.find_one(VirtualAccountApplication.application_id == application_id)
    if fresh is not None and fresh.init_status != "active":
      fresh.init_status = "failed"
      fresh.init_error = "INIT_FAILED"
      fresh.revision += 1
      fresh.updated_at = stamp()
      await fresh.save()
      return application_view(fresh)
  fresh = await VirtualAccountApplication.find_one(VirtualAccountApplication.application_id == application_id)
  return application_view(fresh)


async def advance_pending() -> int:
  rows = await VirtualAccountApplication.find(
    {"status": "approved", "init_status": {"$in": ["pending", "initializing"]}}
  ).to_list()
  for row in rows:
    await advance_application(row.application_id)
  return len(rows)


async def retry_init(application_id: str, user_id: str, username: str, is_admin: bool) -> dict:
  item = await _visible(application_id, user_id, username, is_admin)
  subject = _subject(user_id, username)
  if not is_admin and item.owner_subject != subject and item.owner_user_id != user_id:
    raise ConsoleForbiddenError("不能重试别人的初始化")
  if item.init_status not in {"failed", "initializing"}:
    raise ConflictError("只有初始化失败后可以重试", "INIT_NOT_FAILED")
  item.init_status = "pending"
  item.revision += 1
  item.updated_at = stamp()
  await item.save()
  return await advance_application(application_id)


def _owns(item: VirtualAccountApplication, user_id: str, username: str, is_admin: bool) -> bool:
  if is_admin:
    return True
  subject = _subject(user_id, username)
  return item.owner_subject == subject or item.owner_user_id == user_id


async def _application_for_account(account: VirtualAccount) -> VirtualAccountApplication | None:
  if account.account_id:
    found = await VirtualAccountApplication.find_one(VirtualAccountApplication.account_id == account.account_id)
    if found is not None:
      return found
  return await VirtualAccountApplication.find_one(
    VirtualAccountApplication.public_account_id == (account.public_account_id or account.app_name)
  )


async def deliver_secret(public_id: str, user_id: str, username: str, is_admin: bool) -> dict:
  account = await find_account(public_id)
  item = await _application_for_account(account)
  if item is None or not _owns(item, user_id, username, is_admin):
    raise ConsoleForbiddenError("不能领取该账号的密钥")
  if item.init_status != "active":
    raise NotReadyError("账号尚未初始化完成")
  delivery = await KeyDelivery.find_one({
    "account_id": account.account_id,
    "key_id": "client-secret",
    "idempotency_key": f"v{account.credential_version}",
  })
  if delivery is None:
    raise NotReadyError("密钥交付窗口不存在")
  now = utc_now()
  ends = delivery.window_ends_at
  if ends.tzinfo is None:
    from datetime import timezone
    ends = ends.replace(tzinfo=timezone.utc)
  if ends <= now:
    raise NotReadyError("密钥交付窗口已结束，请在管理页重新生成")
  return {
    "public_account_id": account.public_account_id or account.app_name,
    "client_secret": decrypt_delivery(delivery.encrypted_key),
    "credential_version": account.credential_version,
  }


async def rotate_secret(public_id: str, user_id: str, username: str, is_admin: bool, if_match: str) -> dict:
  account = await find_account(public_id)
  item = await _application_for_account(account)
  if item is not None and not _owns(item, user_id, username, is_admin):
    raise ConsoleForbiddenError("不能重新生成该账号的密钥")
  if item is None and not is_admin:
    raise ConsoleForbiddenError("不能重新生成该账号的密钥")
  if str(if_match or "").strip() != str(account.revision):
    raise PreconditionError("账号已被他人更新")
  secret = secrets.token_urlsafe(32)
  await replace_client_secret(account, secret)
  account.revision += 1
  await account.save()
  await _open_delivery(account, secret)
  await write_audit(
    account_id=account.account_id,
    tenant_id="",
    actor_type="console",
    actor_id=_subject(user_id, username),
    action="account.rotate_secret",
    resource_id=account.account_id,
    detail={"credential_version": account.credential_version},
  )
  return {
    "public_account_id": account.public_account_id or account.app_name,
    "client_secret": secret,
    "credential_version": account.credential_version,
    "revision": account.revision,
  }
