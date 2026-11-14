"""虚拟存储文档。关系使用字符串 id，便于跨文档事务条件更新。"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from beanie import Document
from pydantic import Field
from pymongo import IndexModel

from src.utils.helpers import utc_now

OPEN_SESSION_STATES = (
  "initializing",
  "uploading",
  "completing",
  "aborting",
  "reconciling",
)
OCCUPYING_OBJECT_STATES = ("active", "soft_deleted", "purge_pending")
ACTIVE_TENANT_STATES = ("active", "frozen", "retiring", "purge_pending")


class BucketPool(Document):
  pool_id: str
  endpoint: str
  access_key_enc: str = Field(default="")
  secret_key_enc: str = Field(default="")
  secure: bool = Field(default=False)
  region: str
  bucket: str
  registered_in_legacy_scan: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_bucket_pools"
    indexes = [IndexModel(["pool_id"], unique=True)]


class VirtualAccount(Document):
  account_id: str
  app_name: str
  display_name: str
  home_region: str
  pool_id: str
  enabled: bool = Field(default=True)
  public_account_id: str = Field(default="")
  credential_version: int = Field(default=1)
  management_credential_hash: str
  management_credential_hint: str = Field(default="")
  application_id: str = Field(default="")
  quota_bytes_total: int = Field(default=0)
  quota_bytes_allocated: int = Field(default=0)
  quota_bytes_used: int = Field(default=0)
  quota_bytes_reserved: int = Field(default=0)
  quota_bytes_trashed: int = Field(default=0)
  quota_objects_total: int = Field(default=0)
  quota_objects_allocated: int = Field(default=0)
  quota_objects_used: int = Field(default=0)
  quota_objects_reserved: int = Field(default=0)
  quota_objects_trashed: int = Field(default=0)
  revision: int = Field(default=0)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_accounts"
    indexes = [
      IndexModel(["account_id"], unique=True),
      IndexModel(["app_name"], unique=True),
      IndexModel(
        ["public_account_id"],
        unique=True,
        partialFilterExpression={"public_account_id": {"$gt": ""}},
      ),
    ]


class VirtualAccountGrant(Document):
  """把已验证的控制台用户显式授到一个虚拟账号。不按名称自动匹配。"""
  grant_id: str
  account_id: str
  user_id: str
  username: str
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_account_grants"
    indexes = [
      IndexModel(["grant_id"], unique=True),
      IndexModel([("user_id", 1), ("account_id", 1)], unique=True),
    ]


class Tenant(Document):
  tenant_id: str
  account_id: str
  external_tenant_id: str
  logical_name: str
  generation: int = Field(default=1)
  state: str = Field(default="active")
  quota_bytes_total: int = Field(default=0)
  quota_bytes_used: int = Field(default=0)
  quota_bytes_reserved: int = Field(default=0)
  quota_bytes_trashed: int = Field(default=0)
  quota_objects_total: int = Field(default=0)
  quota_objects_used: int = Field(default=0)
  quota_objects_reserved: int = Field(default=0)
  quota_objects_trashed: int = Field(default=0)
  purge_started: bool = Field(default=False)
  quota_released: bool = Field(default=False)
  revision: int = Field(default=0)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  frozen_at: Optional[datetime] = None
  retire_requested_at: Optional[datetime] = None
  retired_at: Optional[datetime] = None

  class Settings:
    name = "vs_tenants"
    indexes = [
      IndexModel(["tenant_id"], unique=True),
      IndexModel(
        [("account_id", 1), ("external_tenant_id", 1), ("generation", 1)],
        unique=True,
      ),
      IndexModel(
        [("account_id", 1), ("external_tenant_id", 1)],
        unique=True,
        partialFilterExpression={"state": {"$in": list(ACTIVE_TENANT_STATES)}},
      ),
      IndexModel(
        [("account_id", 1), ("logical_name", 1)],
        unique=True,
        partialFilterExpression={"state": {"$in": list(ACTIVE_TENANT_STATES)}},
      ),
    ]


class TenantAPIKey(Document):
  key_id: str
  tenant_id: str
  account_id: str
  purpose: str = Field(default="tenant_data")
  key_hash: str
  key_hint: str
  expires_at: datetime
  enabled: bool = Field(default=True)
  created_at: datetime = Field(default_factory=utc_now)
  revoked_at: Optional[datetime] = None

  class Settings:
    name = "vs_tenant_api_keys"
    indexes = [
      IndexModel(["key_id"], unique=True),
      IndexModel(["key_hash"], unique=True),
      IndexModel([("tenant_id", 1), ("enabled", 1)]),
    ]


class KeyDelivery(Document):
  delivery_id: str
  account_id: str
  tenant_id: str
  key_id: str
  subject_hash: str
  idempotency_key: str
  request_fingerprint: str
  encrypted_key: str
  window_starts_at: datetime
  window_ends_at: datetime
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_key_deliveries"
    indexes = [
      IndexModel(["delivery_id"], unique=True),
      IndexModel(
        [("account_id", 1), ("subject_hash", 1), ("idempotency_key", 1)],
        unique=True,
      ),
    ]


class UploadSession(Document):
  upload_id: str
  account_id: str
  tenant_id: str
  home_region: str
  object_key: str
  object_id: str
  storage_key: str
  bucket: str
  minio_upload_id: str = Field(default="")
  size_bytes: int
  state: str = Field(default="initializing")
  confirmed_parts: dict = Field(default_factory=dict)
  reservation_held: bool = Field(default=True)
  result_uncertain: bool = Field(default=False)
  fence_epoch: int = Field(default=1)
  worker_lease_id: str = Field(default="")
  worker_lease_until: Optional[datetime] = None
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_upload_sessions"
    indexes = [
      IndexModel(["upload_id"], unique=True),
      IndexModel(
        [("tenant_id", 1), ("object_key", 1)],
        unique=True,
        partialFilterExpression={"state": {"$in": list(OPEN_SESSION_STATES)}},
      ),
      IndexModel([("state", 1), ("reservation_held", 1)]),
    ]


class VirtualObject(Document):
  object_id: str
  account_id: str
  tenant_id: str
  object_key: str
  storage_key: str
  bucket: str
  size_bytes: int
  etag: str = Field(default="")
  content_type: str = Field(default="application/octet-stream")
  state: str = Field(default="active")
  trash_released: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  soft_deleted_at: Optional[datetime] = None
  purge_after: Optional[datetime] = None
  purged_at: Optional[datetime] = None

  class Settings:
    name = "vs_objects"
    indexes = [
      IndexModel(["object_id"], unique=True),
      IndexModel(
        [("tenant_id", 1), ("object_key", 1)],
        unique=True,
        partialFilterExpression={"state": {"$in": list(OCCUPYING_OBJECT_STATES)}},
      ),
      IndexModel([("tenant_id", 1), ("state", 1), ("object_key", 1)]),
    ]


class QuotaLedgerEntry(Document):
  entry_id: str
  account_id: str
  tenant_id: str = Field(default="")
  operation: str
  delta_bytes: int = Field(default=0)
  delta_objects: int = Field(default=0)
  resource_type: str
  resource_id: str
  idempotency_key: str
  note: str = Field(default="")
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_quota_ledger"
    indexes = [
      IndexModel(["entry_id"], unique=True),
      IndexModel(["idempotency_key"], unique=True),
      IndexModel([("tenant_id", 1), ("created_at", -1)]),
    ]


class AuditRecord(Document):
  audit_id: str
  account_id: str = Field(default="")
  tenant_id: str = Field(default="")
  actor_type: str
  actor_id: str
  action: str
  resource_id: str = Field(default="")
  detail: dict = Field(default_factory=dict)
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_audit"
    indexes = [IndexModel(["audit_id"], unique=True), IndexModel([("account_id", 1), ("created_at", -1)])]


class MinioCall(Document):
  call_id: str
  resource_id: str
  op: str
  fence_epoch: int
  state: str = Field(default="open")
  started_at: datetime = Field(default_factory=utc_now)
  finished_at: Optional[datetime] = None

  class Settings:
    name = "vs_minio_calls"
    indexes = [IndexModel(["call_id"], unique=True), IndexModel([("resource_id", 1), ("state", 1)])]


OCCUPYING_APPLICATION_FILTER = {
  "$or": [
    {"status": "submitted"},
    {"status": "approved", "init_status": {"$in": ["pending", "initializing", "active", "failed"]}},
  ]
}


class VirtualAccountApplication(Document):
  """申请单。拒绝和撤回是终态，名字立即释放。"""

  application_id: str
  public_account_id: str
  purpose: str = Field(default="")
  requested_quota_bytes: int = Field(default=0)
  requested_quota_objects: int = Field(default=0)
  approved_quota_bytes: int = Field(default=0)
  approved_quota_objects: int = Field(default=0)
  status: str = Field(default="submitted")
  init_status: str = Field(default="")
  owner_user_id: str = Field(default="")
  owner_username: str = Field(default="")
  owner_subject: str = Field(default="")
  revision: int = Field(default=0)
  idempotency_key: str = Field(default="")
  request_fingerprint: str = Field(default="")
  reject_reason: str = Field(default="")
  account_id: str = Field(default="")
  init_attempt: str = Field(default="")
  init_error: str = Field(default="")
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_account_applications"
    indexes = [
      IndexModel(["application_id"], unique=True),
      IndexModel(
        ["public_account_id"],
        unique=True,
        name="public_id_submitted",
        partialFilterExpression={"status": "submitted"},
      ),
      IndexModel(
        ["public_account_id"],
        unique=True,
        name="public_id_approved",
        partialFilterExpression={
          "status": "approved",
          "init_status": {"$in": ["pending", "initializing", "active", "failed"]},
        },
      ),
      IndexModel(
        [("owner_subject", 1), ("idempotency_key", 1)],
        unique=True,
        partialFilterExpression={"idempotency_key": {"$gt": ""}},
      ),
    ]


class OAuthRefreshToken(Document):
  token_id: str
  public_account_id: str
  credential_version: int
  token_hash: str
  expires_at: datetime
  revoked: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_oauth_refresh_tokens"
    indexes = [
      IndexModel(["token_id"], unique=True),
      IndexModel(["token_hash"], unique=True),
      IndexModel([("public_account_id", 1), ("revoked", 1)]),
    ]


class OperationRecord(Document):
  operation_id: str
  account_id: str
  tenant_id: str
  op_type: str
  state: str
  detail: dict = Field(default_factory=dict)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "vs_operations"
    indexes = [IndexModel(["operation_id"], unique=True)]
