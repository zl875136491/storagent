"""标识、审计和配额占用计算。审计明细不得包含明文 Key。"""
from __future__ import annotations

import secrets
from datetime import datetime, timezone

from src.utils.helpers import utc_now
from src.virtual_storage.models.documents import AuditRecord


def new_id(prefix: str) -> str:
  return f"{prefix}_{secrets.token_hex(12)}"


def tenant_key_secret() -> str:
  return "vst_" + secrets.token_urlsafe(32)


def occupied(document) -> tuple[int, int]:
  return (
    int(document.quota_bytes_used) + int(document.quota_bytes_reserved) + int(document.quota_bytes_trashed),
    int(document.quota_objects_used) + int(document.quota_objects_reserved) + int(document.quota_objects_trashed),
  )


def within_quota_expr(extra_bytes: int, extra_objects: int) -> dict:
  return {
    "$expr": {
      "$and": [
        {"$lte": [
          {"$add": ["$quota_bytes_used", "$quota_bytes_reserved", "$quota_bytes_trashed", extra_bytes]},
          "$quota_bytes_total",
        ]},
        {"$lte": [
          {"$add": ["$quota_objects_used", "$quota_objects_reserved", "$quota_objects_trashed", extra_objects]},
          "$quota_objects_total",
        ]},
      ]
    }
  }


async def write_audit(
  *,
  account_id: str,
  tenant_id: str,
  actor_type: str,
  actor_id: str,
  action: str,
  resource_id: str = "",
  detail: dict | None = None,
  session=None,
) -> None:
  clean = dict(detail or {})
  for forbidden in ("api_key", "credential", "secret", "plaintext", "authorization"):
    clean.pop(forbidden, None)
  await AuditRecord(
    audit_id=new_id("audit"),
    account_id=account_id,
    tenant_id=tenant_id,
    actor_type=actor_type,
    actor_id=actor_id,
    action=action,
    resource_id=resource_id,
    detail=clean,
    created_at=utc_now(),
  ).insert(session=session)


def stamp() -> datetime:
  return utc_now()


def as_utc(value: datetime) -> datetime:
  if value.tzinfo is None:
    return value.replace(tzinfo=timezone.utc)
  return value.astimezone(timezone.utc)
