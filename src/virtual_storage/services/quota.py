"""Mongo 双层事务账本。配额权威只在这里，不写 Etcd。"""
from __future__ import annotations

from src.virtual_storage.core.exception import QuotaExceededError
from src.virtual_storage.models.documents import QuotaLedgerEntry, Tenant, VirtualAccount
from src.virtual_storage.services.support import new_id, stamp, within_quota_expr, write_audit


def _accounts():
  return VirtualAccount.get_motor_collection()


def _tenants():
  return Tenant.get_motor_collection()


def _ledger():
  return QuotaLedgerEntry.get_motor_collection()


async def _apply_pair(
  *,
  account_id: str,
  tenant_id: str,
  account_filter: dict,
  tenant_filter: dict,
  account_inc: dict,
  tenant_inc: dict,
  session,
) -> None:
  now = stamp()
  tenant_result = await _tenants().update_one(
    tenant_filter,
    {"$inc": {**tenant_inc, "revision": 1}, "$set": {"updated_at": now}},
    session=session,
  )
  if tenant_result.modified_count != 1:
    raise QuotaExceededError("租户配额条件不满足")
  account_result = await _accounts().update_one(
    account_filter,
    {"$inc": {**account_inc, "revision": 1}, "$set": {"updated_at": now}},
    session=session,
  )
  if account_result.modified_count != 1:
    raise QuotaExceededError("账号配额条件不满足")


async def _entry(
  *,
  account_id: str,
  tenant_id: str,
  operation: str,
  delta_bytes: int,
  delta_objects: int,
  resource_type: str,
  resource_id: str,
  idempotency_key: str,
  session,
) -> None:
  await _ledger().insert_one(
    {
      "entry_id": new_id("led"),
      "account_id": account_id,
      "tenant_id": tenant_id,
      "operation": operation,
      "delta_bytes": delta_bytes,
      "delta_objects": delta_objects,
      "resource_type": resource_type,
      "resource_id": resource_id,
      "idempotency_key": idempotency_key,
      "note": "",
      "created_at": stamp(),
    },
    session=session,
  )


async def allocate_tenant_quota(
  *,
  account_id: str,
  tenant_id: str,
  quota_bytes: int,
  quota_objects: int,
  session,
) -> None:
  now = stamp()
  result = await _accounts().update_one(
    {
      "account_id": account_id,
      "enabled": True,
      "$expr": {
        "$and": [
          {"$lte": [{"$add": ["$quota_bytes_allocated", quota_bytes]}, "$quota_bytes_total"]},
          {"$lte": [{"$add": ["$quota_objects_allocated", quota_objects]}, "$quota_objects_total"]},
        ]
      },
    },
    {"$inc": {
      "quota_bytes_allocated": quota_bytes,
      "quota_objects_allocated": quota_objects,
      "revision": 1,
    }, "$set": {"updated_at": now}},
    session=session,
  )
  if result.modified_count != 1:
    raise QuotaExceededError("账号可分配额度不足")
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="allocate",
    delta_bytes=quota_bytes,
    delta_objects=quota_objects,
    resource_type="tenant",
    resource_id=tenant_id,
    idempotency_key=f"allocate:{tenant_id}",
    session=session,
  )


async def reserve_upload(
  *,
  account_id: str,
  tenant_id: str,
  size_bytes: int,
  upload_id: str,
  session,
) -> None:
  quota_filter = within_quota_expr(size_bytes, 1)
  await _apply_pair(
    account_id=account_id,
    tenant_id=tenant_id,
    tenant_filter={"tenant_id": tenant_id, "state": "active", **quota_filter},
    account_filter={"account_id": account_id, "enabled": True, **quota_filter},
    tenant_inc={"quota_bytes_reserved": size_bytes, "quota_objects_reserved": 1},
    account_inc={"quota_bytes_reserved": size_bytes, "quota_objects_reserved": 1},
    session=session,
  )
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="reserve",
    delta_bytes=size_bytes,
    delta_objects=1,
    resource_type="upload_session",
    resource_id=upload_id,
    idempotency_key=f"reserve:{upload_id}",
    session=session,
  )
  await write_audit(
    account_id=account_id,
    tenant_id=tenant_id,
    actor_type="tenant_key",
    actor_id=tenant_id,
    action="quota.reserve",
    resource_id=upload_id,
    detail={"bytes": size_bytes},
    session=session,
  )


async def release_reservation(
  *,
  account_id: str,
  tenant_id: str,
  size_bytes: int,
  upload_id: str,
  session,
) -> None:
  await _apply_pair(
    account_id=account_id,
    tenant_id=tenant_id,
    tenant_filter={
      "tenant_id": tenant_id,
      "quota_bytes_reserved": {"$gte": size_bytes},
      "quota_objects_reserved": {"$gte": 1},
    },
    account_filter={
      "account_id": account_id,
      "quota_bytes_reserved": {"$gte": size_bytes},
      "quota_objects_reserved": {"$gte": 1},
    },
    tenant_inc={"quota_bytes_reserved": -size_bytes, "quota_objects_reserved": -1},
    account_inc={"quota_bytes_reserved": -size_bytes, "quota_objects_reserved": -1},
    session=session,
  )
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="release",
    delta_bytes=-size_bytes,
    delta_objects=-1,
    resource_type="upload_session",
    resource_id=upload_id,
    idempotency_key=f"release:{upload_id}",
    session=session,
  )


async def commit_reservation(
  *,
  account_id: str,
  tenant_id: str,
  size_bytes: int,
  upload_id: str,
  session,
) -> None:
  move = {
    "quota_bytes_reserved": -size_bytes,
    "quota_bytes_used": size_bytes,
    "quota_objects_reserved": -1,
    "quota_objects_used": 1,
  }
  await _apply_pair(
    account_id=account_id,
    tenant_id=tenant_id,
    tenant_filter={
      "tenant_id": tenant_id,
      "quota_bytes_reserved": {"$gte": size_bytes},
      "quota_objects_reserved": {"$gte": 1},
    },
    account_filter={
      "account_id": account_id,
      "quota_bytes_reserved": {"$gte": size_bytes},
      "quota_objects_reserved": {"$gte": 1},
    },
    tenant_inc=move,
    account_inc=move,
    session=session,
  )
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="commit",
    delta_bytes=0,
    delta_objects=0,
    resource_type="upload_session",
    resource_id=upload_id,
    idempotency_key=f"commit:{upload_id}",
    session=session,
  )


async def move_used_to_trash(
  *,
  account_id: str,
  tenant_id: str,
  size_bytes: int,
  object_id: str,
  session,
) -> None:
  move = {
    "quota_bytes_used": -size_bytes,
    "quota_bytes_trashed": size_bytes,
    "quota_objects_used": -1,
    "quota_objects_trashed": 1,
  }
  await _apply_pair(
    account_id=account_id,
    tenant_id=tenant_id,
    tenant_filter={
      "tenant_id": tenant_id,
      "quota_bytes_used": {"$gte": size_bytes},
      "quota_objects_used": {"$gte": 1},
    },
    account_filter={
      "account_id": account_id,
      "quota_bytes_used": {"$gte": size_bytes},
      "quota_objects_used": {"$gte": 1},
    },
    tenant_inc=move,
    account_inc=move,
    session=session,
  )
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="trash",
    delta_bytes=0,
    delta_objects=0,
    resource_type="object",
    resource_id=object_id,
    idempotency_key=f"trash:{object_id}",
    session=session,
  )


async def move_trash_to_used(
  *,
  account_id: str,
  tenant_id: str,
  size_bytes: int,
  object_id: str,
  session,
) -> None:
  move = {
    "quota_bytes_trashed": -size_bytes,
    "quota_bytes_used": size_bytes,
    "quota_objects_trashed": -1,
    "quota_objects_used": 1,
  }
  await _apply_pair(
    account_id=account_id,
    tenant_id=tenant_id,
    tenant_filter={
      "tenant_id": tenant_id,
      "quota_bytes_trashed": {"$gte": size_bytes},
      "quota_objects_trashed": {"$gte": 1},
    },
    account_filter={
      "account_id": account_id,
      "quota_bytes_trashed": {"$gte": size_bytes},
      "quota_objects_trashed": {"$gte": 1},
    },
    tenant_inc=move,
    account_inc=move,
    session=session,
  )
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="restore",
    delta_bytes=0,
    delta_objects=0,
    resource_type="object",
    resource_id=object_id,
    idempotency_key=f"restore:{object_id}",
    session=session,
  )


async def release_trash_once(
  *,
  account_id: str,
  tenant_id: str,
  size_bytes: int,
  object_id: str,
  session,
) -> None:
  await _apply_pair(
    account_id=account_id,
    tenant_id=tenant_id,
    tenant_filter={
      "tenant_id": tenant_id,
      "quota_bytes_trashed": {"$gte": size_bytes},
      "quota_objects_trashed": {"$gte": 1},
    },
    account_filter={
      "account_id": account_id,
      "quota_bytes_trashed": {"$gte": size_bytes},
      "quota_objects_trashed": {"$gte": 1},
    },
    tenant_inc={"quota_bytes_trashed": -size_bytes, "quota_objects_trashed": -1},
    account_inc={"quota_bytes_trashed": -size_bytes, "quota_objects_trashed": -1},
    session=session,
  )
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="purge",
    delta_bytes=-size_bytes,
    delta_objects=-1,
    resource_type="object",
    resource_id=object_id,
    idempotency_key=f"purge:{object_id}",
    session=session,
  )


async def deallocate_tenant_quota(
  *,
  account_id: str,
  tenant_id: str,
  quota_bytes: int,
  quota_objects: int,
  session,
) -> None:
  now = stamp()
  result = await _accounts().update_one(
    {
      "account_id": account_id,
      "quota_bytes_allocated": {"$gte": quota_bytes},
      "quota_objects_allocated": {"$gte": quota_objects},
    },
    {"$inc": {
      "quota_bytes_allocated": -quota_bytes,
      "quota_objects_allocated": -quota_objects,
      "revision": 1,
    }, "$set": {"updated_at": now}},
    session=session,
  )
  if result.modified_count != 1:
    raise QuotaExceededError("账号已分配额度不足以释放")
  await _entry(
    account_id=account_id,
    tenant_id=tenant_id,
    operation="deallocate",
    delta_bytes=-quota_bytes,
    delta_objects=-quota_objects,
    resource_type="tenant",
    resource_id=tenant_id,
    idempotency_key=f"deallocate:{tenant_id}",
    session=session,
  )
