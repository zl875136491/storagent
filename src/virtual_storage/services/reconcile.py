"""对账覆盖仍占用额度的中间状态。不确定的预留只向上修正，不因租约过期而释放。"""
from __future__ import annotations

from src.virtual_storage.models.documents import (
  OCCUPYING_OBJECT_STATES,
  OPEN_SESSION_STATES,
  Tenant,
  UploadSession,
  VirtualAccount,
  VirtualObject,
)
from src.virtual_storage.services.support import stamp, write_audit

COVERED_SESSION_STATES = OPEN_SESSION_STATES
COVERED_OBJECT_STATES = OCCUPYING_OBJECT_STATES


async def tenant_snapshot(tenant_id: str) -> dict:
  sessions = await UploadSession.find(
    UploadSession.tenant_id == tenant_id,
    {"reservation_held": True, "state": {"$in": list(COVERED_SESSION_STATES)}},
  ).to_list()
  objects = await VirtualObject.find(
    VirtualObject.tenant_id == tenant_id,
    {"state": {"$in": list(COVERED_OBJECT_STATES)}},
  ).to_list()
  reserved_bytes = sum(row.size_bytes for row in sessions)
  reserved_objects = len(sessions)
  used_bytes = sum(row.size_bytes for row in objects if row.state == "active")
  used_objects = sum(1 for row in objects if row.state == "active")
  trashed_bytes = sum(row.size_bytes for row in objects if row.state in {"soft_deleted", "purge_pending"})
  trashed_objects = sum(1 for row in objects if row.state in {"soft_deleted", "purge_pending"})
  return {
    "states_covered": list(COVERED_SESSION_STATES) + list(COVERED_OBJECT_STATES),
    "session_states": sorted({row.state for row in sessions}),
    "object_states": sorted({row.state for row in objects}),
    "reserved_bytes": reserved_bytes,
    "reserved_objects": reserved_objects,
    "used_bytes": used_bytes,
    "used_objects": used_objects,
    "trashed_bytes": trashed_bytes,
    "trashed_objects": trashed_objects,
  }


async def reconcile_tenant(tenant_id: str) -> dict:
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  expected = await tenant_snapshot(tenant_id)
  stored = {
    "reserved_bytes": tenant.quota_bytes_reserved,
    "used_bytes": tenant.quota_bytes_used,
    "trashed_bytes": tenant.quota_bytes_trashed,
  }
  repaired = {}
  updates = {}
  # 只补记低于实体合计的预留/占用，避免把结果不明的额度放掉。
  if tenant.quota_bytes_reserved < expected["reserved_bytes"]:
    updates["quota_bytes_reserved"] = expected["reserved_bytes"]
    updates["quota_objects_reserved"] = expected["reserved_objects"]
    repaired["reserved_bytes"] = expected["reserved_bytes"]
  if tenant.quota_bytes_used < expected["used_bytes"]:
    updates["quota_bytes_used"] = expected["used_bytes"]
    updates["quota_objects_used"] = expected["used_objects"]
    repaired["used_bytes"] = expected["used_bytes"]
  if tenant.quota_bytes_trashed < expected["trashed_bytes"]:
    updates["quota_bytes_trashed"] = expected["trashed_bytes"]
    updates["quota_objects_trashed"] = expected["trashed_objects"]
    repaired["trashed_bytes"] = expected["trashed_bytes"]
  if updates:
    updates["updated_at"] = stamp()
    await Tenant.get_motor_collection().update_one({"tenant_id": tenant_id}, {"$set": updates})
    account = await VirtualAccount.find_one(VirtualAccount.account_id == tenant.account_id)
    if account:
      siblings = await Tenant.find(Tenant.account_id == account.account_id, {"state": {"$ne": "retired"}}).to_list()
      await VirtualAccount.get_motor_collection().update_one(
        {"account_id": account.account_id},
        {"$max": {
          "quota_bytes_reserved": sum(item.quota_bytes_reserved for item in siblings),
          "quota_bytes_used": sum(item.quota_bytes_used for item in siblings),
          "quota_bytes_trashed": sum(item.quota_bytes_trashed for item in siblings),
          "quota_objects_reserved": sum(item.quota_objects_reserved for item in siblings),
          "quota_objects_used": sum(item.quota_objects_used for item in siblings),
          "quota_objects_trashed": sum(item.quota_objects_trashed for item in siblings),
        }, "$set": {"updated_at": stamp()}},
      )
  discrepancy = {
    key: {"stored": stored[key], "expected": expected[key]}
    for key in stored
    if stored[key] != expected[key]
  }
  await write_audit(
    account_id=tenant.account_id,
    tenant_id=tenant_id,
    actor_type="worker",
    actor_id="reconcile",
    action="quota.reconcile",
    resource_id=tenant_id,
    detail={"repaired": repaired, "discrepancy_before_release_guard": discrepancy, "states": expected["states_covered"]},
  )
  return {"expected": expected, "repaired": repaired, "discrepancy": discrepancy}
