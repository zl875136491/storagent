"""冻结、注销和撤销。撤销以不可逆清除是否开始为准，撤销后回到冻结。"""
from __future__ import annotations

from src.virtual_storage.core.database import transaction
from src.virtual_storage.core.exception import ConflictError, TenantNotFoundError
from src.virtual_storage.core.minio_gateway import MinioGateway
from src.virtual_storage.models.documents import (
  OCCUPYING_OBJECT_STATES,
  OPEN_SESSION_STATES,
  OperationRecord,
  Tenant,
  UploadSession,
  VirtualObject,
)
from src.virtual_storage.services.accounts import authenticate_management
from src.virtual_storage.services.objects import purge_object
from src.virtual_storage.services.quota import deallocate_tenant_quota
from src.virtual_storage.services.support import new_id, stamp, write_audit
from src.virtual_storage.services.uploads import drive_session


async def _tenant(app_name: str, credential: str, tenant_id: str) -> tuple[Tenant, object]:
  from src.virtual_storage.models.documents import VirtualAccount
  account = await authenticate_management(app_name, credential)
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id, Tenant.account_id == account.account_id)
  if tenant is None:
    raise TenantNotFoundError(tenant_id)
  return tenant, account


async def freeze_tenant(app_name: str, credential: str, tenant_id: str) -> dict:
  tenant, account = await _tenant(app_name, credential, tenant_id)
  result = await Tenant.get_motor_collection().update_one(
    {"tenant_id": tenant_id, "state": "active"},
    {"$set": {"state": "frozen", "frozen_at": stamp(), "updated_at": stamp()}, "$inc": {"revision": 1}},
  )
  if result.modified_count != 1 and tenant.state != "frozen":
    raise ConflictError("当前状态不能冻结")
  await write_audit(
    account_id=account.account_id, tenant_id=tenant_id, actor_type="management",
    actor_id=account.management_credential_hint, action="tenant.freeze", resource_id=tenant_id,
  )
  refreshed = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  from src.virtual_storage.services.tenants import tenant_view
  return tenant_view(refreshed)


async def unfreeze_tenant(app_name: str, credential: str, tenant_id: str) -> dict:
  tenant, account = await _tenant(app_name, credential, tenant_id)
  result = await Tenant.get_motor_collection().update_one(
    {"tenant_id": tenant_id, "state": "frozen", "purge_started": False},
    {"$set": {"state": "active", "updated_at": stamp()}, "$inc": {"revision": 1}},
  )
  if result.modified_count != 1:
    raise ConflictError("只有未开始清除的冻结租户可以解冻")
  await write_audit(
    account_id=account.account_id, tenant_id=tenant_id, actor_type="management",
    actor_id=account.management_credential_hint, action="tenant.unfreeze", resource_id=tenant_id,
  )
  from src.virtual_storage.services.tenants import tenant_view
  refreshed = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  return tenant_view(refreshed)


async def retire_tenant(app_name: str, credential: str, tenant_id: str) -> dict:
  tenant, account = await _tenant(app_name, credential, tenant_id)
  if tenant.state == "retired":
    raise ConflictError("租户已注销")
  operation_id = new_id("op")
  result = await Tenant.get_motor_collection().update_one(
    {"tenant_id": tenant_id, "state": {"$in": ["active", "frozen"]}, "purge_started": False},
    {"$set": {
      "state": "retiring",
      "retire_requested_at": stamp(),
      "updated_at": stamp(),
    }, "$inc": {"revision": 1}},
  )
  if result.modified_count != 1:
    refreshed = await Tenant.find_one(Tenant.tenant_id == tenant_id)
    if refreshed and refreshed.state in {"retiring", "purge_pending"}:
      existing = await OperationRecord.find_one(
        OperationRecord.tenant_id == tenant_id,
        OperationRecord.op_type == "retire",
        OperationRecord.state == "running",
      )
      return {"operation_id": existing.operation_id if existing else "", "state": refreshed.state}
    raise ConflictError("当前状态不能注销")
  await OperationRecord(
    operation_id=operation_id,
    account_id=account.account_id,
    tenant_id=tenant_id,
    op_type="retire",
    state="running",
    created_at=stamp(),
    updated_at=stamp(),
  ).insert()
  await write_audit(
    account_id=account.account_id, tenant_id=tenant_id, actor_type="management",
    actor_id=account.management_credential_hint, action="tenant.retire", resource_id=operation_id,
  )
  return {"operation_id": operation_id, "state": "retiring"}


async def cancel_retire(app_name: str, credential: str, tenant_id: str) -> dict:
  tenant, account = await _tenant(app_name, credential, tenant_id)
  pending = await VirtualObject.find(
    VirtualObject.tenant_id == tenant_id,
    VirtualObject.state == "purge_pending",
  ).count()
  if tenant.purge_started or pending:
    raise ConflictError("不可逆清除已开始，不能撤销注销")
  result = await Tenant.get_motor_collection().update_one(
    {"tenant_id": tenant_id, "state": "retiring", "purge_started": False},
    {"$set": {"state": "frozen", "frozen_at": stamp(), "updated_at": stamp()}, "$inc": {"revision": 1}},
  )
  if result.modified_count != 1:
    raise ConflictError("当前状态不能撤销注销")
  await OperationRecord.get_motor_collection().update_many(
    {"tenant_id": tenant_id, "op_type": "retire", "state": "running"},
    {"$set": {"state": "cancelled", "updated_at": stamp()}},
  )
  await write_audit(
    account_id=account.account_id, tenant_id=tenant_id, actor_type="management",
    actor_id=account.management_credential_hint, action="tenant.cancel_retire", resource_id=tenant_id,
    detail={"restored_state": "frozen"},
  )
  from src.virtual_storage.services.tenants import tenant_view
  refreshed = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  return tenant_view(refreshed)


async def get_operation(app_name: str, credential: str, operation_id: str) -> dict:
  account = await authenticate_management(app_name, credential)
  operation = await OperationRecord.find_one(
    OperationRecord.operation_id == operation_id,
    OperationRecord.account_id == account.account_id,
  )
  if operation is None:
    raise TenantNotFoundError(operation_id)
  return {
    "operation_id": operation.operation_id,
    "tenant_id": operation.tenant_id,
    "op_type": operation.op_type,
    "state": operation.state,
  }


async def advance_retire(tenant_id: str, gateway: MinioGateway, worker_id: str) -> dict:
  tenant = await Tenant.find_one(Tenant.tenant_id == tenant_id)
  if tenant is None or tenant.state not in {"retiring", "purge_pending"}:
    return {"action": "idle"}
  sessions = await UploadSession.find(
    UploadSession.tenant_id == tenant_id,
    {"state": {"$in": list(OPEN_SESSION_STATES)}},
  ).to_list()
  for session in sessions:
    if session.state != "aborting":
      await UploadSession.get_motor_collection().update_one(
        {"upload_id": session.upload_id, "reservation_held": True},
        {"$set": {"state": "aborting", "updated_at": stamp()}},
      )
    await drive_session(session.upload_id, gateway, worker_id)
  still_open = await UploadSession.find(
    UploadSession.tenant_id == tenant_id,
    {"reservation_held": True},
  ).count()
  if still_open:
    return {"action": "hold", "reason": "sessions"}
  if not tenant.purge_started:
    from src.virtual_storage.services.quota import move_used_to_trash
    occupants = await VirtualObject.find(
      VirtualObject.tenant_id == tenant_id,
      {"state": {"$in": ["active", "soft_deleted"]}},
    ).to_list()
    async with transaction() as session:
      marked = await Tenant.get_motor_collection().update_one(
        {"tenant_id": tenant_id, "state": "retiring", "purge_started": False},
        {"$set": {"purge_started": True, "state": "purge_pending", "updated_at": stamp()}},
        session=session,
      )
      if marked.modified_count == 1:
        for obj in occupants:
          await VirtualObject.get_motor_collection().update_one(
            {"object_id": obj.object_id, "state": obj.state},
            {"$set": {"state": "purge_pending", "updated_at": stamp()}},
            session=session,
          )
          if obj.state == "active":
            await move_used_to_trash(
              account_id=obj.account_id,
              tenant_id=tenant_id,
              size_bytes=obj.size_bytes,
              object_id=obj.object_id,
              session=session,
            )
        await write_audit(
          account_id=tenant.account_id, tenant_id=tenant_id, actor_type="worker",
          actor_id=worker_id, action="tenant.purge_started", resource_id=tenant_id, session=session,
        )
    return {"action": "purge_started"}
  objects = await VirtualObject.find(
    VirtualObject.tenant_id == tenant_id,
    {"state": {"$in": list(OCCUPYING_OBJECT_STATES)}},
  ).to_list()
  for obj in objects:
    await purge_object(obj.object_id, gateway, force=True)
  remaining = await VirtualObject.find(
    VirtualObject.tenant_id == tenant_id,
    {"state": {"$in": list(OCCUPYING_OBJECT_STATES)}},
  ).count()
  reserved = await UploadSession.find(
    UploadSession.tenant_id == tenant_id,
    {"reservation_held": True},
  ).count()
  if remaining or reserved:
    return {"action": "hold", "reason": "purge"}
  async with transaction() as session:
    retired = await Tenant.get_motor_collection().update_one(
      {
        "tenant_id": tenant_id,
        "state": "purge_pending",
        "quota_released": False,
        "quota_bytes_used": 0,
        "quota_bytes_reserved": 0,
        "quota_bytes_trashed": 0,
      },
      {"$set": {
        "state": "retired",
        "quota_released": True,
        "retired_at": stamp(),
        "updated_at": stamp(),
      }},
      session=session,
    )
    if retired.modified_count != 1:
      return {"action": "hold", "reason": "retire_guard"}
    await deallocate_tenant_quota(
      account_id=tenant.account_id,
      tenant_id=tenant_id,
      quota_bytes=tenant.quota_bytes_total,
      quota_objects=tenant.quota_objects_total,
      session=session,
    )
    await OperationRecord.get_motor_collection().update_many(
      {"tenant_id": tenant_id, "op_type": "retire", "state": "running"},
      {"$set": {"state": "succeeded", "updated_at": stamp()}},
      session=session,
    )
  return {"action": "retired"}
