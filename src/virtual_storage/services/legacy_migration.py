"""把已经存在的引导账号补成一张系统迁移申请。

不新建账号，不改密钥哈希，不猜测申请人。缺少明确归属时只返回计划。
"""
from __future__ import annotations

from src.virtual_storage.models.documents import VirtualAccount, VirtualAccountApplication
from src.virtual_storage.services.support import new_id, stamp, write_audit

MIGRATION_PURPOSE = "系统迁移：历史引导账号迁入，未经人工申请或审核"
REFUSED_OWNER_USERNAMES = frozenset({"vsreg-admin"})


class MigrationStopped(Exception):
  """迁移条件不满足。调用方必须停止，不能继续写。"""


def _public_id(value: str) -> str:
  public_id = (value or "").strip().lower()
  if not public_id:
    raise MigrationStopped("缺少要迁入的 account id")
  return public_id


async def _account(public_id: str) -> VirtualAccount:
  account = await VirtualAccount.find_one({"$or": [{"public_account_id": public_id}, {"app_name": public_id}]})
  if account is None:
    raise MigrationStopped("没有找到已存在的账号，拒绝新建")
  if account.public_account_id not in {"", public_id} or account.app_name != public_id:
    raise MigrationStopped("账号的公开 id 与 app_name 不一致，停止")
  return account


def _owner_error(owner_user_id: str, owner_username: str) -> str | None:
  user_id = (owner_user_id or "").strip()
  username = (owner_username or "").strip()
  if not user_id or not username:
    return "缺少明确的申请归属用户，拒绝写入"
  if username.lower() in REFUSED_OWNER_USERNAMES:
    return "不能把测试账号当作历史账号的归属"
  return None


async def plan_legacy_account(public_id: str, owner_user_id: str = "", owner_username: str = "") -> dict:
  """只读计划。不连接之外的副作用，不返回密钥或哈希。"""
  public_id = _public_id(public_id)
  account = await _account(public_id)
  existing = await VirtualAccountApplication.find_one(VirtualAccountApplication.account_id == account.account_id)
  if existing is None:
    existing = await VirtualAccountApplication.find_one(VirtualAccountApplication.public_account_id == public_id)
  plan = {
    "public_account_id": public_id,
    "account_id": account.account_id,
    "home_region": account.home_region,
    "application_id": account.application_id,
    "existing_application": bool(existing),
    "credential_version": account.credential_version,
    "would_write": False,
    "blocked": None,
  }
  blocked = _owner_error(owner_user_id, owner_username)
  if existing is not None:
    if existing.account_id not in {"", account.account_id} or existing.public_account_id != public_id:
      plan["blocked"] = "已有申请指向别的账号，停止"
      return plan
    if existing.status != "approved" or existing.init_status != "active":
      plan["blocked"] = "已有申请不是已生效的迁入记录，停止"
      return plan
    if blocked:
      plan["blocked"] = None
      plan["would_write"] = False
      return plan
    if existing.owner_user_id != owner_user_id.strip() or existing.owner_username != owner_username.strip():
      plan["blocked"] = "已有迁入申请的归属不同，停止"
      return plan
    return plan
  plan["blocked"] = blocked
  plan["would_write"] = blocked is None
  return plan


async def migrate_legacy_account(
  *,
  public_id: str,
  owner_user_id: str = "",
  owner_username: str = "",
  dry_run: bool = True,
) -> dict:
  plan = await plan_legacy_account(public_id, owner_user_id, owner_username)
  if plan["blocked"]:
    raise MigrationStopped(plan["blocked"])
  if not plan["would_write"] or dry_run:
    plan["dry_run"] = True
    return plan
  account = await _account(plan["public_account_id"])
  before_hash = account.management_credential_hash
  before_id = account.account_id
  item = VirtualAccountApplication(
    application_id=new_id("vsa"),
    public_account_id=plan["public_account_id"],
    purpose=MIGRATION_PURPOSE,
    requested_quota_bytes=account.quota_bytes_total,
    requested_quota_objects=account.quota_objects_total,
    approved_quota_bytes=account.quota_bytes_total,
    approved_quota_objects=account.quota_objects_total,
    status="approved",
    init_status="active",
    owner_user_id=owner_user_id.strip(),
    owner_username=owner_username.strip(),
    owner_subject=owner_username.strip().lower(),
    revision=1,
    account_id=account.account_id,
    created_at=stamp(),
    updated_at=stamp(),
  )
  try:
    await item.insert()
  except Exception as exc:
    from pymongo.errors import DuplicateKeyError
    if isinstance(exc, DuplicateKeyError):
      raise MigrationStopped("迁入申请与已有占名冲突，停止") from exc
    raise
  account.application_id = item.application_id
  if not account.public_account_id:
    account.public_account_id = plan["public_account_id"]
  account.updated_at = stamp()
  await account.save()
  if account.management_credential_hash != before_hash or account.account_id != before_id:
    raise MigrationStopped("迁移改动了账号身份或密钥，已停止")
  await write_audit(
    account_id=account.account_id,
    tenant_id="",
    actor_type="system",
    actor_id="migration",
    action="account.migrate_application",
    resource_id=item.application_id,
    detail={"public_account_id": plan["public_account_id"], "source": "legacy-bootstrap"},
  )
  plan["would_write"] = False
  plan["application_id"] = item.application_id
  plan["dry_run"] = False
  return plan
