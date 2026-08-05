"""
跨节点数据同步模块

通过 Etcd 在多地 Storagent 节点间同步：
- roles / users（身份按稳定业务键关联，密码哈希加密存储）
- region / servers（拓扑；MinIO 凭证加密存储）
- topology_layout（图形布局；不包含 MinIO Bucket Replication 规则）
- applications（应用元数据）
- api_keys（API 密钥；字典键为哈希，值为加密后的 Key）
"""
import asyncio
import secrets
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from loguru import logger

from src.configs.configs import settings
from src.core.crypto import (
  api_key_etcd_map_key,
  decrypt_secret,
  decrypt_server_entry,
  encrypt_secret,
  encrypt_server_entry,
)
from src.utils.helpers import utc_now
from src.modules.public.model import DEFAULT_APPLICATION_QUOTA_BYTES
from src.configs.consts import ROLE_SUPERADMIN, ROLE_USER

ETCD_KEY_REGION = "region"
ETCD_KEY_SERVERS = "servers"
ETCD_KEY_ROLES = "roles"
ETCD_KEY_USERS = "users"
ETCD_KEY_APPLICATIONS = "applications"
ETCD_KEY_API_KEYS = "api_keys"
ETCD_KEY_REVOKED_TOKENS = "revoked_tokens"
ETCD_KEY_AI_CONFIG = "ai_config"
ETCD_KEY_TOPOLOGY_LAYOUT = "topology_layout"

TOPOLOGY_LAYOUT_SCHEMA_VERSION = 1

SYNC_USER_PLACEHOLDER = "__sync__"


class LastSuperadminError(RuntimeError):
  pass


class UserIdentityUpdateLockBusyError(RuntimeError):
  pass


class UserIdentityUpdateLockLostError(RuntimeError):
  pass


# Compatibility for imports/tests written when only role writes used this lock.
UserRoleUpdateLockBusyError = UserIdentityUpdateLockBusyError


class UserIdentityVersionConflictError(RuntimeError):
  pass


def _parse_sync_datetime(value: Any) -> datetime | None:
  if not value:
    return None
  try:
    parsed = datetime.fromisoformat(str(value))
  except (TypeError, ValueError):
    return None
  if parsed.tzinfo is None:
    return parsed.replace(tzinfo=timezone.utc)
  return parsed


def _same_sync_datetime(left: datetime | None, right: datetime | None) -> bool:
  if left is None or right is None:
    return left is right
  if left.tzinfo is None:
    left = left.replace(tzinfo=timezone.utc)
  if right.tzinfo is None:
    right = right.replace(tzinfo=timezone.utc)
  return left.astimezone(timezone.utc) == right.astimezone(timezone.utc)


def _linked_document_id(value: Any) -> str | None:
  """Return a stable id for either a fetched Document or a Beanie Link."""
  if value is None:
    return None
  value_id = getattr(value, "id", None)
  if value_id is not None:
    return str(value_id)
  ref_id = getattr(getattr(value, "ref", None), "id", None)
  if ref_id is not None:
    return str(ref_id)
  return None


def _is_newer_sync_datetime(candidate: datetime | None, current: datetime | None) -> bool:
  if candidate is None:
    return False
  if current is None:
    return True
  if candidate.tzinfo is None:
    candidate = candidate.replace(tzinfo=timezone.utc)
  if current.tzinfo is None:
    current = current.replace(tzinfo=timezone.utc)
  return candidate.astimezone(timezone.utc) > current.astimezone(timezone.utc)


def _prefer_user_entry(current: dict | None, candidate: dict) -> bool:
  """Resolve same-username startup conflicts deterministically."""
  if not current:
    return True
  current_updated = _parse_sync_datetime(current.get("updated_at"))
  candidate_updated = _parse_sync_datetime(candidate.get("updated_at"))
  if candidate_updated and (not current_updated or candidate_updated > current_updated):
    return True
  if current_updated and (not candidate_updated or candidate_updated < current_updated):
    return False

  authority = settings.SYNC_AUTHORITY_REGION
  current_origin = str(current.get("origin_region") or "")
  candidate_origin = str(candidate.get("origin_region") or "")
  if candidate_origin == authority and current_origin != authority:
    return True
  if current_origin == authority and candidate_origin != authority:
    return False
  return candidate_origin < current_origin


def role_to_etcd_entry(role) -> dict:
  return {
    "name": role.name,
    "is_admin": bool(role.is_admin),
    "permissions": sorted(set(role.permissions or [])),
    "origin_region": settings.REGION,
  }


def _prefer_authority_entry(current: dict | None, candidate: dict) -> bool:
  if not current:
    return True
  authority = settings.SYNC_AUTHORITY_REGION
  current_origin = str(current.get("origin_region") or "")
  candidate_origin = str(candidate.get("origin_region") or "")
  if candidate_origin == current_origin:
    # Built-in role definitions are deterministic. Let a rolling upgrade from
    # the same origin replace its older permissions instead of pulling them
    # back into Mongo on startup.
    return True
  if candidate_origin == authority and current_origin != authority:
    return True
  if current_origin == authority and candidate_origin != authority:
    return False
  return candidate_origin < current_origin


def user_to_etcd_entry(user) -> dict:
  role_names = sorted({
    role.name
    for role in (user.roles or [])
    if getattr(role, "name", None)
  })
  return {
    "name": user.name,
    "hashed_password_enc": encrypt_secret(user.hashed_password),
    "auth_version": int(getattr(user, "auth_version", 0)),
    "role_names": role_names,
    "roles_version": int(getattr(user, "roles_version", 0)),
    "created_at": user.created_at.isoformat() if user.created_at else None,
    "updated_at": user.updated_at.isoformat() if user.updated_at else None,
    "origin_region": settings.REGION,
  }


def _entry_role_names(entry: dict | None) -> set[str]:
  if not isinstance(entry, dict):
    return set()
  role_names = entry.get("role_names") or []
  if not role_names and entry.get("role_name"):
    role_names = [entry["role_name"]]
  return {str(name) for name in role_names if name}


def _entry_version(entry: dict | None, field: str) -> int:
  if not isinstance(entry, dict):
    return 0
  try:
    return max(int(entry.get(field) or 0), 0)
  except (TypeError, ValueError):
    return 0


def _same_user_password(left: dict, right: dict) -> bool:
  left_value = str(left.get("hashed_password_enc") or "")
  right_value = str(right.get("hashed_password_enc") or "")
  try:
    return decrypt_secret(left_value) == decrypt_secret(right_value)
  except ValueError:
    return left_value == right_value


def _merge_user_entry(
  data: dict,
  username: str,
  entry: dict,
  *,
  fields: set[str] | None = None,
) -> dict:
  """Merge password and roles independently using monotonic field versions.

  Explicit identity mutations pass ``fields`` and must advance the matching
  version. Periodic reconciliation omits it and may only publish a field whose
  local version is newer than Etcd. Equal-version disagreements always keep
  Etcd authoritative, which safely converges legacy records.
  """
  current = data.get(username)
  if not isinstance(current, dict):
    data[username] = entry
    return data

  explicit = fields is not None
  selected = fields or set()
  merge_auth = "auth" in selected if explicit else (
    _entry_version(entry, "auth_version")
    > _entry_version(current, "auth_version")
  )
  merge_roles = "roles" in selected if explicit else (
    _entry_version(entry, "roles_version")
    > _entry_version(current, "roles_version")
  )

  if explicit and merge_auth:
    current_version = _entry_version(current, "auth_version")
    candidate_version = _entry_version(entry, "auth_version")
    same_auth = (
      candidate_version == current_version
      and _same_user_password(entry, current)
    )
    if candidate_version < current_version or (
      candidate_version == current_version and not same_auth
    ):
      raise UserIdentityVersionConflictError(
        f"用户 {username} 的密码版本已变化，请重试"
      )

  current_roles = _entry_role_names(current)
  desired_roles = _entry_role_names(entry)
  if explicit and merge_roles:
    current_version = _entry_version(current, "roles_version")
    candidate_version = _entry_version(entry, "roles_version")
    same_roles = candidate_version == current_version and desired_roles == current_roles
    if candidate_version < current_version or (
      candidate_version == current_version and not same_roles
    ):
      raise UserIdentityVersionConflictError(
        f"用户 {username} 的角色版本已变化，请重试"
      )

  if merge_roles and ROLE_SUPERADMIN in current_roles and ROLE_SUPERADMIN not in desired_roles:
    superadmin_count = sum(
      ROLE_SUPERADMIN in _entry_role_names(item)
      for item in data.values()
    )
    if superadmin_count <= 1:
      raise LastSuperadminError("不能取消系统中唯一的管理员")

  merged = dict(current)
  if merge_auth:
    for key in ("hashed_password_enc", "auth_version"):
      merged[key] = entry.get(key)
  if merge_roles:
    merged["role_names"] = list(entry.get("role_names") or [])
    merged["roles_version"] = _entry_version(entry, "roles_version")
    merged.pop("role_name", None)

  merge_profile = "profile" in selected if explicit else _prefer_user_entry(
    current,
    entry,
  )
  if merge_profile:
    for key in ("name", "created_at"):
      if entry.get(key) is not None:
        merged[key] = entry[key]
  if merge_auth or merge_roles or merge_profile:
    merged["updated_at"] = entry.get("updated_at")
    merged["origin_region"] = entry.get("origin_region")
  data[username] = merged
  return data


async def sync_roles_to_mongo(roles_data: dict) -> None:
  from src.modules.auth.model import Role
  from src.modules.auth import crud as user_crud

  for role_name, data in roles_data.items():
    if not role_name or not isinstance(data, dict):
      continue
    permissions = sorted({
      str(item) for item in (data.get("permissions") or []) if item
    })
    is_admin = bool(data.get("is_admin", False))
    role = await Role.find_one(Role.name == role_name)
    if not role:
      await Role(
        name=role_name,
        is_admin=is_admin,
        permissions=permissions,
      ).save()
      logger.info(f"Etcd sync: 创建 Role {role_name}")
      continue
    if role.is_admin != is_admin or sorted(role.permissions or []) != permissions:
      role.is_admin = is_admin
      role.permissions = permissions
      await role.save()
      logger.info(f"Etcd sync: 更新 Role {role_name}")
  await user_crud.recompute_all_user_permissions()


async def _sync_user_to_mongo_locked(username: str, data: dict):
  from src.modules.auth import crud as user_crud
  from src.modules.auth.model import Role

  encrypted_hash = str(data.get("hashed_password_enc") or "")
  if not encrypted_hash:
    logger.warning(f"Etcd sync: User {username} 缺少密码哈希，已跳过")
    return None
  try:
    hashed_password = decrypt_secret(encrypted_hash)
  except ValueError as e:
    logger.warning(f"Etcd sync: User {username} 密码哈希解密失败: {e}")
    return None

  basic_role = await user_crud.get_basic_role()
  roles = [basic_role] if basic_role else []
  seen_role_names = {ROLE_USER} if basic_role else set()
  role_names = data.get("role_names") or []
  if not role_names and data.get("role_name"):
    role_names = [data["role_name"]]
  for role_name in role_names:
    role = await Role.find_one(Role.name == str(role_name))
    if role and role.name not in seen_role_names:
      roles.append(role)
      seen_role_names.add(role.name)
  desired_permissions = await user_crud.get_all_permissions(roles)
  created_at = _parse_sync_datetime(data.get("created_at")) or utc_now()
  incoming_updated_at = _parse_sync_datetime(data.get("updated_at")) or created_at
  auth_version = _entry_version(data, "auth_version")
  roles_version = _entry_version(data, "roles_version")

  user = await user_crud.read_user_by_username(username)
  if not user:
    user = await user_crud.create_user(
      username=username,
      name=str(data.get("name") or username),
      hashed_password=hashed_password,
      roles=roles,
      is_sync=False,
    )
    user.created_at = created_at
    user.updated_at = incoming_updated_at
    user.auth_version = auth_version
    user.roles_version = roles_version
    await user.save()
    logger.info(f"Etcd sync: 创建可登录 User {username}")
    return user

  current_auth_version = max(int(getattr(user, "auth_version", 0)), 0)
  current_roles_version = max(int(getattr(user, "roles_version", 0)), 0)
  apply_auth = auth_version >= current_auth_version
  apply_roles = roles_version >= current_roles_version
  current_role_names = sorted({
    role.name
    for role in (user.roles or [])
    if getattr(role, "name", None)
  })
  desired_role_names = sorted(role.name for role in roles)
  changed = any((
    user.name != str(data.get("name") or username),
    apply_auth and user.hashed_password != hashed_password,
    apply_roles and current_role_names != desired_role_names,
    apply_roles and sorted(user.permissions or []) != sorted(desired_permissions),
    bool(getattr(user, "is_sync", False)),
    apply_auth and current_auth_version != auth_version,
    apply_roles and current_roles_version != roles_version,
    not _same_sync_datetime(user.created_at, created_at),
    _is_newer_sync_datetime(incoming_updated_at, user.updated_at),
  ))
  if not changed:
    return user
  user.name = str(data.get("name") or username)
  if apply_auth:
    user.hashed_password = hashed_password
    user.auth_version = auth_version
  if apply_roles:
    user.roles = roles
    user.permissions = desired_permissions
    user.roles_version = roles_version
  user.is_sync = False
  user.created_at = created_at
  if _is_newer_sync_datetime(incoming_updated_at, user.updated_at):
    user.updated_at = incoming_updated_at
  await user.save()
  logger.info(f"Etcd sync: 更新可登录 User {username}")
  return user


async def sync_users_to_mongo(users_data: dict) -> None:
  for username, data in users_data.items():
    if not username or not isinstance(data, dict):
      continue
    try:
      async with user_role_update_lock(username, timeout=30):
        await _sync_user_to_mongo_locked(username, data)
    except UserIdentityUpdateLockBusyError:
      logger.warning(f"Etcd sync: User {username} 正在更新，将由周期校准重试")
    except UserIdentityUpdateLockLostError as error:
      logger.warning(f"Etcd sync: User {username} 同步锁已丢失: {error}")


async def refresh_user_identity_from_etcd(username: str, client=None):
  """Pull one authoritative identity while the caller holds its user lock."""
  from src.core import etcd_op
  from src.modules.auth import crud as user_crud

  users_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_USERS, client=client)
  entry = users_data.get(username)
  if isinstance(entry, dict):
    return await _sync_user_to_mongo_locked(username, entry)
  return await user_crud.read_user_by_username(username)


async def publish_roles(client=None) -> None:
  from src.core import etcd_op
  from src.modules.auth.model import Role

  roles = await Role.find_all().to_list()
  entries = {role.name: role_to_etcd_entry(role) for role in roles}

  def mutator(data: dict) -> dict:
    for role_name, entry in entries.items():
      if _prefer_authority_entry(data.get(role_name), entry):
        data[role_name] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_ROLES, mutator, client=client)


async def publish_user(
  user,
  client=None,
  *,
  fields: set[str] | None = None,
) -> dict | None:
  from src.core import etcd_op
  from src.modules.auth import crud as user_crud

  refreshed = await user_crud.read_user_by_id(user.id)
  if not refreshed or getattr(refreshed, "is_sync", False):
    return None
  username = refreshed.username
  entry = user_to_etcd_entry(refreshed)

  def mutator(data: dict) -> dict:
    return _merge_user_entry(data, username, entry, fields=fields)

  users_data = await etcd_op.merge_update_etcd_key(
    ETCD_KEY_USERS,
    mutator,
    client=client,
  )
  return users_data.get(username)


async def publish_local_users(client=None) -> None:
  from src.modules.auth import crud as user_crud

  users = await user_crud.list_local_users()
  for user in users:
    try:
      async with user_role_update_lock(
        user.username,
        client=client,
        timeout=0,
      ) as guard:
        if guard is not None:
          await guard.ensure_owned()
        await publish_user(user, client=client)
    except UserIdentityUpdateLockBusyError:
      continue
    except (LastSuperadminError, UserIdentityUpdateLockLostError) as error:
      # A crashed identity update can leave Mongo stale while Etcd retains the
      # authoritative fields. The following pull repairs this node.
      logger.warning(
        f"跳过本地 User {user.username} 的身份发布，将由 Etcd 回拉修复: {error}"
      )


def application_to_etcd_entry(app) -> dict:
  author = app.author
  approver = app.approver
  return {
    "shown_name": app.shown_name,
    "description": app.description,
    "enabled": app.enabled,
    "provisioning_status": app.provisioning_status,
    "provisioning_error": app.provisioning_error,
    "provisioning_updated_at": (
      app.provisioning_updated_at.isoformat()
      if app.provisioning_updated_at else None
    ),
    "quota_bytes": int(getattr(
      app,
      "quota_bytes",
      DEFAULT_APPLICATION_QUOTA_BYTES,
    )),
    "author_username": author.username if author else "",
    "author_name": author.name if author else "",
    "approver_username": approver.username if approver else "",
    "enabled_at": app.enabled_at.isoformat() if app.enabled_at else None,
    "updated_at": app.updated_at.isoformat() if app.updated_at else None,
    "origin_region": settings.REGION,
  }


async def backfill_application_quotas(client=None) -> int:
  """Idempotently add the deterministic default to legacy Etcd APP entries."""

  from src.core import etcd_op

  current = await etcd_op.pull_from_etcd_by_key(
    ETCD_KEY_APPLICATIONS,
    client=client,
  )

  def needs_backfill(entry: Any) -> bool:
    if not isinstance(entry, dict):
      return False
    try:
      return int(entry.get("quota_bytes")) <= 0
    except (TypeError, ValueError):
      return True

  pending = sum(needs_backfill(entry) for entry in current.values())
  if not pending:
    return 0

  def mutator(data: dict) -> dict:
    for entry in data.values():
      if needs_backfill(entry):
        entry["quota_bytes"] = DEFAULT_APPLICATION_QUOTA_BYTES
    return data

  await etcd_op.merge_update_etcd_key(
    ETCD_KEY_APPLICATIONS,
    mutator,
    client=client,
  )
  logger.info(f"Etcd migration: 已为 {pending} 个历史应用补齐默认存储配额")
  return pending


def api_key_to_etcd_entry(api_key_obj) -> dict:
  from src.core.crypto import is_sha256_hex

  app = api_key_obj.application
  author_username = ""
  if app and app.author:
    author_username = app.author.username
  if getattr(api_key_obj, "key_enc", None):
    key_enc = api_key_obj.key_enc
    if not key_enc.startswith("enc:v1:"):
      key_enc = encrypt_secret(key_enc)
  elif not is_sha256_hex(api_key_obj.key):
    key_enc = encrypt_secret(api_key_obj.key)
  else:
    raise ValueError("API Key 缺少 key_enc，无法同步到 Etcd")
  return {
    "key_enc": key_enc,
    "app_name": app.name if app else "",
    "author_username": author_username,
    "expired_at": api_key_obj.expired_at.isoformat(),
    "deleted": api_key_obj.deleted,
    "deleted_at": api_key_obj.deleted_at.isoformat() if api_key_obj.deleted_at else None,
    "destory_by_admin": bool(getattr(api_key_obj, "destory_by_admin", False)),
    "origin_region": settings.REGION,
  }


def _resolve_api_key_plaintext(map_key: str, data: dict) -> str | None:
  """兼容新旧格式：新格式用 key_enc；旧格式 map_key 即为明文 Key。"""
  if data.get("key_enc"):
    try:
      return decrypt_secret(data["key_enc"])
    except ValueError as e:
      logger.warning(f"API Key 解密失败: {e}")
      return None
  if len(map_key) >= 16 and "app_name" in data:
    return map_key
  return None


async def get_or_create_sync_user(username: str, name: str = "") -> Any:
  """
  获取或创建跨节点同步占位用户。
  使用随机不可猜密码，并标记 is_sync=True 禁止登录。
  """
  from src.modules.auth import crud as user_crud
  from src.core.auth import get_password_hash

  if not username:
    username = SYNC_USER_PLACEHOLDER
  async with user_role_update_lock(username, timeout=30):
    user = await user_crud.read_user_by_username(username)
    if user:
      return user
    basic_role = await user_crud.get_basic_role()
    roles = [basic_role] if basic_role else []
    return await user_crud.create_user(
      username=username,
      name=name or username,
      hashed_password=get_password_hash(secrets.token_urlsafe(48)),
      roles=roles,
      is_sync=True,
    )


async def upsert_application_from_etcd(app_name: str, data: dict):
  from src.modules.public import crud as public_crud
  from src.modules.public.model import Application
  from pymongo.errors import DuplicateKeyError

  author_username = data.get("author_username") or SYNC_USER_PLACEHOLDER
  author = await get_or_create_sync_user(author_username, data.get("author_name", ""))

  app_obj = await public_crud.read_application_by_name(app_name)
  enabled_at = None
  if data.get("enabled_at"):
    try:
      enabled_at = datetime.fromisoformat(data["enabled_at"])
    except ValueError:
      pass

  approver = None
  if data.get("approver_username"):
    approver = await get_or_create_sync_user(data["approver_username"])

  enabled = bool(data.get("enabled", False))
  provisioning_status = data.get("provisioning_status") or (
    "ready" if enabled else "pending"
  )
  provisioning_updated_at = _parse_sync_datetime(
    data.get("provisioning_updated_at")
  )
  try:
    quota_bytes = int(data.get("quota_bytes", DEFAULT_APPLICATION_QUOTA_BYTES))
  except (TypeError, ValueError):
    quota_bytes = DEFAULT_APPLICATION_QUOTA_BYTES
  if quota_bytes <= 0:
    quota_bytes = DEFAULT_APPLICATION_QUOTA_BYTES

  if not app_obj:
    candidate = Application(
      name=app_name,
      shown_name=data.get("shown_name", app_name),
      description=data.get("description", ""),
      enabled=enabled,
      author=author,
      approver=approver,
      enabled_at=enabled_at if enabled else None,
      provisioning_status=provisioning_status,
      provisioning_error=data.get("provisioning_error", ""),
      provisioning_updated_at=provisioning_updated_at,
      quota_bytes=quota_bytes,
    )
    try:
      await candidate.save()
    except DuplicateKeyError:
      # A watch event and the request that won the Etcd claim may project the
      # same APP concurrently. Re-read the winner and converge below.
      app_obj = await public_crud.read_application_by_name(app_name)
      if not app_obj:
        raise
    else:
      logger.info(f"Etcd sync: 创建 Application {app_name}")
      return candidate, True

  changed = False
  if app_obj.shown_name != data.get("shown_name", app_obj.shown_name):
    app_obj.shown_name = data.get("shown_name", app_obj.shown_name)
    changed = True
  if app_obj.description != data.get("description", app_obj.description):
    app_obj.description = data.get("description", app_obj.description)
    changed = True
  if _linked_document_id(app_obj.author) != _linked_document_id(author):
    app_obj.author = author
    changed = True
  if _linked_document_id(app_obj.approver) != _linked_document_id(approver):
    app_obj.approver = approver
    changed = True
  if app_obj.enabled != enabled:
    app_obj.enabled = enabled
    changed = True
  incoming_enabled_at = enabled_at if enabled else None
  if not _same_sync_datetime(app_obj.enabled_at, incoming_enabled_at):
    app_obj.enabled_at = incoming_enabled_at
    changed = True
  if app_obj.provisioning_status != provisioning_status:
    app_obj.provisioning_status = provisioning_status
    changed = True
  incoming_error = data.get("provisioning_error", "")
  if app_obj.provisioning_error != incoming_error:
    app_obj.provisioning_error = incoming_error
    changed = True
  if not _same_sync_datetime(
    app_obj.provisioning_updated_at,
    provisioning_updated_at,
  ):
    app_obj.provisioning_updated_at = provisioning_updated_at
    changed = True
  if getattr(app_obj, "quota_bytes", DEFAULT_APPLICATION_QUOTA_BYTES) != quota_bytes:
    app_obj.quota_bytes = quota_bytes
    changed = True
  if changed:
    app_obj.updated_at = utc_now()
    await app_obj.save()
    logger.info(f"Etcd sync: 更新 Application {app_name}")
  return app_obj, changed


async def upsert_api_key_from_etcd(key: str, data: dict):
  from src.modules.public import crud as public_crud

  app_name = data.get("app_name")
  if not app_name or not key:
    return None

  app_obj = await public_crud.read_application_by_name(app_name)
  if not app_obj:
    app_obj, _ = await upsert_application_from_etcd(app_name, {
      "shown_name": app_name,
      "description": "",
      "enabled": True,
      "provisioning_status": "ready",
      "author_username": data.get("author_username", SYNC_USER_PLACEHOLDER),
    })
  if not app_obj.enabled:
    app_obj.enabled = True
    app_obj.enabled_at = utc_now()
    app_obj.provisioning_status = "ready"
    app_obj.provisioning_error = ""
    app_obj.provisioning_updated_at = utc_now()
    await app_obj.save()

  expired_at = datetime.fromisoformat(data["expired_at"]) if data.get("expired_at") else utc_now()
  deleted = data.get("deleted", False)

  existing = await public_crud.read_api_key_by_key_including_deleted(key)
  if existing:
    if deleted and not existing.deleted:
      existing.deleted = True
      existing.deleted_at = utc_now()
      existing.destory_by_admin = bool(data.get("destory_by_admin", False))
      await existing.save()
      logger.info(f"Etcd sync: 吊销 API Key {key[:8]}...")
    elif deleted and existing.deleted:
      flag = bool(data.get("destory_by_admin", False))
      if getattr(existing, "destory_by_admin", False) != flag:
        existing.destory_by_admin = flag
        await existing.save()
    return existing

  if deleted:
    return None

  api_key_obj = await public_crud.create_api_key(app_obj, key, expired_at)
  logger.info(f"Etcd sync: 创建 API Key {key[:8]}... (app={app_name})")
  return api_key_obj


async def sync_applications_to_mongo(applications_data: dict):
  for app_name, app_data in applications_data.items():
    try:
      await upsert_application_from_etcd(app_name, app_data)
    except Exception as e:
      logger.warning(f"同步 Application {app_name} 失败: {e}")


async def sync_api_keys_to_mongo(api_keys_data: dict):
  for map_key, key_data in api_keys_data.items():
    try:
      plain_key = _resolve_api_key_plaintext(map_key, key_data)
      if not plain_key:
        continue
      await upsert_api_key_from_etcd(plain_key, key_data)
    except Exception as e:
      logger.warning(f"同步 API Key 失败: {e}")


async def sync_region_to_mongo(region_data: dict):
  from src.modules.public import crud as public_crud
  from src.modules.public.model import Region

  for region_value, region_name in region_data.items():
    region_obj = await public_crud.read_region_by_name(region_value)
    if not region_obj:
      await public_crud.create_region(region_value, region_name)
      logger.info(f"Etcd sync: 创建 Region {region_value}")
    elif region_obj.shown_name != region_name:
      region_obj.shown_name = region_name
      await region_obj.save()
      logger.info(f"Etcd sync: 更新 Region {region_value} shown_name")

  # 收敛：Etcd 中已移除的远程 Region（保留本节点 REGION）
  known = set(region_data.keys())
  for region_obj in await Region.find_all().to_list():
    if region_obj.name == settings.REGION:
      continue
    if region_obj.name not in known:
      await region_obj.delete()
      logger.info(f"Etcd sync: 移除已下线 Region {region_obj.name}")


async def sync_servers_to_mongo(servers_data: dict) -> list[str]:
  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud

  new_servers: list[str] = []
  for server_region_name, raw in servers_data.items():
    try:
      server_data = decrypt_server_entry(raw)
    except ValueError as e:
      logger.warning(f"解密 server {server_region_name} 失败: {e}")
      continue
    if any(k not in server_data for k in (
      "host", "server_port", "minio_port", "access_key", "secret_key", "replicate_weight"
    )):
      continue
    region_obj = await public_crud.read_region_by_name(server_region_name)
    if not region_obj:
      region_obj = await public_crud.create_region(server_region_name, server_region_name)
    server_obj = await storage_crud.read_minio_server_by_region(region_obj)
    host = server_data.get("host", settings.SERVER_HOST)
    if not server_obj:
      await storage_crud.create_minio_server(
        region=region_obj,
        name=server_region_name,
        host=host,
        server_port=server_data["server_port"],
        minio_port=server_data["minio_port"],
        access_key=server_data["access_key"],
        secret_key=server_data["secret_key"],
        replicate_weight=server_data["replicate_weight"],
      )
      new_servers.append(server_region_name)
      logger.info(f"Etcd sync: 创建 MinIO 服务器 {server_region_name}")
    else:
      await storage_crud.update_minio_server(
        minio_server=server_obj,
        host=host,
        server_port=server_data["server_port"],
        minio_port=server_data["minio_port"],
        access_key=server_data["access_key"],
        secret_key=server_data["secret_key"],
        replicate_weight=server_data["replicate_weight"],
      )

  # 收敛：移除 Etcd 中已不存在的远程 MinIO（保留本节点）
  known = set(servers_data.keys())
  for server_obj in await storage_crud.read_minio_server_list():
    region = server_obj.region
    region_name = region.name if region and hasattr(region, "name") else server_obj.name
    if region_name == settings.REGION:
      continue
    if region_name not in known:
      await server_obj.delete()
      logger.info(f"Etcd sync: 移除已下线 MinIO 服务器 {region_name}")

  return new_servers


def _topology_entry_timestamp() -> str:
  return utc_now().isoformat()


def _topology_layout_initialized(layout: dict) -> bool:
  meta = layout.get("_meta") if isinstance(layout, dict) else None
  return bool(
    isinstance(meta, dict)
    and meta.get("initialized") is True
    and meta.get("schema_version") == TOPOLOGY_LAYOUT_SCHEMA_VERSION
    and meta.get("authority_region") == settings.SYNC_AUTHORITY_REGION
  )


async def build_topology_layout_snapshot() -> dict:
  """Build UI layout metadata only; MinIO replication rules are not copied."""
  from src.modules.graph.model import BucketEdgePosition, BucketNodePosition

  now = _topology_entry_timestamp()
  nodes: dict[str, dict[str, dict]] = {}
  for item in await BucketNodePosition.find_all().to_list():
    nodes.setdefault(item.bucket, {})[item.server] = {
      "position_x": int(item.position_x),
      "position_y": int(item.position_y),
      "updated_at": now,
      "origin_region": settings.REGION,
    }

  edges: dict[str, dict[str, dict[str, dict]]] = {}
  for item in await BucketEdgePosition.find_all().to_list():
    edges.setdefault(item.bucket, {}).setdefault(item.from_server, {})[
      item.to_server
    ] = {
      "from_position": item.from_position,
      "to_position": item.to_position,
      "updated_at": now,
      "origin_region": settings.REGION,
    }

  return {
    "_meta": {
      "initialized": True,
      "schema_version": TOPOLOGY_LAYOUT_SCHEMA_VERSION,
      "authority_region": settings.SYNC_AUTHORITY_REGION,
      "initialized_at": now,
    },
    "nodes": nodes,
    "edges": edges,
  }


async def bootstrap_topology_layout(client=None) -> bool:
  """Initialize topology layout exactly once, and only from the authority region."""
  if settings.REGION != settings.SYNC_AUTHORITY_REGION:
    return False

  from src.core import etcd_op

  current = await etcd_op.pull_from_etcd_by_key(
    ETCD_KEY_TOPOLOGY_LAYOUT,
    client=client,
  )
  if _topology_layout_initialized(current):
    return False
  snapshot = await build_topology_layout_snapshot()

  def mutator(data: dict) -> dict:
    if _topology_layout_initialized(data):
      return data
    return snapshot

  await etcd_op.merge_update_etcd_key(
    ETCD_KEY_TOPOLOGY_LAYOUT,
    mutator,
    client=client,
  )
  logger.info(
    f"Etcd sync: 已由 {settings.REGION} 初始化拓扑布局权威快照"
  )
  return True


def _validated_topology_records(
  layout: dict,
) -> tuple[dict[tuple[str, str], dict], dict[tuple[str, str, str], dict]]:
  if not _topology_layout_initialized(layout):
    raise ValueError("拓扑布局尚未由权威区域初始化")
  nodes_data = layout.get("nodes")
  edges_data = layout.get("edges")
  if not isinstance(nodes_data, dict) or not isinstance(edges_data, dict):
    raise ValueError("拓扑布局 nodes/edges 格式错误")

  nodes: dict[tuple[str, str], dict] = {}
  for bucket, server_map in nodes_data.items():
    if not isinstance(bucket, str) or not isinstance(server_map, dict):
      raise ValueError("拓扑节点层级格式错误")
    for server, entry in server_map.items():
      if not isinstance(server, str) or not isinstance(entry, dict):
        raise ValueError("拓扑节点条目格式错误")
      position_x = entry.get("position_x")
      position_y = entry.get("position_y")
      if isinstance(position_x, bool) or not isinstance(position_x, int):
        raise ValueError("拓扑节点 position_x 格式错误")
      if isinstance(position_y, bool) or not isinstance(position_y, int):
        raise ValueError("拓扑节点 position_y 格式错误")
      nodes[(bucket, server)] = {
        "position_x": position_x,
        "position_y": position_y,
      }

  valid_positions = {"up", "down", "left", "right"}
  edges: dict[tuple[str, str, str], dict] = {}
  for bucket, from_map in edges_data.items():
    if not isinstance(bucket, str) or not isinstance(from_map, dict):
      raise ValueError("拓扑连线层级格式错误")
    for from_server, to_map in from_map.items():
      if not isinstance(from_server, str) or not isinstance(to_map, dict):
        raise ValueError("拓扑连线源节点格式错误")
      for to_server, entry in to_map.items():
        if not isinstance(to_server, str) or not isinstance(entry, dict):
          raise ValueError("拓扑连线条目格式错误")
        from_position = entry.get("from_position")
        to_position = entry.get("to_position")
        if from_position not in valid_positions or to_position not in valid_positions:
          raise ValueError("拓扑连线端点格式错误")
        edges[(bucket, from_server, to_server)] = {
          "from_position": from_position,
          "to_position": to_position,
        }
  return nodes, edges


async def sync_topology_layout_to_mongo(layout: dict) -> None:
  """Converge Mongo UI layout to Etcd without touching MinIO replication rules."""
  from src.modules.graph import crud as graph_crud
  from src.modules.graph.model import BucketEdgePosition, BucketNodePosition

  desired_nodes, desired_edges = _validated_topology_records(layout)
  for (bucket, server), entry in desired_nodes.items():
    await graph_crud.update_bucket_node_position(
      bucket,
      server,
      entry["position_x"],
      entry["position_y"],
    )
  for item in await BucketNodePosition.find_all().to_list():
    if (item.bucket, item.server) not in desired_nodes:
      await item.delete()

  for (bucket, from_server, to_server), entry in desired_edges.items():
    await graph_crud.update_bucket_edge_position(
      bucket,
      from_server,
      to_server,
      entry["from_position"],
      entry["to_position"],
    )
  for item in await BucketEdgePosition.find_all().to_list():
    if (item.bucket, item.from_server, item.to_server) not in desired_edges:
      await item.delete()


def _require_topology_layout(layout: dict) -> None:
  if not _topology_layout_initialized(layout):
    raise RuntimeError(
      f"拓扑布局尚未由权威区域 {settings.SYNC_AUTHORITY_REGION} 初始化"
    )


async def publish_topology_node_position(
  bucket: str,
  server: str,
  position_x: int,
  position_y: int,
  client=None,
) -> None:
  from src.core import etcd_op

  entry = {
    "position_x": int(position_x),
    "position_y": int(position_y),
    "updated_at": _topology_entry_timestamp(),
    "origin_region": settings.REGION,
  }

  def mutator(layout: dict) -> dict:
    _require_topology_layout(layout)
    layout.setdefault("nodes", {}).setdefault(bucket, {})[server] = entry
    return layout

  await etcd_op.merge_update_etcd_key(
    ETCD_KEY_TOPOLOGY_LAYOUT,
    mutator,
    client=client,
  )


async def publish_topology_edge_position(
  bucket: str,
  from_server: str,
  to_server: str,
  from_position: str,
  to_position: str,
  client=None,
) -> None:
  from src.core import etcd_op

  entry = {
    "from_position": from_position,
    "to_position": to_position,
    "updated_at": _topology_entry_timestamp(),
    "origin_region": settings.REGION,
  }

  def mutator(layout: dict) -> dict:
    _require_topology_layout(layout)
    layout.setdefault("edges", {}).setdefault(bucket, {}).setdefault(
      from_server,
      {},
    )[to_server] = entry
    return layout

  await etcd_op.merge_update_etcd_key(
    ETCD_KEY_TOPOLOGY_LAYOUT,
    mutator,
    client=client,
  )


async def unpublish_topology_edge_position(
  bucket: str,
  from_server: str,
  to_server: str,
  client=None,
) -> None:
  from src.core import etcd_op

  def mutator(layout: dict) -> dict:
    _require_topology_layout(layout)
    bucket_edges = layout.setdefault("edges", {}).get(bucket, {})
    targets = bucket_edges.get(from_server, {})
    targets.pop(to_server, None)
    if not targets:
      bucket_edges.pop(from_server, None)
    if not bucket_edges:
      layout["edges"].pop(bucket, None)
    return layout

  await etcd_op.merge_update_etcd_key(
    ETCD_KEY_TOPOLOGY_LAYOUT,
    mutator,
    client=client,
  )


async def unpublish_topology_server(server: str, client=None) -> None:
  """Remove layout metadata for an offline server, not MinIO data or rules."""
  from src.core import etcd_op

  def mutator(layout: dict) -> dict:
    _require_topology_layout(layout)
    nodes = layout.setdefault("nodes", {})
    for bucket in list(nodes):
      nodes[bucket].pop(server, None)
      if not nodes[bucket]:
        nodes.pop(bucket, None)
    edges = layout.setdefault("edges", {})
    for bucket in list(edges):
      edges[bucket].pop(server, None)
      for from_server in list(edges[bucket]):
        edges[bucket][from_server].pop(server, None)
        if not edges[bucket][from_server]:
          edges[bucket].pop(from_server, None)
      if not edges[bucket]:
        edges.pop(bucket, None)
    return layout

  await etcd_op.merge_update_etcd_key(
    ETCD_KEY_TOPOLOGY_LAYOUT,
    mutator,
    client=client,
  )


async def unpublish_region(region_name: str) -> None:
  """从 Etcd region map 移除区域（跨节点下线）。"""
  from src.core import etcd_op

  def mutator(data: dict) -> dict:
    data.pop(region_name, None)
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_REGION, mutator)


async def unpublish_server(region_name: str) -> None:
  """从 Etcd servers map 移除服务点。"""
  from src.core import etcd_op

  def mutator(data: dict) -> dict:
    data.pop(region_name, None)
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_SERVERS, mutator)


async def setup_mc_aliases(servers_data: dict):
  from src.core import minio_op

  for server_region_name, raw in servers_data.items():
    try:
      server_data = decrypt_server_entry(raw)
    except ValueError:
      continue
    if any(k not in server_data for k in ("host", "minio_port", "access_key", "secret_key")):
      continue
    host = server_data.get("host", settings.SERVER_HOST)
    success, res = await minio_op.set_site_alias(
      site_name=server_region_name,
      endpoint=f"{host}:{server_data['minio_port']}",
      admin_user=server_data["access_key"],
      admin_password=server_data["secret_key"],
    )
    if not success:
      logger.warning(f"mc alias 设置失败 {server_region_name}: {res}")


class ReplicationPolicyError(RuntimeError):
  def __init__(self, message: str, policy: dict | None = None):
    super().__init__(message)
    self.policy = policy or {}


class ReplicationLockBusyError(RuntimeError):
  pass


@asynccontextmanager
async def user_role_update_lock(
  username: str,
  *,
  client=None,
  timeout: int = 0,
):
  """Serialize every password/role write for one user across regions."""
  from src.core import etcd_op

  own_client = client is None
  if own_client:
    client = await etcd_op.get_etcd_client()
  ttl = 30
  lock = client.lock(
    f"/storagent/locks/user-role-update/{username}".encode(),
    ttl=ttl,
  )
  acquired = False
  refresh_job = None
  owner_task = asyncio.current_task()
  lease_failure: BaseException | None = None

  class Guard:
    async def ensure_owned(self) -> None:
      nonlocal lease_failure
      if lease_failure is not None:
        raise UserIdentityUpdateLockLostError(
          f"用户 {username} 的身份更新锁续租失败"
        ) from lease_failure
      checker = getattr(lock, "is_acquired", None)
      if checker is None:
        return
      try:
        still_owned = await checker()
      except asyncio.CancelledError:
        raise
      except BaseException as error:
        lease_failure = error
        raise UserIdentityUpdateLockLostError(
          f"用户 {username} 的身份更新锁状态无法确认"
        ) from error
      if not still_owned:
        lease_failure = RuntimeError("Etcd lock ownership lost")
        raise UserIdentityUpdateLockLostError(
          f"用户 {username} 的身份更新锁已丢失"
        ) from lease_failure

  guard = Guard()

  async def refresh_lock():
    nonlocal lease_failure
    try:
      while True:
        await asyncio.sleep(ttl / 3)
        await lock.refresh()
        await guard.ensure_owned()
    except asyncio.CancelledError:
      raise
    except BaseException as error:
      lease_failure = error
      if owner_task is not None and not owner_task.done():
        owner_task.cancel()

  try:
    acquired = await lock.acquire(timeout=timeout)
    if not acquired:
      raise UserIdentityUpdateLockBusyError(
        f"用户 {username} 的身份信息正在由其他节点更新"
      )
    refresh_job = asyncio.create_task(refresh_lock())
    try:
      yield guard
      await guard.ensure_owned()
    except asyncio.CancelledError as error:
      if lease_failure is not None:
        raise UserIdentityUpdateLockLostError(
          f"用户 {username} 的身份更新锁续租失败"
        ) from lease_failure
      raise error
  finally:
    if refresh_job is not None:
      refresh_job.cancel()
      try:
        await refresh_job
      except asyncio.CancelledError:
        pass
      except Exception as error:
        logger.warning(f"用户身份锁续租任务异常 username={username}: {error}")
    if acquired:
      try:
        await lock.release()
      except Exception as error:
        logger.warning(f"用户身份锁释放失败 username={username}: {error}")
    if own_client:
      try:
        await client.close()
      except Exception as error:
        logger.warning(f"用户身份锁 Etcd 连接关闭失败 username={username}: {error}")


@asynccontextmanager
async def application_replication_lock(
  application_name: str,
  *,
  client=None,
  timeout: int | None = None,
):
  """Serialize full-mesh provisioning across all Storagent regions."""
  from src.core import etcd_op

  own_client = client is None
  if own_client:
    client = await etcd_op.get_etcd_client()
  ttl = max(int(settings.REPLICATION_LOCK_TTL_SECONDS), 30)
  wait_timeout = (
    int(settings.REPLICATION_LOCK_TIMEOUT_SECONDS)
    if timeout is None else timeout
  )
  lock = client.lock(
    f"/storagent/locks/application-replication/{application_name}".encode(),
    ttl=ttl,
  )
  acquired = False
  refresh_job = None

  async def refresh_lock():
    while True:
      await asyncio.sleep(max(ttl / 3, 5))
      await lock.refresh()

  try:
    acquired = await lock.acquire(timeout=wait_timeout)
    if not acquired:
      raise ReplicationLockBusyError(
        f"应用 {application_name} 的复制策略正在由其他节点配置"
      )
    refresh_job = asyncio.create_task(refresh_lock())
    yield
  finally:
    if refresh_job is not None:
      refresh_job.cancel()
      try:
        await refresh_job
      except asyncio.CancelledError:
        pass
      except Exception as e:
        logger.warning(f"复制策略锁续租任务异常: {e}")
    if acquired:
      try:
        await lock.release()
      except Exception as e:
        logger.warning(f"复制策略锁释放失败 {application_name}: {e}")
    if own_client:
      await client.close()


def replication_priority(
  server_names: list[str],
  from_server: str,
  to_server: str,
) -> int:
  targets = [name for name in sorted(set(server_names)) if name != from_server]
  return targets.index(to_server) + 1


async def setup_bucket_replication(
  bucket_name: str,
  server_names: list[str] | None = None,
  *,
  readback_attempts: int = 5,
  readback_delay: float = 0.5,
) -> dict:
  """Idempotently create and strictly verify an N x (N-1) full mesh."""
  from src.core import minio_op
  from src.modules.storage import crud as storage_crud
  from src.modules.storage import service as storage_service

  if server_names is None:
    server_names = await storage_crud.read_minio_server_names()
  server_names = sorted(set(server_names))
  initial = await storage_service.get_bucket_replicate_infos(bucket_name)
  initial_policy = initial["policy"]
  if (
    initial_policy.get("read_errors")
    or initial_policy.get("unmapped_rule_count", 0)
  ):
    raise ReplicationPolicyError(
      f"无法安全识别存储桶 {bucket_name} 的现有复制规则",
      initial_policy,
    )

  existing_pairs = {
    (rule.get("from"), rule.get("to"))
    for rule in initial.get("replicates", [])
  }
  failures: dict[str, str] = {}

  async def create_missing_for_source(from_server: str):
    for to_server in server_names:
      if from_server == to_server or (from_server, to_server) in existing_pairs:
        continue
      success, err = await minio_op.create_bucket_replicate(
        from_server,
        to_server,
        bucket_name,
        priority=replication_priority(server_names, from_server, to_server),
        enabled=True,
        replicate_options=[
          "delete",
          "delete-marker",
          "existing-objects",
          "metadata-sync",
        ],
      )
      pair = f"{from_server}->{to_server}"
      if success:
        logger.info(f"Bucket Replication: {from_server}/{bucket_name} -> {to_server}")
      else:
        failures[pair] = str(err)
        logger.warning(f"Bucket Replication 失败 {pair}/{bucket_name}: {err}")

  await asyncio.gather(*(
    create_missing_for_source(from_server)
    for from_server in server_names
  ))
  if failures:
    detail = "; ".join(f"{pair}: {error}" for pair, error in sorted(failures.items()))
    raise ReplicationPolicyError(f"全连接复制规则创建失败: {detail}", initial_policy)

  latest = initial
  attempts = max(readback_attempts, 1)
  for attempt in range(attempts):
    latest = await storage_service.get_bucket_replicate_infos(bucket_name)
    if latest["policy"].get("complete"):
      return latest["policy"]
    if attempt + 1 < attempts and readback_delay > 0:
      await asyncio.sleep(readback_delay)

  policy = latest["policy"]
  raise ReplicationPolicyError(
    (
      f"全连接复制策略验收失败: "
      f"规则 {policy.get('actual_rule_count', 0)}/"
      f"{policy.get('expected_rule_count', 0)}, "
      f"健康 {policy.get('healthy_rule_count', 0)}"
    ),
    policy,
  )


async def ensure_bucket_quotas(
  bucket_name: str,
  quota_bytes: int,
  server_names: list[str],
) -> None:
  """Idempotently enforce the same hard quota on every regional bucket."""
  from src.core import minio_op

  names = sorted(set(server_names))
  results = await asyncio.gather(*(
    minio_op.ensure_bucket_hard_quota(
      server_name,
      bucket_name,
      quota_bytes,
      timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
    )
    for server_name in names
  ))
  failures = {
    server_name: error
    for server_name, (success, error) in zip(names, results)
    if not success
  }
  if failures:
    detail = "; ".join(
      f"{server_name}: {error}" for server_name, error in failures.items()
    )
    raise RuntimeError(f"存储桶配额设置失败: {detail}")


async def reconcile_replication_policies_task():
  """Authority-region loop that repairs missing rules for enabled apps."""
  if settings.REGION != settings.SYNC_AUTHORITY_REGION:
    logger.info("非权威区域不执行复制策略校准")
    return

  from src.modules.public import crud as public_crud
  from src.modules.public import service as public_service
  from src.modules.files import quota as files_quota
  from src.modules.storage import crud as storage_crud

  interval = max(float(settings.REPLICATION_RECONCILE_INTERVAL_SECONDS), 30.0)
  while True:
    try:
      applications = await public_crud.read_application_list()
      server_names = await storage_crud.read_minio_server_names()
      for app in applications:
        if not app.enabled:
          continue
        try:
          async with application_replication_lock(app.name, timeout=0):
            reconcile_error = None
            try:
              await setup_bucket_replication(app.name, server_names)
            except Exception as error:
              reconcile_error = error

            authoritative_quota = None
            try:
              async with files_quota.application_quota_lock(app.name) as quota_client:
                authoritative_quota = await public_service.get_application_quota_limit(
                  app.name,
                  client=quota_client,
                )
                if reconcile_error is None:
                  await ensure_bucket_quotas(
                    app.name,
                    authoritative_quota,
                    server_names,
                  )
            except Exception as error:
              # Without the authoritative value, saving the stale object loaded
              # before the locks could overwrite a newer cross-region quota.
              if authoritative_quota is None:
                raise
              reconcile_error = error

            quota_changed = int(getattr(
              app,
              "quota_bytes",
              DEFAULT_APPLICATION_QUOTA_BYTES,
            )) != authoritative_quota
            app.quota_bytes = authoritative_quota
            if reconcile_error is None:
              if (
                app.provisioning_status != "ready"
                or app.provisioning_error
                or quota_changed
              ):
                app.provisioning_status = "ready"
                app.provisioning_error = ""
                app.provisioning_updated_at = utc_now()
                app.updated_at = utc_now()
                await app.save()
                await publish_application(app)
            else:
              app.provisioning_status = "degraded"
              app.provisioning_error = str(reconcile_error)
              app.provisioning_updated_at = utc_now()
              app.updated_at = utc_now()
              await app.save()
              try:
                await publish_application(app)
              except Exception as publish_error:
                logger.warning(f"复制策略异常状态同步失败 {app.name}: {publish_error}")
              logger.warning(f"复制策略校准失败 {app.name}: {reconcile_error}")
        except ReplicationLockBusyError:
          continue
        except Exception as e:
          logger.warning(f"复制策略校准失败 {app.name}: {e}")
    except asyncio.CancelledError:
      logger.info("复制策略周期校准已停止")
      raise
    except Exception as e:
      logger.warning(f"复制策略周期校准失败: {e}")
    await asyncio.sleep(interval)


async def ensure_local_buckets_for_app(app_name: str):
  from src.core import minio_op
  from src.modules.storage import crud as storage_crud

  local_server = await storage_crud.read_minio_server_by_region_name(settings.REGION)
  if not local_server:
    return
  server_name = settings.REGION
  existed = await minio_op.check_server_bucket_existed(server_name, app_name)
  if not existed:
    success, err = await minio_op.create_bucket(server_name, app_name)
    if not success:
      logger.warning(f"本地建桶失败 {app_name}: {err}")
      return
  await minio_op.enable_bucket_versioning(server_name, app_name)
  from src.modules.public import crud as public_crud
  app = await public_crud.read_application_by_name(app_name)
  quota_bytes = int(getattr(app, "quota_bytes", DEFAULT_APPLICATION_QUOTA_BYTES))
  success, error = await minio_op.ensure_bucket_hard_quota(
    server_name,
    app_name,
    quota_bytes,
    timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
  )
  if not success:
    logger.warning(f"本地存储桶配额设置失败 {server_name}/{app_name}: {error}")


async def publish_application(app) -> None:
  from src.core import etcd_op
  from src.modules.public import crud as public_crud

  app = await public_crud.read_application_by_id(app.id)
  if not app:
    return
  entry = application_to_etcd_entry(app)
  name = app.name

  def mutator(data: dict) -> dict:
    current = data.get(name)
    if not isinstance(current, dict):
      data[name] = entry
      return data

    # Lifecycle writers may run from a delayed local projection. Preserve the
    # Etcd-owned identity and quota fields while publishing only authorization
    # and replication-operational state.
    for field in (
      "enabled",
      "provisioning_status",
      "provisioning_error",
      "provisioning_updated_at",
      "approver_username",
      "enabled_at",
      "updated_at",
    ):
      current[field] = entry[field]
    for field in (
      "shown_name",
      "description",
      "quota_bytes",
      "author_username",
      "author_name",
      "origin_region",
    ):
      current.setdefault(field, entry[field])
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_APPLICATIONS, mutator)


async def publish_api_key(api_key_obj) -> None:
  from src.core import etcd_op
  from src.core.crypto import api_key_etcd_map_key, is_sha256_hex
  from src.modules.public import crud as public_crud

  api_key_obj = await public_crud.read_api_key_by_id(api_key_obj.id)
  if not api_key_obj:
    return
  map_key = api_key_obj.key if is_sha256_hex(api_key_obj.key) else api_key_etcd_map_key(api_key_obj.key)
  entry = api_key_to_etcd_entry(api_key_obj)

  def mutator(data: dict) -> dict:
    data[map_key] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_API_KEYS, mutator)


async def publish_ai_config(config: dict) -> None:
  """Publish the encrypted AI provider configuration to every region."""
  from src.core import etcd_op

  def mutator(_current: dict) -> dict:
    return dict(config)

  await etcd_op.merge_update_etcd_key(ETCD_KEY_AI_CONFIG, mutator)


async def sync_ai_config_to_mongo(config: dict) -> None:
  if not isinstance(config, dict) or not config:
    return
  from src.modules.ai import crud as ai_crud

  await ai_crud.upsert_config(config)




async def publish_revoked_token(token_hash: str, expired_at: datetime) -> None:
  """跨区同步 JWT 吊销（仅同步哈希，不落明文 token）。"""
  from src.core import etcd_op

  entry = {
    "expired_at": expired_at.isoformat(),
    "origin_region": settings.REGION,
  }

  def mutator(data: dict) -> dict:
    data[token_hash] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_REVOKED_TOKENS, mutator)


async def sync_revoked_tokens_to_mongo(revoked_data: dict) -> None:
  from src.modules.auth.model import DestoryedToken

  for token_hash, meta in revoked_data.items():
    if not token_hash or not isinstance(meta, dict):
      continue
    existing = await DestoryedToken.find_one(DestoryedToken.token_hash == token_hash)
    if existing:
      continue
    expired_at = utc_now()
    raw_exp = meta.get("expired_at")
    if raw_exp:
      try:
        expired_at = datetime.fromisoformat(raw_exp)
      except ValueError:
        pass
    await DestoryedToken(token="", token_hash=token_hash, expired_at=expired_at).save()
    logger.info(f"Etcd sync: 导入吊销 token_hash {token_hash[:12]}...")


async def publish_region(region_name: str, shown_name: str) -> None:
  from src.core import etcd_op

  def mutator(data: dict) -> dict:
    data[region_name] = shown_name
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_REGION, mutator)


async def publish_servers() -> None:
  from src.core import etcd_op

  entry = encrypt_server_entry({
    "host": settings.SERVER_HOST,
    "server_port": settings.SERVER_PORT,
    "minio_port": settings.MINIO_PORT,
    "access_key": settings.MINIO_ACCESS_KEY,
    "secret_key": settings.MINIO_SECRET_KEY,
    "replicate_weight": settings.MINIO_REPLICATE_WEIGHT,
  })
  region = settings.REGION

  def mutator(data: dict) -> dict:
    data[region] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_SERVERS, mutator)


async def publish_server_entry(
  region_name: str,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int,
) -> None:
  from src.core import etcd_op

  entry = encrypt_server_entry({
    "host": host,
    "server_port": server_port,
    "minio_port": minio_port,
    "access_key": access_key,
    "secret_key": secret_key,
    "replicate_weight": replicate_weight,
  })

  def mutator(data: dict) -> dict:
    data[region_name] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_SERVERS, mutator)


async def pull_all_and_sync(client=None):
  from src.core import etcd_op

  own_client = client is None
  if own_client:
    client = await etcd_op.get_etcd_client()
  try:
    roles_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_ROLES, client=client)
    if roles_data:
      await sync_roles_to_mongo(roles_data)

    users_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_USERS, client=client)
    if users_data:
      await sync_users_to_mongo(users_data)

    region_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_REGION, client=client)
    if region_data:
      await sync_region_to_mongo(region_data)

    servers_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_SERVERS, client=client)
    if servers_data:
      await sync_servers_to_mongo(servers_data)
      await setup_mc_aliases(servers_data)

    topology_layout = await etcd_op.pull_from_etcd_by_key(
      ETCD_KEY_TOPOLOGY_LAYOUT,
      client=client,
    )
    if topology_layout:
      await sync_topology_layout_to_mongo(topology_layout)

    apps_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_APPLICATIONS, client=client)
    if apps_data:
      await sync_applications_to_mongo(apps_data)
      for app_name, app_data in apps_data.items():
        if app_data.get("enabled"):
          await ensure_local_buckets_for_app(app_name)

    keys_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_API_KEYS, client=client)
    if keys_data:
      await sync_api_keys_to_mongo(keys_data)

    revoked_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_REVOKED_TOKENS, client=client)
    if revoked_data:
      await sync_revoked_tokens_to_mongo(revoked_data)

    ai_config = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_AI_CONFIG, client=client)
    if ai_config:
      await sync_ai_config_to_mongo(ai_config)
  finally:
    if own_client:
      await client.close()
