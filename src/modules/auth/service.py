import asyncio
from datetime import datetime
from jose import JWTError, jwt
from datetime import timedelta
from src.modules.auth.model import User, DestoryedToken
from src.modules.auth import crud as user_crud
from src.core.exception import CustomException, ErrorDesc
from src.core.auth import (
  authenticate_user,
  create_token,
  get_password_hash,
  password_check,
  preset_admin_user,
)
from src.configs.configs import settings
from src.configs.consts import (
  ROLE_APPLICATION_ADMIN,
  ROLE_OPERATIONS_ADMIN,
  ROLE_SUPERADMIN,
  ROLE_USER,
  ROLE_USER_ADMIN,
)
from src.utils.helpers import (
  convert_utc_to_local_str,
  import_user_from_springboard,
  local_utc_now,
  utc_now,
)
from src.modules.auth import oa as oa_service


AUTH_PURPOSE_REGISTER = "register"
AUTH_PURPOSE_PASSWORD_RESET = "password_reset"
AUTH_PURPOSE_LOGIN = "login"
ROLE_DISPLAY_ORDER = (
  ROLE_USER,
  ROLE_APPLICATION_ADMIN,
  ROLE_OPERATIONS_ADMIN,
  ROLE_USER_ADMIN,
  ROLE_SUPERADMIN,
)


def _ordered_role_names(names) -> list[str]:
  unique = {str(name) for name in names if name}
  rank = {name: index for index, name in enumerate(ROLE_DISPLAY_ORDER)}
  return sorted(unique, key=lambda name: (rank.get(name, len(rank)), name))


def _role_sort_key(name: str) -> tuple[int, str]:
  rank = {role_name: index for index, role_name in enumerate(ROLE_DISPLAY_ORDER)}
  return rank.get(name, len(rank)), name


async def _ensure_user_lock_owned(guard) -> None:
  if guard is not None:
    await guard.ensure_owned()


# A short wait absorbs an expired Etcd lease or a just-finished update. A
# zero-time acquire was surfacing transient lock state as a false claim that
# another administrator was editing the user.
USER_IDENTITY_LOCK_ACQUIRE_TIMEOUT_SECONDS = 10


def _directory_display_name(user_info: dict | None, username: str) -> str:
  if not isinstance(user_info, dict):
    return username
  nested = user_info.get("user_info")
  if not isinstance(nested, dict):
    return username
  return str(nested.get("l") or nested.get("name") or username).strip() or username


async def _request_oa_challenge(
  *,
  username: str,
  purpose: str,
  title: str,
  content: str,
  password_hash: str = "",
  display_name: str = "",
) -> dict:
  from src.core import audit

  challenge, code = await user_crud.create_auth_challenge(
    username=username,
    purpose=purpose,
    password_hash=password_hash,
    display_name=display_name,
  )
  delivery = await oa_service.send_oa_auth_message(
    username,
    title,
    content,
    code,
  )
  if not delivery.accepted:
    await user_crud.delete_auth_challenge(challenge)
    audit.audit(
      "auth.oa_request",
      actor=username,
      resource=purpose,
      detail=delivery.detail,
      success=False,
    )
    raise CustomException(ErrorDesc.SEND_CODE_FAILED, "OA 消息发送失败，请稍后重试")

  await user_crud.set_auth_challenge_delivery(challenge, delivery.status)
  audit.audit(
    "auth.oa_request",
    actor=username,
    resource=purpose,
    detail={"delivery_status": delivery.status},
  )
  if delivery.status == "unknown":
    message = "发送结果暂未确认，请先检查 OA 消息；收到的链接仍然有效"
  else:
    message = "验证链接已发送至 OA，请在有效期内打开"
  return {
    "message": message,
    "expires_in_seconds": max(settings.OA_AUTH_CODE_EXPIRE_MINUTES, 1) * 60,
    "delivery_status": delivery.status,
  }


async def request_registration(username: str, password: str) -> dict:
  existing = await user_crud.read_user_by_username(username)
  if existing and not getattr(existing, "is_sync", False):
    raise CustomException(ErrorDesc.USER_ALREADY_REGISTERED, "请直接登录或使用忘记密码")
  if not password_check(password):
    raise CustomException(
      ErrorDesc.INVALID_PARAMS,
      "密码至少 8 位，并同时包含字母和数字",
    )

  user_info = await import_user_from_springboard(username)
  if user_info is None:
    raise CustomException(ErrorDesc.USER_NOT_FOUND, "OA 中不存在该 itcode")
  display_name = _directory_display_name(user_info, username)
  return await _request_oa_challenge(
    username=username,
    purpose=AUTH_PURPOSE_REGISTER,
    password_hash=get_password_hash(password),
    display_name=display_name,
    title="Storagent 注册确认",
    content="正在注册 Storagent。链接 15 分钟内有效；如非本人操作，请忽略。",
  )


async def request_password_reset(username: str, password: str) -> dict:
  user = await user_crud.read_user_by_username(username)
  if not user or getattr(user, "is_sync", False):
    raise CustomException(ErrorDesc.USER_NOT_FOUND, "用户尚未完成注册")
  if not password_check(password):
    raise CustomException(
      ErrorDesc.INVALID_PARAMS,
      "密码至少 8 位，并同时包含字母和数字",
    )
  return await _request_oa_challenge(
    username=username,
    purpose=AUTH_PURPOSE_PASSWORD_RESET,
    password_hash=get_password_hash(password),
    title="Storagent 密码重置确认",
    content="正在重置 Storagent 密码。链接 15 分钟内有效；如非本人操作，请忽略。",
  )


async def request_login_link(username: str) -> dict:
  user = await user_crud.read_user_by_username(username)
  if not user or getattr(user, "is_sync", False):
    raise CustomException(ErrorDesc.USER_NOT_FOUND, "用户尚未完成注册")
  return await _request_oa_challenge(
    username=username,
    purpose=AUTH_PURPOSE_LOGIN,
    title="Storagent 快捷登录",
    content="正在登录 Storagent。链接 15 分钟内有效且只能使用一次；如非本人操作，请忽略。",
  )


async def _complete_registration(challenge) -> User:
  from src.core import sync as sync_module

  try:
    async with sync_module.user_role_update_lock(
      challenge.username,
      timeout=USER_IDENTITY_LOCK_ACQUIRE_TIMEOUT_SECONDS,
    ) as guard:
      user = await sync_module.refresh_user_identity_from_etcd(challenge.username)
      if user and not getattr(user, "is_sync", False):
        return user
      if not challenge.password_hash:
        raise CustomException(ErrorDesc.AUTH_CODE_INVALID)

      basic_role = await user_crud.get_basic_role()
      admin_role = (
        await user_crud.get_admin_role()
        if preset_admin_user(challenge.username) else None
      )
      if basic_role is None or (
        preset_admin_user(challenge.username) and admin_role is None
      ):
        raise CustomException(ErrorDesc.STATUS_ERR, "系统角色尚未初始化")
      roles = [basic_role]
      if admin_role:
        roles.append(admin_role)

      previous = None
      created = user is None
      if user:
        previous = {
          "name": user.name,
          "hashed_password": user.hashed_password,
          "roles": list(user.roles or []),
          "permissions": list(getattr(user, "permissions", []) or []),
          "is_sync": bool(getattr(user, "is_sync", False)),
          "auth_version": int(getattr(user, "auth_version", 0)),
          "roles_version": int(getattr(user, "roles_version", 0)),
          "updated_at": getattr(user, "updated_at", None),
        }
      try:
        await _ensure_user_lock_owned(guard)
        if user:
          user.name = challenge.display_name or challenge.username
          user.hashed_password = challenge.password_hash
          user.roles = roles
          user.permissions = await user_crud.get_all_permissions(roles)
          user.is_sync = False
          user.roles_version = int(getattr(user, "roles_version", 0)) + 1
          user.updated_at = utc_now()
          await user.save()
        else:
          user = await user_crud.create_user(
            username=challenge.username,
            name=challenge.display_name or challenge.username,
            hashed_password=challenge.password_hash,
            roles=roles,
          )
          user.roles_version = 1
          await user.save()
        await _ensure_user_lock_owned(guard)
        await sync_module.publish_user(
          user,
          fields={"auth", "roles", "profile"},
        )
        await _ensure_user_lock_owned(guard)
      except BaseException:
        if created and user is not None:
          await asyncio.shield(user.delete())
        elif user is not None and previous is not None:
          for field, value in previous.items():
            setattr(user, field, value)
          await asyncio.shield(user.save())
        raise
      return user
  except sync_module.UserIdentityUpdateLockBusyError as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "该用户的身份同步正在进行，请稍后重试",
    ) from error
  except sync_module.UserIdentityUpdateLockLostError as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "用户身份同步状态暂时无法确认，请刷新后重试",
    ) from error
  except sync_module.UserIdentityVersionConflictError as error:
    raise CustomException(ErrorDesc.SYNC_FAILED, str(error)) from error
  except CustomException:
    raise
  except Exception as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "用户注册信息未能同步到所有区域，请稍后重试",
    ) from error


async def _complete_password_reset(challenge) -> User:
  from src.core import sync as sync_module

  try:
    async with sync_module.user_role_update_lock(
      challenge.username,
      timeout=USER_IDENTITY_LOCK_ACQUIRE_TIMEOUT_SECONDS,
    ) as guard:
      user = await sync_module.refresh_user_identity_from_etcd(challenge.username)
      if not user or getattr(user, "is_sync", False) or not challenge.password_hash:
        raise CustomException(ErrorDesc.AUTH_CODE_INVALID)
      previous_hash = user.hashed_password
      previous_auth_version = int(getattr(user, "auth_version", 0))
      previous_updated_at = getattr(user, "updated_at", None)
      try:
        await _ensure_user_lock_owned(guard)
        user.hashed_password = challenge.password_hash
        user.auth_version = previous_auth_version + 1
        user.updated_at = utc_now()
        await user.save()
        await _ensure_user_lock_owned(guard)
        await sync_module.publish_user(user, fields={"auth"})
        await _ensure_user_lock_owned(guard)
      except BaseException:
        user.hashed_password = previous_hash
        user.auth_version = previous_auth_version
        user.updated_at = previous_updated_at
        await asyncio.shield(user.save())
        raise
      return user
  except sync_module.UserIdentityUpdateLockBusyError as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "该用户的身份同步正在进行，请稍后重试",
    ) from error
  except sync_module.UserIdentityUpdateLockLostError as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "用户身份同步状态暂时无法确认，请刷新后重试",
    ) from error
  except sync_module.UserIdentityVersionConflictError as error:
    raise CustomException(ErrorDesc.SYNC_FAILED, str(error)) from error
  except CustomException:
    raise
  except Exception as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "密码重置未能同步到所有区域，请稍后重试",
    ) from error


async def login_by_code(username: str, code: str) -> dict:
  from src.core import audit

  challenge = await user_crud.consume_auth_challenge(username, code)
  if challenge is None:
    # 前端会并发询问全部后端；未持有本地挑战是正常分支，不记失败审计。
    from src.core import metrics as metrics_mod
    metrics_mod.incr("oa_auth_challenge_misses_total")
    raise CustomException(ErrorDesc.AUTH_CODE_INVALID)

  try:
    if challenge.purpose == AUTH_PURPOSE_REGISTER:
      user = await _complete_registration(challenge)
    elif challenge.purpose == AUTH_PURPOSE_PASSWORD_RESET:
      user = await _complete_password_reset(challenge)
    elif challenge.purpose == AUTH_PURPOSE_LOGIN:
      user = await user_crud.read_user_by_username(username)
      if not user or getattr(user, "is_sync", False):
        raise CustomException(ErrorDesc.AUTH_CODE_INVALID)
    else:
      raise CustomException(ErrorDesc.AUTH_CODE_INVALID)
  except CustomException:
    await user_crud.restore_auth_challenge(challenge)
    raise
  except Exception:
    await user_crud.restore_auth_challenge(challenge)
    raise

  audit.audit(
    "auth.oa_verify",
    actor=username,
    resource=challenge.purpose,
  )
  return await create_token(user.username, int(getattr(user, "auth_version", 0)))


async def login_user(username: str, password: str) -> dict:
  """
  用户登录，返回 JWT token
  
  Args:
    username: 用户名
    password: 密码
    
  Returns:
    dict: 包含 access_token 和 token_type 的字典
    
  Raises:
    CustomException: 登录失败
  """
  user = await authenticate_user(username, password)
  if not user:
    raise CustomException(ErrorDesc.LOGIN_ERR)
  
  # 创建 access token
  token_data = await create_token(user.username, int(getattr(user, "auth_version", 0)))
  return token_data

async def refresh_token(refresh_token_str: str) -> dict:
  """
  使用 refresh token 获取新的 access token
  """
  from src.core.auth import TOKEN_TYP_REFRESH

  token_is_valid = await user_crud.check_token_valid(refresh_token_str)
  if not token_is_valid:
    raise CustomException(ErrorDesc.TOKEN_DESTROYED)
  try:
    payload = jwt.decode(refresh_token_str, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    username: str | None = payload.get("sub")
    if username is None:
      raise CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
    typ = payload.get("typ")
    # 兼容旧 refresh（无 typ）；有 typ 时必须为 refresh，禁止用 access 续期
    if typ is not None and typ != TOKEN_TYP_REFRESH:
      raise CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
  except JWTError:
    raise CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
  user = await user_crud.read_user_by_username(username)
  if user is None:
    raise CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
  try:
    token_version = int(payload.get("ver", 0))
  except (TypeError, ValueError):
    raise CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
  if token_version != int(getattr(user, "auth_version", 0)):
    raise CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
  # 旋转：旧 refresh 立即失效
  await user_crud.destroy_token(refresh_token_str)
  return await create_token(user.username, int(getattr(user, "auth_version", 0)))

async def get_user_profile(user: User) -> dict:
  """
  获取用户个人信息
  
  Args:
    user: 用户对象
    
  Returns:
    dict: 用户个人信息
  """
  user_obj = await user_crud.read_user_by_username(user.username)
  user_roles = []
  is_admin = False
  admin_role = await user_crud.get_admin_role()
  profile_roles = sorted(
    (role for role in (user_obj.roles or []) if getattr(role, "name", None)),
    key=lambda role: _role_sort_key(role.name),
  )
  for role in profile_roles:
    if (
      getattr(role, "name", None) == ROLE_SUPERADMIN
      or (admin_role and getattr(role, "id", None) == admin_role.id)
    ):
      is_admin = True
    user_roles.append({
      "id": str(role.id),
      "name": role.name
    })
  return dict[str, str | datetime | list](
    id=str(user_obj.id),
    username=user_obj.username,
    name=user_obj.name,
    roles=user_roles,
    permissions=sorted(getattr(user_obj, "permissions", []) or []),
    is_admin=is_admin,
    created_at=convert_utc_to_local_str(user_obj.created_at),
    updated_at=convert_utc_to_local_str(user_obj.updated_at),
    system_time=convert_utc_to_local_str(local_utc_now())
  )

async def logout_user(token: str) -> dict:
  """
  用户登出
  
  Args:
    token: 令牌
    
  Returns:
    dict: 登出成功
  """
  await user_crud.destroy_token(token)
  return dict[str, str](
    message="登出成功"
  )


def _user_role_summary(user: User, admin_role) -> dict:
  role_items = []
  role_items = [
    {"id": str(role.id), "name": role.name}
    for role in (user.roles or [])
    if getattr(role, "name", None)
  ]
  role_items.sort(key=lambda item: _role_sort_key(item["name"]))
  role_names = [item["name"] for item in role_items]
  is_admin = ROLE_SUPERADMIN in role_names or any(
    admin_role and item["id"] == str(admin_role.id) for item in role_items
  )
  specialty_names = [name for name in role_names if name != ROLE_USER]
  role_name = ROLE_SUPERADMIN if is_admin else (
    specialty_names[-1] if specialty_names else ROLE_USER
  )
  return {
    "id": str(user.id),
    "username": user.username,
    "name": user.name,
    "is_admin": is_admin,
    "role_name": role_name,
    "roles": role_items,
    "permissions": sorted(getattr(user, "permissions", []) or []),
    "created_at": convert_utc_to_local_str(user.created_at),
    "updated_at": convert_utc_to_local_str(user.updated_at),
  }


async def list_users_for_admin() -> dict:
  """管理员：列出可登录用户及其角色。"""
  admin_role = await user_crud.get_admin_role()
  users = await user_crud.list_local_users()
  data = [_user_role_summary(user, admin_role) for user in users]
  data.sort(key=lambda item: item["username"])
  return {"data": data}


async def update_user_role_for_admin(
  user_id: str,
  role_names: list[str] | str,
  actor: User | None = None,
) -> dict:
  """Update a user's system roles, preserving the mandatory basic role."""
  legacy_call = isinstance(role_names, str)
  submitted = [role_names] if legacy_call else list(role_names or [])
  desired_names = []
  for value in submitted:
    role_name = (value or "").strip()
    if role_name and role_name not in desired_names:
      desired_names.append(role_name)
  if ROLE_USER not in desired_names:
    desired_names.insert(0, ROLE_USER)
  allowed_names = {
    ROLE_USER,
    ROLE_APPLICATION_ADMIN,
    ROLE_OPERATIONS_ADMIN,
    ROLE_USER_ADMIN,
    ROLE_SUPERADMIN,
  }
  unknown_names = sorted(set(desired_names) - allowed_names)
  if unknown_names:
    raise CustomException(
      ErrorDesc.INVALID_PARAMS,
      f"不支持的系统角色: {', '.join(unknown_names)}",
    )

  # Resolve the stable username before taking the cross-region lock. The
  # target is read again inside the lock so authorization and rollback state
  # come from the serialized snapshot.
  target = await user_crud.read_user_by_id(user_id)
  if not target or getattr(target, "is_sync", False):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "用户不存在")
  from src.core import sync as sync_module
  try:
    async with sync_module.user_role_update_lock(
      target.username,
      timeout=USER_IDENTITY_LOCK_ACQUIRE_TIMEOUT_SECONDS,
    ) as guard:
      await sync_module.refresh_user_identity_from_etcd(target.username)
      return await _update_user_roles_locked(
        user_id,
        desired_names,
        actor,
        legacy_call=legacy_call,
        guard=guard,
      )
  except sync_module.UserRoleUpdateLockBusyError as error:
    raise CustomException(
      ErrorDesc.STATUS_ERR,
      "身份同步暂时未完成，请稍后重试",
    ) from error
  except sync_module.UserIdentityUpdateLockLostError as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "用户身份更新锁已丢失，角色变更已回滚，请重试",
    ) from error


async def _update_user_roles_locked(
  user_id: str,
  desired_names: list[str],
  actor: User | None,
  *,
  legacy_call: bool,
  guard=None,
) -> dict:
  from src.core import sync as sync_module

  target = await user_crud.read_user_by_id(user_id)
  if not target or getattr(target, "is_sync", False):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "用户不存在")

  admin_role = await user_crud.get_admin_role()
  basic_role = await user_crud.get_basic_role()
  if not admin_role or not basic_role:
    raise CustomException(ErrorDesc.STATUS_ERR, "系统角色未初始化")

  current_names = {
    getattr(role, "name", None) for role in (target.roles or [])
    if getattr(role, "name", None)
  }
  currently_admin = ROLE_SUPERADMIN in current_names or any(
    getattr(role, "id", None) == admin_role.id for role in (target.roles or [])
  )
  wants_admin = ROLE_SUPERADMIN in desired_names
  actor_is_admin = actor is None or any(
    getattr(role, "name", None) == ROLE_SUPERADMIN
    or getattr(role, "id", None) == admin_role.id
    for role in (getattr(actor, "roles", []) or [])
  )
  if currently_admin != wants_admin and not actor_is_admin:
    raise CustomException(
      ErrorDesc.INSUFFICIENT_PERMISSIONS,
      "只有管理员可以授予或移除管理员角色",
    )
  if actor is not None and str(actor.id) == str(target.id) and not actor_is_admin:
    added_roles = set(desired_names) - current_names - {ROLE_USER}
    if added_roles:
      raise CustomException(ErrorDesc.INSUFFICIENT_PERMISSIONS, "不能提升自己的系统角色")
  if currently_admin and not wants_admin:
    if await user_crud.count_admin_users() <= 1:
      raise CustomException(ErrorDesc.INVALID_PARAMS, "不能取消系统中唯一的管理员")

  roles_by_name = {ROLE_USER: basic_role, ROLE_SUPERADMIN: admin_role}
  for role_name in desired_names:
    if role_name in roles_by_name:
      continue
    role = await user_crud.get_role_by_name(role_name)
    if role is None:
      raise CustomException(ErrorDesc.STATUS_ERR, f"系统角色未初始化: {role_name}")
    roles_by_name[role_name] = role
  selected_roles = [roles_by_name[name] for name in desired_names]
  previous_roles = list(target.roles or [])
  previous_permissions = list(getattr(target, "permissions", []) or [])
  previous_roles_version = int(getattr(target, "roles_version", 0))
  previous_updated_at = getattr(target, "updated_at", None)
  try:
    await _ensure_user_lock_owned(guard)
    target.roles_version = previous_roles_version + 1
    if legacy_call:
      compatibility_role = admin_role if wants_admin else basic_role
      updated = await user_crud.update_user_role(target, compatibility_role)
    else:
      updated = await user_crud.update_user_roles(target, selected_roles)
    await _ensure_user_lock_owned(guard)
    await sync_module.publish_user(updated, fields={"roles"})
    await _ensure_user_lock_owned(guard)
  except BaseException as e:
    target.roles = previous_roles
    target.permissions = previous_permissions
    target.roles_version = previous_roles_version
    target.updated_at = previous_updated_at
    await asyncio.shield(target.save())
    if isinstance(e, sync_module.LastSuperadminError):
      raise CustomException(ErrorDesc.INVALID_PARAMS, str(e)) from e
    if isinstance(e, (asyncio.CancelledError, sync_module.UserIdentityUpdateLockLostError)):
      raise
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "用户角色未能同步到所有区域，请稍后重试",
    ) from e
  refreshed = await user_crud.read_user_by_id(updated.id)
  summary = _user_role_summary(refreshed or updated, admin_role)
  return {
    "id": summary["id"],
    "username": summary["username"],
    "name": summary["name"],
    "is_admin": summary["is_admin"],
    "role_name": summary["role_name"],
    "roles": summary["roles"],
    "permissions": summary["permissions"],
    "created_at": summary["created_at"],
    "updated_at": summary["updated_at"],
  }
