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


def _directory_display_name(user_info: dict | None, username: str) -> str:
  if not isinstance(user_info, dict):
    return username
  nested = user_info.get("user_info")
  if not isinstance(nested, dict):
    return username
  return str(nested.get("l") or nested.get("name") or username).strip() or username


async def _publish_user_best_effort(user: User) -> None:
  try:
    from src.core import sync as sync_module
    await sync_module.publish_user(user)
  except Exception as exc:
    from src.core import metrics as metrics_mod
    from src.utils.logger import logger
    metrics_mod.incr("sync_failures_total")
    logger.warning(
      f"User {user.username} 同步到 Etcd 失败，将由周期校准重试: {exc}"
    )


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
  user = await user_crud.read_user_by_username(challenge.username)
  if user and not getattr(user, "is_sync", False):
    return user
  if not challenge.password_hash:
    raise CustomException(ErrorDesc.AUTH_CODE_INVALID)

  if preset_admin_user(challenge.username):
    role = await user_crud.get_admin_role()
  else:
    role = await user_crud.get_basic_role()
  if role is None:
    raise CustomException(ErrorDesc.STATUS_ERR, "系统角色尚未初始化")

  if user:
    user.name = challenge.display_name or challenge.username
    user.hashed_password = challenge.password_hash
    user.roles = [role]
    user.permissions = await user_crud.get_all_permissions([role])
    user.is_sync = False
    user.updated_at = utc_now()
    await user.save()
  else:
    user = await user_crud.create_user(
      username=challenge.username,
      name=challenge.display_name or challenge.username,
      hashed_password=challenge.password_hash,
      roles=[role],
    )
  await _publish_user_best_effort(user)
  return user


async def _complete_password_reset(challenge) -> User:
  user = await user_crud.read_user_by_username(challenge.username)
  if not user or getattr(user, "is_sync", False) or not challenge.password_hash:
    raise CustomException(ErrorDesc.AUTH_CODE_INVALID)
  user.hashed_password = challenge.password_hash
  user.auth_version = int(getattr(user, "auth_version", 0)) + 1
  user.updated_at = utc_now()
  await user.save()
  await _publish_user_best_effort(user)
  return user


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
  for role in user_obj.roles:
    if role.id == admin_role.id:
      is_admin = True
    user_roles.append({
      "id": str(role.id),
      "name": role.name
    })
  # user_permissions = []
  # for permission in user.permissions:
  #   user_permissions.append(permission)
  return dict[str, str | datetime | list](
    id=str(user.id),
    username=user.username,
    name=user.name,
    roles=user_roles,
    is_admin=is_admin,
    created_at=convert_utc_to_local_str(user.created_at),
    updated_at=convert_utc_to_local_str(user.updated_at),
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
  is_admin = False
  role_name = "用户"
  for role in user.roles or []:
    if admin_role and getattr(role, "id", None) == admin_role.id:
      is_admin = True
      role_name = role.name or "管理员"
      break
    if getattr(role, "name", None):
      role_name = role.name
  return {
    "id": str(user.id),
    "username": user.username,
    "name": user.name,
    "is_admin": is_admin,
    "role_name": role_name,
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


async def update_user_role_for_admin(user_id: str, role_name: str) -> dict:
  """管理员：将用户角色设为「用户」或「管理员」。"""
  role_name = (role_name or "").strip()
  if role_name not in ("用户", "管理员"):
    raise CustomException(ErrorDesc.INVALID_PARAMS, "角色仅支持「用户」或「管理员」")

  target = await user_crud.read_user_by_id(user_id)
  if not target or getattr(target, "is_sync", False):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "用户不存在")

  admin_role = await user_crud.get_admin_role()
  basic_role = await user_crud.get_basic_role()
  if not admin_role or not basic_role:
    raise CustomException(ErrorDesc.STATUS_ERR, "系统角色未初始化")

  currently_admin = any(
    getattr(role, "id", None) == admin_role.id for role in (target.roles or [])
  )
  if currently_admin and role_name == "用户":
    if await user_crud.count_admin_users() <= 1:
      raise CustomException(ErrorDesc.INVALID_PARAMS, "不能取消系统中唯一的管理员")

  new_role = admin_role if role_name == "管理员" else basic_role
  previous_roles = list(target.roles or [])
  previous_permissions = list(getattr(target, "permissions", []) or [])
  previous_updated_at = getattr(target, "updated_at", None)
  updated = await user_crud.update_user_role(target, new_role)
  try:
    from src.core import sync as sync_module
    await sync_module.publish_user(updated)
  except Exception as e:
    target.roles = previous_roles
    target.permissions = previous_permissions
    target.updated_at = previous_updated_at
    await target.save()
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
  }
