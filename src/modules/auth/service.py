from datetime import datetime
from jose import JWTError, jwt
from datetime import timedelta
from src.modules.auth.model import User, DestoryedToken
from src.modules.auth import crud as user_crud
from src.core.exception import CustomException, ErrorDesc
from src.core.auth import authenticate_user, create_token
from src.configs.configs import settings
from src.utils.helpers import convert_utc_to_local_str, local_utc_now


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
  token_data = await create_token(user.username)
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
  # 旋转：旧 refresh 立即失效
  await user_crud.destroy_token(refresh_token_str)
  return await create_token(user.username)

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
