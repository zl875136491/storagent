import bcrypt
from fastapi import Depends, applications
from jose import JWTError, jwt
from datetime import timedelta
from typing import Optional, List
from fastapi.security import OAuth2PasswordBearer

from src.configs.configs import settings
from src.modules.auth.model import User
from src.utils.helpers import utc_now, import_user_from_springboard
from src.modules.auth import crud as user_crud
from src.core.exception import CustomException, ErrorDesc
from src.configs.consts import preset_permissions


# OAuth2 密码流（用于从请求中提取 token）
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login")

def password_check(password: str) -> bool:
  """
  检查密码是否符合要求
  """
  if len(password) < 8:
    return False
  # 没有数字
  if not any(char.isdigit() for char in password):
    return False
  # 没有字母
  if not any(char.isalpha() for char in password):
    return False
  return True

def verify_password(plain_password: str, hashed_password: str) -> bool:
  """
  验证密码是否匹配
  
  Args:
    plain_password: 明文密码
    hashed_password: 哈希后的密码
      
  Returns:
    bool: 密码是否匹配
  """
  # bcrypt 需要字节字符串
  password_bytes = plain_password.encode('utf-8')
  hashed_bytes = hashed_password.encode('utf-8')
  return bcrypt.checkpw(password_bytes, hashed_bytes)

def get_password_hash(password: str) -> str:
  """
  对密码进行哈希处理
  
  Args:
    password: 明文密码
      
  Returns:
    str: 哈希后的密码
  """
  # bcrypt 限制密码长度为 72 字节
  # 将密码编码为字节，如果超过 72 字节则截断
  password_bytes = password.encode('utf-8')
  if len(password_bytes) > 72:
    password_bytes = password_bytes[:72]
  
  # # 生成盐并哈希密码
  
  # salt = bcrypt.gensalt(rounds=settings.BCRYPT_ROUNDS)
  # 使用静态盐，保证多后端的认证互认
  salt = settings.BCRYPT_SALT.encode('utf-8')
  hashed = bcrypt.hashpw(password_bytes, salt)
  return hashed.decode('utf-8')

TOKEN_TYP_ACCESS = "access"
TOKEN_TYP_REFRESH = "refresh"

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
  """
  创建 JWT access token
  
  Args:
    data: 要编码到 token 中的数据（通常是用户ID或邮箱）
    expires_delta: token 过期时间增量，如果为 None 则使用默认值
      
  Returns:
    str: JWT token 字符串
  """
  to_encode = data.copy()
  if expires_delta:
    expire = utc_now() + expires_delta
  else:
    expire = utc_now() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
  
  to_encode.update({"exp": expire})
  encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
  return encoded_jwt

async def create_token(username: str) -> dict:
  """
  创建 JWT token（access / refresh 带 typ，防止互相冒用）
  
  Args:
    username: 用户名
    
  Returns:
    dict: 包含 access_token 和 refresh_token 的 token 字典
  """
  access_token_expires = timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
  refresh_token_expires = timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
  access_token = create_access_token(
    data={"sub": username, "typ": TOKEN_TYP_ACCESS},
    expires_delta=access_token_expires
  )
  refresh_token = create_access_token(
    data={"sub": username, "typ": TOKEN_TYP_REFRESH},
    expires_delta=refresh_token_expires
  )
  return {
    "access_token": access_token,
    "refresh_token": refresh_token,
    "token_type": "bearer"
  }

async def authenticate_user(username: str, password: str) -> Optional[User]:
  """
  验证用户凭据
  
  Args:
    username: 用户名
    password: 密码
    
  Returns:
    User: 如果验证成功返回用户对象，否则返回 None
  """
  user = await user_crud.read_user_by_username(username)
  if not user or getattr(user, "is_sync", False):
    user_info = await import_user_from_springboard(username)
    if user_info is None:
      raise CustomException(ErrorDesc.LOGIN_ERR, "用户不存在")
    if not password_check(password):
      raise CustomException(ErrorDesc.PASSWORD_UNSET, "密码不符合要求(至少8位，包含数字和字母)")
    hashed_password = get_password_hash(password)
    if preset_admin_user(username):
      admin_role = await user_crud.get_admin_role()
      roles = [admin_role]
    else:
      basic_role = await user_crud.get_basic_role()
      roles = [basic_role]
    if user:
      user.name = user_info["user_info"]["l"]
      user.hashed_password = hashed_password
      user.roles = roles
      user.permissions = await user_crud.get_all_permissions(roles)
      user.is_sync = False
      user.updated_at = utc_now()
      await user.save()
    else:
      user = await user_crud.create_user(
        username=username,
        name=user_info["user_info"]["l"],
        hashed_password=hashed_password,
        roles=roles
      )
  if not verify_password(password, user.hashed_password):
    raise CustomException(ErrorDesc.LOGIN_ERR, "密码错误")
  return user

def preset_admin_user(username):
  """
  检查用户是否为预设的管理员用户
  """
  from src.configs.consts import preset_admin_users
  if username in preset_admin_users:
    return True
  return False

async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
  """
  从 JWT token 中获取当前用户
  
  这是一个 FastAPI 依赖项，会自动从请求头中提取 token 并验证
  
  Args:
    token: JWT token（从请求头中自动提取）
      
  Returns:
    User: 当前用户对象
      
  Raises:
    CustomException: token 无效或用户不存在
  """
  credentials_exception = CustomException(
    msg=ErrorDesc.CREDENTIALS_NOT_VALID,
    reason={"headers": {"WWW-Authenticate": "Bearer"}}
  )
  
  token_is_valid = await user_crud.check_token_valid(token)
  if not token_is_valid:
    destroyed_token_exception = CustomException(
      msg=ErrorDesc.TOKEN_DESTROYED,
      reason={"headers": {"WWW-Authenticate": "Bearer"}}
    )
    raise destroyed_token_exception
  
  try:
    payload = jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    username: str | None = payload.get("sub")
    if username is None:
      raise credentials_exception
    typ = payload.get("typ")
    # 兼容旧 token（无 typ）；有 typ 时必须为 access
    if typ is not None and typ != TOKEN_TYP_ACCESS:
      raise credentials_exception
      
  except JWTError:
    raise credentials_exception
  
  user = await user_crud.read_user_by_username(username)
  if user is None:
    raise credentials_exception
  
  return user

async def check_permissions(user: User = Depends(get_current_user), permissions: List[str] = []) -> bool:
  """
  检查用户是否具有指定的权限
  """
  admin_role = await user_crud.get_admin_role()
  if admin_role:
    for role in user.roles:
      if role.to_ref().id == admin_role.id:
        return True
  for permission in permissions:
    if permission not in user.permissions:
      permission_name = preset_permissions[permission]["name"]
      raise CustomException(ErrorDesc.INSUFFICIENT_PERMISSIONS, f"用户缺少权限: {permission_name}")
  return user


async def require_admin(user: User = Depends(get_current_user)) -> User:
  """Require the preset administrator role for sensitive system settings."""
  admin_role = await user_crud.get_admin_role()
  if admin_role:
    for role in user.roles:
      if role.to_ref().id == admin_role.id:
        return user
  raise CustomException(ErrorDesc.INSUFFICIENT_PERMISSIONS, "仅管理员可以管理系统配置")

# 从请求头中提取 API-KEY 字段作为 App 数据源
from fastapi.security import APIKeyHeader
from src.modules.public import crud as public_crud
from src.utils.helpers import before_compare
async def get_current_app(api_key: str = Depends(APIKeyHeader(name="x-api-key"))) -> str:
  """
  从请求头中提取 API-KEY 字段作为输入源
  """
  from beanie.odm.fields import Link
  from src.modules.public.model import Application
  api_key_obj = await public_crud.read_api_key_by_key(api_key)
  if not api_key_obj or api_key_obj.deleted:
    raise CustomException(ErrorDesc.API_KEY_INVALID, "API-KEY 无效")
  if before_compare(api_key_obj.expired_at) < utc_now():
    raise CustomException(ErrorDesc.API_KEY_EXPIRED, "API-KEY 已过期")
  application_obj = api_key_obj.application
  if isinstance(application_obj, Link):
    application_obj = await public_crud.read_application_by_id(application_obj.ref.id)
  if not isinstance(application_obj, Application):
    raise CustomException(ErrorDesc.API_KEY_INVALID, "API-KEY 关联的应用不存在")
  if application_obj.enabled:
    return application_obj.name
  else:
    raise CustomException(ErrorDesc.APP_NOT_ENABLED, "应用未启用")
