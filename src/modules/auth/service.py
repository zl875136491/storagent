from datetime import datetime
from src.modules.auth.model import User, DestoryedToken
from src.modules.auth import crud as user_crud
from src.core.exception import CustomException, ErrorDesc
from src.core.auth import authenticate_user, create_token
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