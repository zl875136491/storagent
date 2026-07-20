import asyncio
from src.modules.auth.model import User, Role, DestoryedToken
from typing import List
from src.utils.helpers import utc_now, get_full_permissions
from src.core.exception import CustomException, ErrorDesc
from src.utils.logger import logger

async def read_user_by_username(username: str) -> User:
  """
  根据用户名获取用户
  """
  return await User.find_one(User.username == username, fetch_links=True)

# async def read_temp_code(username: str, code: str) -> TempCode:
#   """
#   根据用户名和代码获取临时代码
#   """
#   return await TempCode.find_one(TempCode.username == username, TempCode.code == code)

async def create_user(
  username: str,
  name: str,
  hashed_password: str,
  roles: List[Role],
  is_sync: bool = False) -> User:
  """
  创建用户
  """
  permissions = await get_all_permissions(roles)
  user = User(
    username=username,
    name=name,
    hashed_password=hashed_password,
    roles=roles,
    permissions=permissions,
    is_sync=is_sync,
  )
  try:
    await user.save()
  except Exception as e:
    raise CustomException(ErrorDesc.CREATE_USER_FAILED, "创建用户失败")
  return user

async def check_token_valid(token: str) -> bool:
  """
  检查 token 是否有效
  """
  destoryed_token = await DestoryedToken.find_one(DestoryedToken.token == token)
  if destoryed_token:
    return False
  return True

async def create_role(name: str, is_admin: bool, permissions: List[str]) -> Role:
  """
  创建角色
  
  Args:
    name: 角色名称
    is_admin: 是否为管理员
    permissions: 权限

  Returns:
    Role: 角色
  """
  role = Role(
    name=name,
    is_admin=is_admin,
    permissions=permissions
  )
  await role.save()
  return role

async def get_admin_role() -> Role:
  """
  获取管理员角色
  """
  return await Role.find_one(Role.is_admin == True)

async def get_basic_role() -> Role:
  """
  获取基础用户角色
  """
  return await Role.find_one(Role.is_admin == False)

async def destroy_token(token: str) -> None:
  """
  销毁 token
  """
  await DestoryedToken(token=token, expired_at=utc_now()).save()

async def cleanup_expired_tokens() -> int:
  """
  清理已过期的黑名单 token
  """
  now = utc_now()
  expired = await DestoryedToken.find(DestoryedToken.expired_at < now).to_list()
  count = len(expired)
  for token_obj in expired:
    await token_obj.delete()
  return count

async def cleanup_expired_tokens_task():
  """
  后台任务：每小时清理过期 token
  """
  while True:
    try:
      count = await cleanup_expired_tokens()
      if count:
        logger.info(f"清理了 {count} 条过期 token")
    except asyncio.CancelledError:
      break
    except Exception as e:
      logger.warning(f"token 清理失败: {e}")
    await asyncio.sleep(3600)

async def get_all_permissions(roles: List[Role]) -> List[str]:
  """
  获取所有角色权限
  
  Args:
    roles: 角色列表

  Returns:
    List[str]: 权限列表
  """
  permissions = []
  for role in roles:
    full_permissions = get_full_permissions(role.permissions)
    permissions.extend(full_permissions)
  permissions = list(set(permissions))
  return permissions