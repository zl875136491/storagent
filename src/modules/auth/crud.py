from src.modules.auth.model import User, Role, DestoryedToken
from typing import List
from src.utils.helpers import utc_now

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
  roles: List[Role]) -> User:
  """
  创建用户
  
  Args:
    username: 用户名
    name: 姓名
    hashed_password: 哈希后的密码
    roles: 角色

  Returns:
    User: 用户
  """
  user = User(
    username=username,
    name=name,
    hashed_password=hashed_password,
    roles=roles,
    permissions=[]
  )
  await user.save()
  return user

async def check_token_valid(token: str) -> bool:
  """
  检查 token 是否有效
  """
  destoryed_token = await DestoryedToken.find_one(DestoryedToken.token == token)
  if destoryed_token:
    return False
  return True

async def get_admin_role() -> Role:
  """
  获取管理员角色
  """
  return await Role.find_one(Role.is_admin == True)

async def destroy_token(token: str) -> None:
  """
  销毁 token
  """
  await DestoryedToken(token=token, expired_at=utc_now()).save()