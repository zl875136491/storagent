async def init_project():
  """
  初始化项目
  """
  from src.modules.auth import crud as user_crud
  from src.utils.helpers import get_full_permissions
  # 新建基础角色
  basic_role = await user_crud.get_basic_role()
  admin_role = await user_crud.get_admin_role()
  admin_permissions = get_full_permissions(["system_manage"])
  basic_permissions = get_full_permissions(["application_view", "region_view"])
  if not basic_role:
    await user_crud.create_role(name="用户", is_admin=False, permissions=basic_permissions)
  if not admin_role:
    await user_crud.create_role(name="管理员", is_admin=True, permissions=admin_permissions)

async def init_service():
  """
  初始化服务
  """
  # TODO: 检查 mc 命令是否可用