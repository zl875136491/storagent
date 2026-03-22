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
  # 0: 检查 mc 命令是否可用
  
  # 1. 确定 Region 信息
  from src.configs.configs import settings
  from src.core.exception import CustomException, ErrorDesc
  from src.modules.public import crud as public_crud
  region_config_status = public_crud.read_system_config_by_key("region_config_status")
  if not region_config_status:
    region_config_status = await public_crud.create_system_config(
      key="region",
      value="none",
      name="区域",
      description="区域",
      value_type="str"
    )
  if region_config_status.value == "none":
    region_name = settings.REGION
    if region_name == "undefined":
      raise CustomException(ErrorDesc.REGION_NOT_DEF)
    existed_region = await public_crud.read_region_by_name(region_name)
    if existed_region:
      raise CustomException(ErrorDesc.REGION_EXISTED)
    region = await public_crud.create_region(region_name, region_name)
    await public_crud.update_system_config_by_key("region_config_status", region_name)
  else:
    region_name = region_config_status.value
  
  # 2. 连接 minio 存储
  
