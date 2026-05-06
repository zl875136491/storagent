from src.utils.logger import logger

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
  logger.info(f"Role Data Created.")

async def init_service():
  """
  初始化服务
  """
  from src.configs.configs import settings
  from src.core.exception import CustomException, ErrorDesc
  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud

  # 0: 检查 mc 命令是否可用
  
  # 1. 确定 Region 信息
  region_status_key = "region_config_status"
  region_config_status = await public_crud.read_system_config_by_key(region_status_key)

  if not region_config_status:
    region_config_status = await public_crud.create_system_config(
      key=region_status_key,
      value="none",
      name="区域",
      description="区域",
      value_type="str"
    )
  if region_config_status.value == "none":
    region_name = settings.REGION
    if region_name == "undefined":
      raise CustomException(ErrorDesc.REGION_NOT_DEF, "")
    existed_region = await public_crud.read_region_by_name(region_name)
    if existed_region:
      raise CustomException(ErrorDesc.REGION_EXISTED, "")
    region_obj = await public_crud.create_region(region_name, region_name)
    await public_crud.update_system_config_by_key(region_status_key, region_name)
  else:
    region_name = region_config_status.value
    region_obj = await public_crud.read_region_by_name(region_name)
    if not region_obj:
      raise CustomException(ErrorDesc.RES_NOT_FOUND, region_name)
  logger.info(f"Region Initialized: {region_name}.")
  
  # 2. 连接 minio 存储服务
  from src.core import minio_op
  minio_op.test_minio_server(
    host=settings.MINIO_HOST,
    port=settings.MINIO_PORT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY
  )
  logger.info(f"Minio Server Tested: {settings.MINIO_HOST}:{settings.MINIO_PORT}.")

  # 3. 创建 Minio 服务数据
  minio_server_obj = await storage_crud.read_minio_server_by_region(region_obj)
  if not minio_server_obj:
    minio_server_obj = await storage_crud.create_minio_server(
      region=region_obj,
      name=region_name,
      host=settings.SERVER_HOST,
      server_port=settings.SERVER_PORT,
      minio_port=settings.MINIO_PORT,
      access_key=settings.MINIO_ACCESS_KEY,
      secret_key=settings.MINIO_SECRET_KEY
    )
  else:
    if any([
      settings.SERVER_HOST != minio_server_obj.host,
      settings.SERVER_PORT != minio_server_obj.server_port
    ]):
      minio_server_obj.host = settings.SERVER_HOST
      minio_server_obj.server_port = settings.SERVER_PORT
      await minio_server_obj.save()
  logger.info(f"Minio Server Created: {region_name}.")
  
  # 4. 创建别名
  success, res = await minio_op.set_site_alias(
    site_name=region_name,
    endpoint=f"{settings.MINIO_HOST}:{settings.MINIO_PORT}",
    admin_user=settings.MINIO_ACCESS_KEY,
    admin_password=settings.MINIO_SECRET_KEY
  )
  if not success:
    raise CustomException(ErrorDesc.MINIO_ALIAS_FAILED, res)
  logger.info(f"Minio Alias Created: {region_name}.")
  