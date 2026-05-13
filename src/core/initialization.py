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
  from src.core import etcd_op
  etcd_client = await etcd_op.get_etcd_client()
  
  # 0: 检查 mc 命令是否可用

  # 1. 连接 minio 存储服务
  from src.core import minio_op
  minio_op.test_minio_server(
    host=settings.MINIO_HOST,
    port=settings.MINIO_PORT,
    access_key=settings.MINIO_ACCESS_KEY,
    secret_key=settings.MINIO_SECRET_KEY
  )
  logger.info(f"Minio Server Tested: {settings.MINIO_HOST}:{settings.MINIO_PORT}.")

  # 2. Region 信息
  region_name = settings.REGION_NAME
  region_value = settings.REGION
  # 2.1 注册本地 Region 信息到 Etcd
  region_data = await etcd_op.pull_from_etcd_by_key("region", client=etcd_client)
  region_data[region_value] = region_name
  await etcd_op.push_to_etcd("region", region_data, client=etcd_client)
  # 2.2 Etcd 信息同步到 MongoDB
  for region_value_item, region_name_item in region_data.items():
    region_obj = await public_crud.read_region_by_name(region_value_item)
    if not region_obj:
      region_obj = await public_crud.create_region(region_value_item, region_name_item)
  logger.info(f"Region Initialized: {region_name} ({region_value}).")
  
  # 3. MinIO 信息
  # 3.1 注册本地 MinIO 信息到 Etcd
  servers_data = await etcd_op.pull_from_etcd_by_key("servers", client=etcd_client)
  this_server_data = {
    "server_port": settings.SERVER_PORT,
    "minio_port": settings.MINIO_PORT,
    "access_key": settings.MINIO_ACCESS_KEY,
    "secret_key": settings.MINIO_SECRET_KEY,
    "replicate_weight": settings.MINIO_REPLICATE_WEIGHT
  }
  servers_data[region_value] = this_server_data
  await etcd_op.push_to_etcd("servers", servers_data, client=etcd_client)
  
  # 3.2 Etcd 信息同步到 MongoDB
  for server_region_name, server_data in servers_data.items():
    if any([
      "server_port" not in server_data,
      "minio_port" not in server_data,
      "access_key" not in server_data,
      "secret_key" not in server_data,
      "replicate_weight" not in server_data
    ]):
      # 非有效数据, 跳过
      continue
    else:
      region_obj = await public_crud.read_region_by_name(server_region_name)
      if not region_obj:
        region_obj = await public_crud.create_region(server_region_name, server_region_name)
      server_obj = await storage_crud.read_minio_server_by_region(region_obj)
      if not server_obj:
        server_obj = await storage_crud.create_minio_server(
          region=region_obj,
          name=server_region_name,
          host=settings.SERVER_HOST,
          server_port=server_data["server_port"],
          minio_port=server_data["minio_port"],
          access_key=server_data["access_key"],
          secret_key=server_data["secret_key"],
          replicate_weight=server_data["replicate_weight"]
        )
      else:
        await storage_crud.update_minio_server(
          minio_server=server_obj,
          host=settings.SERVER_HOST,
          server_port=server_data["server_port"],
          minio_port=server_data["minio_port"],
          access_key=server_data["access_key"],
          secret_key=server_data["secret_key"],
          replicate_weight=server_data["replicate_weight"]
        )  
  
  # 5. 创建别名
  for server_region_name, server_data in servers_data.items():
    if any([
      "server_port" not in server_data,
      "minio_port" not in server_data,
      "access_key" not in server_data,
      "secret_key" not in server_data,
      "replicate_weight" not in server_data
    ]):
      # 非有效数据, 跳过
      continue
    else:
      success, res = await minio_op.set_site_alias(
        site_name=server_region_name,
        endpoint=f"{settings.SERVER_HOST}:{server_data["minio_port"]}",
        admin_user=server_data["access_key"],
        admin_password=server_data["secret_key"]
      )
      if not success:
        raise CustomException(ErrorDesc.MINIO_ALIAS_FAILED, res)
  logger.info(f"Minio Alias Created: {region_value}.")