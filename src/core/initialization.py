from src.utils.logger import logger

async def init_project():
  """
  初始化项目
  """
  from src.modules.auth import crud as user_crud
  from src.utils.helpers import get_full_permissions
  basic_role = await user_crud.get_basic_role()
  admin_role = await user_crud.get_admin_role()
  admin_permissions = get_full_permissions(["system_manage"])
  basic_permissions = get_full_permissions(["application_view", "region_view"])
  if not basic_role:
    await user_crud.create_role(name="用户", is_admin=False, permissions=basic_permissions)
  if not admin_role:
    await user_crud.create_role(name="管理员", is_admin=True, permissions=admin_permissions)
  logger.info("Role Data Created.")

async def init_service():
  """
  初始化服务：注册本节点到 Etcd，并从 Etcd 全量同步到 MongoDB
  """
  from src.configs.configs import settings
  from src.core.exception import CustomException, ErrorDesc
  from src.core import etcd_op, minio_op, sync as sync_module

  etcd_client = await etcd_op.get_etcd_client()
  try:
    minio_op.test_minio_server(
      host=settings.MINIO_HOST,
      port=settings.MINIO_PORT,
      access_key=settings.MINIO_ACCESS_KEY,
      secret_key=settings.MINIO_SECRET_KEY
    )
    logger.info(f"Minio Server Tested: {settings.MINIO_HOST}:{settings.MINIO_PORT}.")

    # 1. 注册本节点 Region 到 Etcd
    region_data = await etcd_op.pull_from_etcd_by_key(sync_module.ETCD_KEY_REGION, client=etcd_client)
    region_data[settings.REGION] = settings.REGION_NAME
    await etcd_op.push_to_etcd(sync_module.ETCD_KEY_REGION, region_data, client=etcd_client)

    # 2. 注册本节点 MinIO Server 到 Etcd（凭证加密）
    from src.core.crypto import encrypt_server_entry
    servers_data = await etcd_op.pull_from_etcd_by_key(sync_module.ETCD_KEY_SERVERS, client=etcd_client)
    servers_data[settings.REGION] = encrypt_server_entry({
      "host": settings.SERVER_HOST,
      "server_port": settings.SERVER_PORT,
      "minio_port": settings.MINIO_PORT,
      "access_key": settings.MINIO_ACCESS_KEY,
      "secret_key": settings.MINIO_SECRET_KEY,
      "replicate_weight": settings.MINIO_REPLICATE_WEIGHT,
    })
    await etcd_op.push_to_etcd(sync_module.ETCD_KEY_SERVERS, servers_data, client=etcd_client)

    # 3. 全量同步 Etcd -> MongoDB（含 applications / api_keys）
    await sync_module.pull_all_and_sync(client=etcd_client)

    logger.info(f"Service Initialized: {settings.REGION_NAME} ({settings.REGION}).")
  finally:
    await etcd_client.close()
