from src.utils.logger import logger

async def init_project():
  """
  初始化项目
  """
  from src.modules.auth import crud as user_crud
  from src.configs.consts import system_role_definitions
  from src.utils.helpers import get_full_permissions

  for role_name, definition in system_role_definitions.items():
    await user_crud.upsert_role(
      name=role_name,
      is_admin=bool(definition["is_admin"]),
      permissions=get_full_permissions(definition["permissions"]),
    )
  updated_users = await user_crud.recompute_all_user_permissions()
  logger.info(f"System roles initialized; normalized {updated_users} users.")

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

    # 1. 注册本节点 Region 到 Etcd（CAS 合并）
    await etcd_op.merge_update_etcd_key(
      sync_module.ETCD_KEY_REGION,
      lambda data: {**data, settings.REGION: settings.REGION_NAME},
      client=etcd_client,
    )

    # 2. 注册本节点 MinIO Server 到 Etcd（凭证加密 + CAS）
    from src.core.crypto import encrypt_server_entry
    entry = encrypt_server_entry({
      "host": settings.SERVER_HOST,
      "server_port": settings.SERVER_PORT,
      "minio_port": settings.MINIO_PORT,
      "access_key": settings.MINIO_ACCESS_KEY,
      "secret_key": settings.MINIO_SECRET_KEY,
      "replicate_weight": settings.MINIO_REPLICATE_WEIGHT,
    })
    await etcd_op.merge_update_etcd_key(
      sync_module.ETCD_KEY_SERVERS,
      lambda data: {**data, settings.REGION: entry},
      client=etcd_client,
    )

    # 3. 合并本地身份数据。用户按 username 合并，不会覆盖其他区域独有用户。
    await sync_module.publish_roles(client=etcd_client)
    await sync_module.publish_local_users(client=etcd_client)

    # 4. 拓扑布局仅首次由权威区域写入；后续所有区域均走共享 CAS 更新。
    await sync_module.bootstrap_topology_layout(client=etcd_client)

    # 任一节点都可用确定性默认值原子补齐历史应用，不覆盖合法自定义配额。
    await sync_module.backfill_application_quotas(client=etcd_client)

    # 5. 全量同步 Etcd -> MongoDB（含身份、应用、API Key、拓扑布局）
    await sync_module.pull_all_and_sync(client=etcd_client)

    logger.info(f"Service Initialized: {settings.REGION_NAME} ({settings.REGION}).")
  finally:
    await etcd_client.close()
