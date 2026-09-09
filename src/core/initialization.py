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
  """Register this node and pull control-plane state.

  Etcd/lock slowness must not block process startup. Each step is bounded,
  busy user locks are skipped, and ``mc`` aliases can fall back to Mongo.
  """
  import asyncio

  from src.configs.configs import settings
  from src.core import etcd_op, minio_op, sync as sync_module

  overall = max(float(getattr(settings, "INIT_SERVICE_TIMEOUT_SECONDS", 20.0) or 20.0), 1.0)
  step = max(float(getattr(settings, "INIT_SERVICE_STEP_TIMEOUT_SECONDS", 8.0) or 8.0), 0.5)

  async def _run_step(name: str, coro) -> None:
    try:
      await asyncio.wait_for(coro, timeout=step)
    except Exception as error:
      logger.warning(
        f"init_service 步骤 {name} 未完成: {type(error).__name__}: {error}"
      )

  async def _body() -> None:
    etcd_client = await asyncio.wait_for(etcd_op.get_etcd_client(), timeout=step)
    try:
      await _run_step(
        "minio-probe",
        asyncio.to_thread(
          minio_op.test_minio_server,
          host=settings.MINIO_HOST,
          port=settings.MINIO_PORT,
          access_key=settings.MINIO_ACCESS_KEY,
          secret_key=settings.MINIO_SECRET_KEY,
        ),
      )

      await _run_step(
        "register-region",
        etcd_op.merge_update_etcd_key(
          sync_module.ETCD_KEY_REGION,
          lambda data: {**data, settings.REGION: settings.REGION_NAME},
          client=etcd_client,
        ),
      )

      from src.core.crypto import encrypt_server_entry
      entry = encrypt_server_entry({
        "domain": settings.PUBLIC_DOMAIN,
        "host": settings.SERVER_HOST,
        "server_port": settings.SERVER_PORT,
        "minio_port": settings.MINIO_PORT,
        "access_key": settings.MINIO_ACCESS_KEY,
        "secret_key": settings.MINIO_SECRET_KEY,
        "replicate_weight": settings.MINIO_REPLICATE_WEIGHT,
      })
      await _run_step(
        "register-server",
        etcd_op.merge_update_etcd_key(
          sync_module.ETCD_KEY_SERVERS,
          lambda data: {**data, settings.REGION: entry},
          client=etcd_client,
        ),
      )

      await _run_step("publish-roles", sync_module.publish_roles(client=etcd_client))
      await _run_step("publish-users", sync_module.publish_local_users(client=etcd_client))
      await _run_step(
        "bootstrap-topology",
        sync_module.bootstrap_topology_layout(client=etcd_client),
      )
      await _run_step(
        "backfill-quotas",
        sync_module.backfill_application_quotas(client=etcd_client),
      )
      await _run_step(
        "pull-sync",
        sync_module.pull_all_and_sync(client=etcd_client, user_lock_timeout=0),
      )
      logger.info(f"Service Initialized: {settings.REGION_NAME} ({settings.REGION}).")
    finally:
      try:
        await etcd_client.close()
      except Exception:
        pass

  try:
    await asyncio.wait_for(_body(), timeout=overall)
  except asyncio.TimeoutError:
    logger.warning("init_service 总体超时，继续绑定端口")
  except Exception as error:
    logger.warning(f"init_service 失败（服务仍可启动）: {error}")

  await _run_step("mc-aliases-mongo", sync_module.ensure_mc_aliases_from_mongo())
