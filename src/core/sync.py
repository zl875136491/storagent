"""
跨节点数据同步模块

通过 Etcd 在多地 Storagent 节点间同步：
- region / servers（拓扑）
- applications（应用元数据）
- api_keys（API 密钥）
"""
from datetime import datetime
from typing import Any

from loguru import logger

from src.configs.configs import settings
from src.utils.helpers import utc_now

# Etcd 中各 key 的名称
ETCD_KEY_REGION = "region"
ETCD_KEY_SERVERS = "servers"
ETCD_KEY_APPLICATIONS = "applications"
ETCD_KEY_API_KEYS = "api_keys"

SYNC_USER_PLACEHOLDER = "__sync__"


def application_to_etcd_entry(app) -> dict:
  """将 Application 文档序列化为 Etcd 条目"""
  author = app.author
  approver = app.approver
  return {
    "shown_name": app.shown_name,
    "description": app.description,
    "enabled": app.enabled,
    "author_username": author.username if author else "",
    "author_name": author.name if author else "",
    "approver_username": approver.username if approver else "",
    "enabled_at": app.enabled_at.isoformat() if app.enabled_at else None,
    "updated_at": app.updated_at.isoformat() if app.updated_at else None,
    "origin_region": settings.REGION,
  }


def api_key_to_etcd_entry(api_key_obj) -> dict:
  """将 APIKey 文档序列化为 Etcd 条目"""
  app = api_key_obj.application
  author_username = ""
  if app and app.author:
    author_username = app.author.username
  return {
    "app_name": app.name if app else "",
    "author_username": author_username,
    "expired_at": api_key_obj.expired_at.isoformat(),
    "deleted": api_key_obj.deleted,
    "deleted_at": api_key_obj.deleted_at.isoformat() if api_key_obj.deleted_at else None,
    "origin_region": settings.REGION,
  }


async def get_or_create_sync_user(username: str, name: str = "") -> Any:
  """
  获取或创建用于跨节点同步的占位用户（按 username 关联）
  """
  from src.modules.auth import crud as user_crud
  from src.core.auth import get_password_hash

  if not username:
    username = SYNC_USER_PLACEHOLDER
  user = await user_crud.read_user_by_username(username)
  if user:
    return user
  basic_role = await user_crud.get_basic_role()
  roles = [basic_role] if basic_role else []
  return await user_crud.create_user(
    username=username,
    name=name or username,
    hashed_password=get_password_hash("SyncUser!000"),
    roles=roles,
  )


async def upsert_application_from_etcd(app_name: str, data: dict):
  """
  从 Etcd 数据 upsert Application 到本地 MongoDB
  """
  from src.modules.public import crud as public_crud
  from src.modules.public.model import Application

  author_username = data.get("author_username") or SYNC_USER_PLACEHOLDER
  author = await get_or_create_sync_user(author_username, data.get("author_name", ""))

  app_obj = await public_crud.read_application_by_name(app_name)
  enabled_at = None
  if data.get("enabled_at"):
    try:
      enabled_at = datetime.fromisoformat(data["enabled_at"])
    except ValueError:
      pass

  approver = None
  if data.get("approver_username"):
    approver = await get_or_create_sync_user(data["approver_username"])

  if not app_obj:
    enabled = data.get("enabled", False)
    app_obj = Application(
      name=app_name,
      shown_name=data.get("shown_name", app_name),
      description=data.get("description", ""),
      enabled=enabled,
      author=author,
      approver=approver,
      enabled_at=enabled_at if enabled else None,
    )
    await app_obj.save()
    logger.info(f"Etcd sync: 创建 Application {app_name}")
    if enabled:
      await ensure_local_buckets_for_app(app_name)
      from src.modules.storage import crud as storage_crud
      server_names = await storage_crud.read_minio_server_names()
      await setup_bucket_replication(app_name, server_names)
    return app_obj, True

  changed = False
  if app_obj.shown_name != data.get("shown_name", app_obj.shown_name):
    app_obj.shown_name = data.get("shown_name", app_obj.shown_name)
    changed = True
  if app_obj.description != data.get("description", app_obj.description):
    app_obj.description = data.get("description", app_obj.description)
    changed = True
  if data.get("enabled") and not app_obj.enabled:
    app_obj.enabled = True
    app_obj.enabled_at = enabled_at or utc_now()
    if approver:
      app_obj.approver = approver
    changed = True
    newly_enabled = True
  else:
    newly_enabled = False
  if changed:
    app_obj.updated_at = utc_now()
    await app_obj.save()
    logger.info(f"Etcd sync: 更新 Application {app_name}")
    if newly_enabled:
      await ensure_local_buckets_for_app(app_name)
      from src.modules.storage import crud as storage_crud
      server_names = await storage_crud.read_minio_server_names()
      await setup_bucket_replication(app_name, server_names)
  return app_obj, changed


async def upsert_api_key_from_etcd(key: str, data: dict):
  """
  从 Etcd 数据 upsert APIKey 到本地 MongoDB
  """
  from src.modules.public import crud as public_crud

  app_name = data.get("app_name")
  if not app_name:
    return None

  app_obj, _ = await upsert_application_from_etcd(app_name, {
    "shown_name": app_name,
    "description": "",
    "enabled": True,
    "author_username": data.get("author_username", SYNC_USER_PLACEHOLDER),
  })
  if not app_obj.enabled:
    app_obj.enabled = True
    app_obj.enabled_at = utc_now()
    await app_obj.save()

  expired_at = datetime.fromisoformat(data["expired_at"]) if data.get("expired_at") else utc_now()
  deleted = data.get("deleted", False)

  existing = await public_crud.read_api_key_by_key_including_deleted(key)
  if existing:
    if deleted and not existing.deleted:
      existing.deleted = True
      existing.deleted_at = utc_now()
      await existing.save()
      logger.info(f"Etcd sync: 吊销 API Key {key[:8]}...")
    return existing

  if deleted:
    return None

  api_key_obj = await public_crud.create_api_key(app_obj, key, expired_at)
  logger.info(f"Etcd sync: 创建 API Key {key[:8]}... (app={app_name})")
  return api_key_obj


async def sync_applications_to_mongo(applications_data: dict):
  """批量同步 applications 到 MongoDB"""
  for app_name, app_data in applications_data.items():
    try:
      await upsert_application_from_etcd(app_name, app_data)
    except Exception as e:
      logger.warning(f"同步 Application {app_name} 失败: {e}")


async def sync_api_keys_to_mongo(api_keys_data: dict):
  """批量同步 api_keys 到 MongoDB"""
  for key, key_data in api_keys_data.items():
    try:
      await upsert_api_key_from_etcd(key, key_data)
    except Exception as e:
      logger.warning(f"同步 API Key 失败: {e}")


async def sync_region_to_mongo(region_data: dict):
  """同步 region 到 MongoDB（含 shown_name 更新）"""
  from src.modules.public import crud as public_crud

  for region_value, region_name in region_data.items():
    region_obj = await public_crud.read_region_by_name(region_value)
    if not region_obj:
      await public_crud.create_region(region_value, region_name)
      logger.info(f"Etcd sync: 创建 Region {region_value}")
    elif region_obj.shown_name != region_name:
      region_obj.shown_name = region_name
      await region_obj.save()
      logger.info(f"Etcd sync: 更新 Region {region_value} shown_name")


async def sync_servers_to_mongo(servers_data: dict) -> list[str]:
  """
  同步 servers 到 MongoDB，返回新发现的服务器名称列表
  """
  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud

  new_servers: list[str] = []
  for server_region_name, server_data in servers_data.items():
    if any(k not in server_data for k in (
      "host", "server_port", "minio_port", "access_key", "secret_key", "replicate_weight"
    )):
      continue
    region_obj = await public_crud.read_region_by_name(server_region_name)
    if not region_obj:
      region_obj = await public_crud.create_region(server_region_name, server_region_name)
    server_obj = await storage_crud.read_minio_server_by_region(region_obj)
    host = server_data.get("host", settings.SERVER_HOST)
    if not server_obj:
      await storage_crud.create_minio_server(
        region=region_obj,
        name=server_region_name,
        host=host,
        server_port=server_data["server_port"],
        minio_port=server_data["minio_port"],
        access_key=server_data["access_key"],
        secret_key=server_data["secret_key"],
        replicate_weight=server_data["replicate_weight"],
      )
      new_servers.append(server_region_name)
      logger.info(f"Etcd sync: 创建 MinIO 服务器 {server_region_name}")
    else:
      await storage_crud.update_minio_server(
        minio_server=server_obj,
        host=host,
        server_port=server_data["server_port"],
        minio_port=server_data["minio_port"],
        access_key=server_data["access_key"],
        secret_key=server_data["secret_key"],
        replicate_weight=server_data["replicate_weight"],
      )
  return new_servers


async def setup_mc_aliases(servers_data: dict):
  """为所有已知 server 设置 mc alias"""
  from src.core import minio_op

  for server_region_name, server_data in servers_data.items():
    if any(k not in server_data for k in ("host", "minio_port", "access_key", "secret_key")):
      continue
    host = server_data.get("host", settings.SERVER_HOST)
    success, res = await minio_op.set_site_alias(
      site_name=server_region_name,
      endpoint=f"{host}:{server_data['minio_port']}",
      admin_user=server_data["access_key"],
      admin_password=server_data["secret_key"],
    )
    if not success:
      logger.warning(f"mc alias 设置失败 {server_region_name}: {res}")


async def join_site_replication_for_new_servers(new_server_names: list[str]):
  """
  将新发现的远端 server 加入 MinIO Site Replication
  """
  from src.core import minio_op
  from src.modules.storage import crud as storage_crud

  if not new_server_names:
    return
  master = await storage_crud.read_master_minio_server()
  if not master:
    return
  master_region = master.region
  master_name = master_region.name if hasattr(master_region, "name") else master.name
  for site_name in new_server_names:
    if site_name == settings.REGION:
      continue
    success, res = await minio_op.add_new_site(master_name, site_name)
    if success:
      logger.info(f"Site Replication: {site_name} 已加入 {master_name}")
    else:
      logger.warning(f"Site Replication 加入失败 {site_name}: {res}")


async def setup_bucket_replication(bucket_name: str, server_names: list[str] | None = None):
  """
  为指定 bucket 在所有 server 之间配置 bucket-level replication
  """
  from src.core import minio_op
  from src.modules.storage import crud as storage_crud

  if server_names is None:
    server_names = await storage_crud.read_minio_server_names()
  if len(server_names) < 2:
    return

  for from_server in server_names:
    for to_server in server_names:
      if from_server == to_server:
        continue
      success, err = await minio_op.create_bucket_replicate(from_server, to_server, bucket_name)
      if success:
        logger.info(f"Bucket Replication: {from_server}/{bucket_name} -> {to_server}")
      else:
        logger.warning(f"Bucket Replication 失败 {from_server}->{to_server}/{bucket_name}: {err}")


async def ensure_local_buckets_for_app(app_name: str):
  """
  远端同步 enabled 应用后，在本地 MinIO 上确保 bucket 存在并开启版本控制
  """
  from src.core import minio_op
  from src.modules.storage import crud as storage_crud

  local_server = await storage_crud.read_minio_server_by_region_name(settings.REGION)
  if not local_server:
    return
  server_name = local_server.name
  existed = await minio_op.check_server_bucket_existed(server_name, app_name)
  if not existed:
    success, err = await minio_op.create_bucket(server_name, app_name)
    if not success:
      logger.warning(f"本地建桶失败 {app_name}: {err}")
      return
  await minio_op.enable_bucket_versioning(server_name, app_name)


async def publish_application(app) -> None:
  """将 Application 发布到 Etcd"""
  from src.core import etcd_op
  from src.modules.public import crud as public_crud

  app = await public_crud.read_application_by_id(app.id)
  if not app:
    return
  client = await etcd_op.get_etcd_client()
  try:
    data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_APPLICATIONS, client=client)
    data[app.name] = application_to_etcd_entry(app)
    await etcd_op.push_to_etcd(ETCD_KEY_APPLICATIONS, data, client=client)
  finally:
    await client.close()


async def publish_api_key(api_key_obj) -> None:
  """将 API Key 发布到 Etcd"""
  from src.core import etcd_op
  from src.modules.public import crud as public_crud

  api_key_obj = await public_crud.read_api_key_by_key_including_deleted(api_key_obj.key)
  if not api_key_obj:
    return
  client = await etcd_op.get_etcd_client()
  try:
    data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_API_KEYS, client=client)
    data[api_key_obj.key] = api_key_to_etcd_entry(api_key_obj)
    await etcd_op.push_to_etcd(ETCD_KEY_API_KEYS, data, client=client)
  finally:
    await client.close()


async def publish_region(region_name: str, shown_name: str) -> None:
  """将 Region 发布到 Etcd"""
  from src.core import etcd_op

  client = await etcd_op.get_etcd_client()
  try:
    data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_REGION, client=client)
    data[region_name] = shown_name
    await etcd_op.push_to_etcd(ETCD_KEY_REGION, data, client=client)
  finally:
    await client.close()


async def publish_servers() -> None:
  """将当前节点 server 条目合并发布到 Etcd"""
  from src.core import etcd_op

  client = await etcd_op.get_etcd_client()
  try:
    data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_SERVERS, client=client)
    data[settings.REGION] = {
      "host": settings.SERVER_HOST,
      "server_port": settings.SERVER_PORT,
      "minio_port": settings.MINIO_PORT,
      "access_key": settings.MINIO_ACCESS_KEY,
      "secret_key": settings.MINIO_SECRET_KEY,
      "replicate_weight": settings.MINIO_REPLICATE_WEIGHT,
    }
    await etcd_op.push_to_etcd(ETCD_KEY_SERVERS, data, client=client)
  finally:
    await client.close()


async def publish_server_entry(
  region_name: str,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int,
) -> None:
  """将指定 server 条目合并发布到 Etcd"""
  from src.core import etcd_op

  client = await etcd_op.get_etcd_client()
  try:
    data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_SERVERS, client=client)
    data[region_name] = {
      "host": host,
      "server_port": server_port,
      "minio_port": minio_port,
      "access_key": access_key,
      "secret_key": secret_key,
      "replicate_weight": replicate_weight,
    }
    await etcd_op.push_to_etcd(ETCD_KEY_SERVERS, data, client=client)
  finally:
    await client.close()


async def pull_all_and_sync(client=None):
  """
  从 Etcd 全量拉取并同步到本地 MongoDB（启动时调用）
  """
  from src.core import etcd_op

  own_client = client is None
  if own_client:
    client = await etcd_op.get_etcd_client()
  try:
    region_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_REGION, client=client)
    if region_data:
      await sync_region_to_mongo(region_data)

    servers_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_SERVERS, client=client)
    if servers_data:
      new_servers = await sync_servers_to_mongo(servers_data)
      await setup_mc_aliases(servers_data)
      await join_site_replication_for_new_servers(new_servers)

    apps_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_APPLICATIONS, client=client)
    if apps_data:
      await sync_applications_to_mongo(apps_data)
      for app_name, app_data in apps_data.items():
        if app_data.get("enabled"):
          await ensure_local_buckets_for_app(app_name)

    keys_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_API_KEYS, client=client)
    if keys_data:
      await sync_api_keys_to_mongo(keys_data)
  finally:
    if own_client:
      await client.close()
