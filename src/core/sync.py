"""
跨节点数据同步模块

通过 Etcd 在多地 Storagent 节点间同步：
- region / servers（拓扑；MinIO 凭证加密存储）
- applications（应用元数据）
- api_keys（API 密钥；字典键为哈希，值为加密后的 Key）
"""
import secrets
from datetime import datetime
from typing import Any

from loguru import logger

from src.configs.configs import settings
from src.core.crypto import (
  api_key_etcd_map_key,
  decrypt_secret,
  decrypt_server_entry,
  encrypt_secret,
  encrypt_server_entry,
)
from src.utils.helpers import utc_now

ETCD_KEY_REGION = "region"
ETCD_KEY_SERVERS = "servers"
ETCD_KEY_APPLICATIONS = "applications"
ETCD_KEY_API_KEYS = "api_keys"
ETCD_KEY_REVOKED_TOKENS = "revoked_tokens"
ETCD_KEY_AI_CONFIG = "ai_config"

SYNC_USER_PLACEHOLDER = "__sync__"


def application_to_etcd_entry(app) -> dict:
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
  from src.core.crypto import is_sha256_hex

  app = api_key_obj.application
  author_username = ""
  if app and app.author:
    author_username = app.author.username
  if getattr(api_key_obj, "key_enc", None):
    key_enc = api_key_obj.key_enc
    if not key_enc.startswith("enc:v1:"):
      key_enc = encrypt_secret(key_enc)
  elif not is_sha256_hex(api_key_obj.key):
    key_enc = encrypt_secret(api_key_obj.key)
  else:
    raise ValueError("API Key 缺少 key_enc，无法同步到 Etcd")
  return {
    "key_enc": key_enc,
    "app_name": app.name if app else "",
    "author_username": author_username,
    "expired_at": api_key_obj.expired_at.isoformat(),
    "deleted": api_key_obj.deleted,
    "deleted_at": api_key_obj.deleted_at.isoformat() if api_key_obj.deleted_at else None,
    "destory_by_admin": bool(getattr(api_key_obj, "destory_by_admin", False)),
    "origin_region": settings.REGION,
  }


def _resolve_api_key_plaintext(map_key: str, data: dict) -> str | None:
  """兼容新旧格式：新格式用 key_enc；旧格式 map_key 即为明文 Key。"""
  if data.get("key_enc"):
    try:
      return decrypt_secret(data["key_enc"])
    except ValueError as e:
      logger.warning(f"API Key 解密失败: {e}")
      return None
  if len(map_key) >= 16 and "app_name" in data:
    return map_key
  return None


async def get_or_create_sync_user(username: str, name: str = "") -> Any:
  """
  获取或创建跨节点同步占位用户。
  使用随机不可猜密码，并标记 is_sync=True 禁止登录。
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
    hashed_password=get_password_hash(secrets.token_urlsafe(48)),
    roles=roles,
    is_sync=True,
  )


async def upsert_application_from_etcd(app_name: str, data: dict):
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
  elif data.get("enabled") is False and app_obj.enabled:
    app_obj.enabled = False
    changed = True
    newly_enabled = False
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
  from src.modules.public import crud as public_crud

  app_name = data.get("app_name")
  if not app_name or not key:
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
      existing.destory_by_admin = bool(data.get("destory_by_admin", False))
      await existing.save()
      logger.info(f"Etcd sync: 吊销 API Key {key[:8]}...")
    elif deleted and existing.deleted:
      flag = bool(data.get("destory_by_admin", False))
      if getattr(existing, "destory_by_admin", False) != flag:
        existing.destory_by_admin = flag
        await existing.save()
    return existing

  if deleted:
    return None

  api_key_obj = await public_crud.create_api_key(app_obj, key, expired_at)
  logger.info(f"Etcd sync: 创建 API Key {key[:8]}... (app={app_name})")
  return api_key_obj


async def sync_applications_to_mongo(applications_data: dict):
  for app_name, app_data in applications_data.items():
    try:
      await upsert_application_from_etcd(app_name, app_data)
    except Exception as e:
      logger.warning(f"同步 Application {app_name} 失败: {e}")


async def sync_api_keys_to_mongo(api_keys_data: dict):
  for map_key, key_data in api_keys_data.items():
    try:
      plain_key = _resolve_api_key_plaintext(map_key, key_data)
      if not plain_key:
        continue
      await upsert_api_key_from_etcd(plain_key, key_data)
    except Exception as e:
      logger.warning(f"同步 API Key 失败: {e}")


async def sync_region_to_mongo(region_data: dict):
  from src.modules.public import crud as public_crud
  from src.modules.public.model import Region

  for region_value, region_name in region_data.items():
    region_obj = await public_crud.read_region_by_name(region_value)
    if not region_obj:
      await public_crud.create_region(region_value, region_name)
      logger.info(f"Etcd sync: 创建 Region {region_value}")
    elif region_obj.shown_name != region_name:
      region_obj.shown_name = region_name
      await region_obj.save()
      logger.info(f"Etcd sync: 更新 Region {region_value} shown_name")

  # 收敛：Etcd 中已移除的远程 Region（保留本节点 REGION）
  known = set(region_data.keys())
  for region_obj in await Region.find_all().to_list():
    if region_obj.name == settings.REGION:
      continue
    if region_obj.name not in known:
      await region_obj.delete()
      logger.info(f"Etcd sync: 移除已下线 Region {region_obj.name}")


async def sync_servers_to_mongo(servers_data: dict) -> list[str]:
  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud

  new_servers: list[str] = []
  for server_region_name, raw in servers_data.items():
    try:
      server_data = decrypt_server_entry(raw)
    except ValueError as e:
      logger.warning(f"解密 server {server_region_name} 失败: {e}")
      continue
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

  # 收敛：移除 Etcd 中已不存在的远程 MinIO（保留本节点）
  known = set(servers_data.keys())
  for server_obj in await storage_crud.read_minio_server_list():
    region = server_obj.region
    region_name = region.name if region and hasattr(region, "name") else server_obj.name
    if region_name == settings.REGION:
      continue
    if region_name not in known:
      await server_obj.delete()
      logger.info(f"Etcd sync: 移除已下线 MinIO 服务器 {region_name}")

  return new_servers


async def unpublish_region(region_name: str) -> None:
  """从 Etcd region map 移除区域（跨节点下线）。"""
  from src.core import etcd_op

  def mutator(data: dict) -> dict:
    data.pop(region_name, None)
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_REGION, mutator)


async def unpublish_server(region_name: str) -> None:
  """从 Etcd servers map 移除服务点。"""
  from src.core import etcd_op

  def mutator(data: dict) -> dict:
    data.pop(region_name, None)
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_SERVERS, mutator)


async def setup_mc_aliases(servers_data: dict):
  from src.core import minio_op

  for server_region_name, raw in servers_data.items():
    try:
      server_data = decrypt_server_entry(raw)
    except ValueError:
      continue
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


async def setup_bucket_replication(bucket_name: str, server_names: list[str] | None = None):
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
  from src.core import etcd_op
  from src.modules.public import crud as public_crud

  app = await public_crud.read_application_by_id(app.id)
  if not app:
    return
  entry = application_to_etcd_entry(app)
  name = app.name

  def mutator(data: dict) -> dict:
    data[name] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_APPLICATIONS, mutator)


async def publish_api_key(api_key_obj) -> None:
  from src.core import etcd_op
  from src.core.crypto import api_key_etcd_map_key, is_sha256_hex
  from src.modules.public import crud as public_crud

  api_key_obj = await public_crud.read_api_key_by_id(api_key_obj.id)
  if not api_key_obj:
    return
  map_key = api_key_obj.key if is_sha256_hex(api_key_obj.key) else api_key_etcd_map_key(api_key_obj.key)
  entry = api_key_to_etcd_entry(api_key_obj)

  def mutator(data: dict) -> dict:
    data[map_key] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_API_KEYS, mutator)


async def publish_ai_config(config: dict) -> None:
  """Publish the encrypted AI provider configuration to every region."""
  from src.core import etcd_op

  def mutator(_current: dict) -> dict:
    return dict(config)

  await etcd_op.merge_update_etcd_key(ETCD_KEY_AI_CONFIG, mutator)


async def sync_ai_config_to_mongo(config: dict) -> None:
  if not isinstance(config, dict) or not config:
    return
  from src.modules.ai import crud as ai_crud

  await ai_crud.upsert_config(config)




async def publish_revoked_token(token_hash: str, expired_at: datetime) -> None:
  """跨区同步 JWT 吊销（仅同步哈希，不落明文 token）。"""
  from src.core import etcd_op

  entry = {
    "expired_at": expired_at.isoformat(),
    "origin_region": settings.REGION,
  }

  def mutator(data: dict) -> dict:
    data[token_hash] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_REVOKED_TOKENS, mutator)


async def sync_revoked_tokens_to_mongo(revoked_data: dict) -> None:
  from src.modules.auth.model import DestoryedToken

  for token_hash, meta in revoked_data.items():
    if not token_hash or not isinstance(meta, dict):
      continue
    existing = await DestoryedToken.find_one(DestoryedToken.token_hash == token_hash)
    if existing:
      continue
    expired_at = utc_now()
    raw_exp = meta.get("expired_at")
    if raw_exp:
      try:
        expired_at = datetime.fromisoformat(raw_exp)
      except ValueError:
        pass
    await DestoryedToken(token="", token_hash=token_hash, expired_at=expired_at).save()
    logger.info(f"Etcd sync: 导入吊销 token_hash {token_hash[:12]}...")


async def publish_region(region_name: str, shown_name: str) -> None:
  from src.core import etcd_op

  def mutator(data: dict) -> dict:
    data[region_name] = shown_name
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_REGION, mutator)


async def publish_servers() -> None:
  from src.core import etcd_op

  entry = encrypt_server_entry({
    "host": settings.SERVER_HOST,
    "server_port": settings.SERVER_PORT,
    "minio_port": settings.MINIO_PORT,
    "access_key": settings.MINIO_ACCESS_KEY,
    "secret_key": settings.MINIO_SECRET_KEY,
    "replicate_weight": settings.MINIO_REPLICATE_WEIGHT,
  })
  region = settings.REGION

  def mutator(data: dict) -> dict:
    data[region] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_SERVERS, mutator)


async def publish_server_entry(
  region_name: str,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int,
) -> None:
  from src.core import etcd_op

  entry = encrypt_server_entry({
    "host": host,
    "server_port": server_port,
    "minio_port": minio_port,
    "access_key": access_key,
    "secret_key": secret_key,
    "replicate_weight": replicate_weight,
  })

  def mutator(data: dict) -> dict:
    data[region_name] = entry
    return data

  await etcd_op.merge_update_etcd_key(ETCD_KEY_SERVERS, mutator)


async def pull_all_and_sync(client=None):
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
      await sync_servers_to_mongo(servers_data)
      await setup_mc_aliases(servers_data)

    apps_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_APPLICATIONS, client=client)
    if apps_data:
      await sync_applications_to_mongo(apps_data)
      for app_name, app_data in apps_data.items():
        if app_data.get("enabled"):
          await ensure_local_buckets_for_app(app_name)

    keys_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_API_KEYS, client=client)
    if keys_data:
      await sync_api_keys_to_mongo(keys_data)

    revoked_data = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_REVOKED_TOKENS, client=client)
    if revoked_data:
      await sync_revoked_tokens_to_mongo(revoked_data)

    ai_config = await etcd_op.pull_from_etcd_by_key(ETCD_KEY_AI_CONFIG, client=client)
    if ai_config:
      await sync_ai_config_to_mongo(ai_config)
  finally:
    if own_client:
      await client.close()
