import aetcd
import asyncio
from loguru import logger
from src.configs.configs import settings
from fastapi import Depends, Request
from json import loads as json_loads
from json import dumps as json_dumps
from src.core import sync as sync_module

ETCD_PREFIX = "/storagent/"

async def get_etcd_client() -> aetcd.Client:
  """
  获取 Etcd 客户端
  """
  client = aetcd.Client(
    host=settings.ETCD_HOST,
    port=settings.ETCD_PORT,
    username=settings.ETCD_USERNAME,
    password=settings.ETCD_PASSWORD
  )
  return client

async def _handle_etcd_put(key: str, value: str):
  """
  处理 Etcd PUT 事件
  """
  short_key = key.replace(ETCD_PREFIX, "")
  if not value:
    return
  try:
    data = json_loads(value)
  except (json.JSONDecodeError, TypeError):
    logger.warning(f"Etcd 事件值解析失败: key={key}")
    return

  if short_key == sync_module.ETCD_KEY_REGION:
    await sync_module.sync_region_to_mongo(data)
  elif short_key == sync_module.ETCD_KEY_SERVERS:
    new_servers = await sync_module.sync_servers_to_mongo(data)
    await sync_module.setup_mc_aliases(data)
    await sync_module.join_site_replication_for_new_servers(new_servers)
  elif short_key == sync_module.ETCD_KEY_APPLICATIONS:
    await sync_module.sync_applications_to_mongo(data)
    for app_name, app_data in data.items():
      if app_data.get("enabled"):
        await sync_module.ensure_local_buckets_for_app(app_name)
  elif short_key == sync_module.ETCD_KEY_API_KEYS:
    await sync_module.sync_api_keys_to_mongo(data)

async def watch_etcd_task(client: aetcd.Client):
  """
  增量更新订阅：监听 Etcd 变更并同步到 MongoDB
  """
  try:
    encoded_prefix = ETCD_PREFIX.encode()
    logger.info("Etcd watch 已启动（region / servers / applications / api_keys）")
    async for event in await client.watch_prefix(encoded_prefix):
      key = event.kv.key.decode("utf-8")
      value = event.kv.value.decode("utf-8") if event.kv.value else ""
      logger.info(f"Etcd 变更: {key}")
      try:
        await _handle_etcd_put(key, value)
      except Exception as e:
        logger.warning(f"Etcd 事件处理失败: {e}")
  except asyncio.CancelledError:
    logger.info("Etcd watch 已停止")

async def get_etcd(request: Request) -> aetcd.Client:
  """
  依赖注入: 获取 Etcd 客户端
  """
  return await get_etcd_client()

async def push_to_etcd(
  key: str,
  value: dict,
  client: aetcd.Client | None = None):
  """
  发送数据到 Etcd
  """
  if client is None:
    client = await get_etcd_client()
    should_close = True
  else:
    should_close = False
  try:
    plain_value = json_dumps(value)
    await client.put(f"{ETCD_PREFIX}{key}".encode(), plain_value.encode())
  finally:
    if should_close:
      await client.close()

async def pull_from_etcd_by_prefix(
  prefix: str,
  client: aetcd.Client | None = None):
  """
  从 Etcd 拉取前缀数据
  """
  if client is None:
    client = await get_etcd_client()
    should_close = True
  else:
    should_close = False
  try:
    response = await client.get_prefix(f"{ETCD_PREFIX}{prefix}".encode())
    data = {}
    for kv in response.kvs:
      value = kv.value.decode("utf-8")
      json_value = json_loads(value)
      data[kv.key.decode("utf-8")] = json_value
    return data
  finally:
    if should_close:
      await client.close()

async def pull_from_etcd_by_key(
  key: str,
  client: aetcd.Client | None = None) -> dict:
  """
  从 Etcd 拉取单 key 数据
  """
  if client is None:
    client = await get_etcd_client()
    should_close = True
  else:
    should_close = False
  try:
    response = await client.get(f"{ETCD_PREFIX}{key}".encode())
    if not response:
      return {}
    value = response.value.decode("utf-8")
    json_value = json_loads(value)
    return json_value
  finally:
    if should_close:
      await client.close()
