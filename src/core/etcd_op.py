import aetcd
import asyncio
from copy import deepcopy
from typing import Callable
from loguru import logger
from src.configs.configs import settings
from fastapi import Request
from json import loads as json_loads
from json import dumps as json_dumps
from json import JSONDecodeError
from src.core import sync as sync_module

ETCD_PREFIX = "/storagent/"
CAS_MAX_RETRIES = 8


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
  except (JSONDecodeError, TypeError):
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
  elif short_key == sync_module.ETCD_KEY_REVOKED_TOKENS:
    await sync_module.sync_revoked_tokens_to_mongo(data)
  elif short_key == sync_module.ETCD_KEY_AI_CONFIG:
    await sync_module.sync_ai_config_to_mongo(data)


async def _handle_etcd_delete(key: str):
  """
  处理 Etcd DELETE：整 key 被删时，按空 map 收敛拓扑；应用/密钥仅告警不批量清空
  """
  short_key = key.replace(ETCD_PREFIX, "")
  logger.warning(f"Etcd key 已删除: {short_key}")
  if short_key == sync_module.ETCD_KEY_REGION:
    await sync_module.sync_region_to_mongo({})
  elif short_key == sync_module.ETCD_KEY_SERVERS:
    await sync_module.sync_servers_to_mongo({})
  elif short_key in (sync_module.ETCD_KEY_APPLICATIONS, sync_module.ETCD_KEY_API_KEYS):
    logger.warning(f"跳过对 {short_key} 的批量清空，等待显式 PUT 收敛")
  else:
    logger.info(f"未处理的 Etcd DELETE: {short_key}")


async def watch_etcd_task(client: aetcd.Client):
  """
  增量更新订阅：监听 Etcd 变更并同步到 MongoDB（断线自动重连）
  """
  backoff = 1.0
  while True:
    try:
      encoded_prefix = ETCD_PREFIX.encode()
      logger.info("Etcd watch 已启动（region / servers / applications / api_keys / ai_config）")
      async for event in await client.watch_prefix(encoded_prefix):
        backoff = 1.0
        key = event.kv.key.decode("utf-8")
        value = event.kv.value.decode("utf-8") if event.kv.value else ""
        kind = getattr(event, "kind", "PUT")
        logger.info(f"Etcd 变更: {kind} {key}")
        try:
          if kind == "DELETE":
            await _handle_etcd_delete(key)
          else:
            await _handle_etcd_put(key, value)
        except Exception as e:
          logger.warning(f"Etcd 事件处理失败: {e}")
    except asyncio.CancelledError:
      logger.info("Etcd watch 已停止")
      raise
    except Exception as e:
      logger.warning(f"Etcd watch 异常，{backoff:.0f}s 后重连: {e}")
      from src.core import metrics as metrics_mod
      metrics_mod.incr("etcd_watch_reconnects_total")
      await asyncio.sleep(backoff)
      backoff = min(backoff * 2, 30.0)
      try:
        await client.close()
      except Exception:
        pass
      try:
        client = await get_etcd_client()
      except Exception as ce:
        logger.warning(f"Etcd 客户端重建失败: {ce}")


async def get_etcd(request: Request) -> aetcd.Client:
  return await get_etcd_client()


async def push_to_etcd(
  key: str,
  value: dict,
  client: aetcd.Client | None = None):
  """
  无条件覆盖写入（启动注册等场景可用；业务更新请用 merge_update_etcd_key）
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
    return json_loads(value)
  finally:
    if should_close:
      await client.close()


async def pull_from_etcd_by_key_with_rev(
  key: str,
  client: aetcd.Client,
) -> tuple[dict, int | None]:
  """
  拉取单 key，返回 (dict, mod_revision)。
  key 不存在时返回 ({}, None)。
  """
  response = await client.get(f"{ETCD_PREFIX}{key}".encode())
  if not response:
    return {}, None
  value = response.value.decode("utf-8")
  return json_loads(value), response.mod_revision


async def merge_update_etcd_key(
  key: str,
  mutator: Callable[[dict], dict],
  client: aetcd.Client | None = None,
  max_retries: int = CAS_MAX_RETRIES,
) -> dict:
  """
  基于 mod_revision 的 compare-and-swap 合并更新，避免多节点互相覆盖。

  mutator(current_dict) -> new_dict
  """
  if client is None:
    client = await get_etcd_client()
    should_close = True
  else:
    should_close = False

  full_key = f"{ETCD_PREFIX}{key}".encode()
  try:
    last_err = None
    for attempt in range(max_retries):
      current, mod_rev = await pull_from_etcd_by_key_with_rev(key, client=client)
      base = deepcopy(current) if current else {}
      updated = mutator(base)
      plain = json_dumps(updated).encode()

      if mod_rev is None:
        # 创建：仅当 create_revision == 0（键不存在）
        status, _ = await client.transaction(
          compare=[client.transactions.create(full_key) == 0],
          success=[client.transactions.put(full_key, plain)],
          failure=[],
        )
      else:
        status, _ = await client.transaction(
          compare=[client.transactions.mod(full_key) == mod_rev],
          success=[client.transactions.put(full_key, plain)],
          failure=[],
        )

      if status:
        return updated

      last_err = f"CAS conflict on {key} (attempt {attempt + 1}/{max_retries})"
      logger.warning(last_err)
      from src.core import metrics as metrics_mod
      metrics_mod.incr("etcd_cas_conflicts_total")
      await asyncio.sleep(0.05 * (attempt + 1))

    raise RuntimeError(last_err or f"Etcd CAS failed for {key}")
  finally:
    if should_close:
      await client.close()
