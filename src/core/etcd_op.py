import aetcd
import asyncio
import inspect
import time
from copy import deepcopy
from typing import Any, Callable
from loguru import logger
from src.configs.configs import settings
from fastapi import Request
from json import loads as json_loads
from json import dumps as json_dumps
from json import JSONDecodeError
from src.core import sync as sync_module

ETCD_PREFIX = "/storagent/"
CAS_MAX_RETRIES = 8
_WATCH_IGNORED_PREFIXES = (
  f"{ETCD_PREFIX}locks/",
  f"{ETCD_PREFIX}quota/",
  f"{ETCD_PREFIX}capacity_planning",
  f"{ETCD_PREFIX}object_archive_policy",
)

_pool_lock: asyncio.Lock | None = None
_pool_lock_loop: asyncio.AbstractEventLoop | None = None
_shared_inner: aetcd.Client | None = None
_shared_wrapper: "_PooledEtcdClient | None" = None
_shared_loop: asyncio.AbstractEventLoop | None = None


def is_stale_etcd_auth_error(error: BaseException) -> bool:
  """True when a pooled channel's auth token was invalidated (rolling restart)."""
  text = " ".join(str(error).split()).lower()
  return "invalid auth token" in text


def is_recoverable_etcd_pool_error(error: BaseException) -> bool:
  """True when replacing the pooled channel is likely to restore Etcd access."""
  if is_stale_etcd_auth_error(error):
    return True
  text = " ".join(str(error).split()).lower()
  markers = (
    "connection refused",
    "connection reset",
    "failed to connect",
    "socket closed",
    "unavailable",
    "statuscode.unavailable",
    "connecterror",
    "goaway",
  )
  return any(item in text for item in markers)


class _PooledEtcdClient:
  """Share one authenticated gRPC channel across request-path Etcd calls.

  Callers historically create-and-close a client per request. Closing the
  pooled inner client would force every subsequent upload to Authenticate
  again, which on a loaded WAN cluster costs seconds.

  After a rolling Etcd restart the cached token becomes ``invalid auth token``.
  Unary RPCs retry once on a freshly authenticated inner client.
  """

  def __init__(self, inner: aetcd.Client):
    self._inner = inner

  def __getattr__(self, name: str):
    attr = getattr(self._inner, name)
    if inspect.isasyncgenfunction(attr):
      return attr
    if not inspect.iscoroutinefunction(attr):
      return attr

    async def _wrapped(*args, **kwargs):
      try:
        return await getattr(self._inner, name)(*args, **kwargs)
      except Exception as error:
        if not is_recoverable_etcd_pool_error(error):
          raise
        logger.warning("etcd 连接池失效，重建请求路径连接: {}", error)
        await refresh_shared_etcd_client()
        return await getattr(self._inner, name)(*args, **kwargs)

    return _wrapped

  async def close(self) -> None:
    return None


def _is_runtime_etcd_key(key: str) -> bool:
  """Return whether a key is ephemeral runtime state, not control-plane data."""
  return key.startswith(_WATCH_IGNORED_PREFIXES)


def _etcd_client_options() -> dict[str, Any]:
  options: dict[str, Any] = {"host": settings.ETCD_HOST, "port": settings.ETCD_PORT}
  username = str(getattr(settings, "ETCD_USERNAME", "") or "")
  password = str(getattr(settings, "ETCD_PASSWORD", "") or "")
  if username or password:
    options.update(username=username, password=password)
  return options


def _current_pool_lock() -> asyncio.Lock:
  global _pool_lock, _pool_lock_loop
  loop = asyncio.get_running_loop()
  if _pool_lock is None or _pool_lock_loop is not loop:
    _pool_lock = asyncio.Lock()
    _pool_lock_loop = loop
  return _pool_lock


async def close_shared_etcd_client() -> None:
  """Drop the request-path pool. Watch connections are owned separately."""
  global _shared_inner, _shared_wrapper, _shared_loop
  async with _current_pool_lock():
    inner = _shared_inner
    _shared_inner = None
    _shared_wrapper = None
    _shared_loop = None
  if inner is None:
    return
  try:
    await inner.close()
  except Exception:
    pass


async def refresh_shared_etcd_client() -> "_PooledEtcdClient":
  """Replace the pooled inner client and keep existing wrapper references."""
  global _shared_inner, _shared_wrapper, _shared_loop
  async with _current_pool_lock():
    stale = _shared_inner
    _shared_inner = aetcd.Client(**_etcd_client_options())
    _shared_loop = asyncio.get_running_loop()
    if _shared_wrapper is None:
      _shared_wrapper = _PooledEtcdClient(_shared_inner)
    else:
      _shared_wrapper._inner = _shared_inner
  if stale is not None:
    try:
      await stale.close()
    except Exception:
      pass
  return _shared_wrapper


async def get_etcd_client(*, dedicated: bool = False) -> aetcd.Client:
  """Return an Etcd client.

  Request traffic reuses one authenticated connection. Pass dedicated=True
  for the watch loop so a reconnect can close that stream without dropping
  in-flight compact admission CAS.
  """
  if dedicated:
    return aetcd.Client(**_etcd_client_options())

  loop = asyncio.get_running_loop()
  async with _current_pool_lock():
    global _shared_inner, _shared_wrapper, _shared_loop
    if (
      _shared_inner is not None
      and _shared_wrapper is not None
      and _shared_loop is loop
    ):
      return _shared_wrapper
    stale = _shared_inner
    _shared_inner = aetcd.Client(**_etcd_client_options())
    _shared_wrapper = _PooledEtcdClient(_shared_inner)
    _shared_loop = loop
  if stale is not None:
    try:
      await stale.close()
    except Exception:
      pass
  return _shared_wrapper


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

  if short_key == sync_module.ETCD_KEY_ROLES:
    await sync_module.sync_roles_to_mongo(data)
  elif short_key == sync_module.ETCD_KEY_USERS:
    await sync_module.sync_users_to_mongo(data)
  elif short_key == sync_module.ETCD_KEY_REGION:
    await sync_module.sync_region_to_mongo(data)
  elif short_key == sync_module.ETCD_KEY_SERVERS:
    await sync_module.sync_servers_to_mongo(data)
    await sync_module.setup_mc_aliases(data)
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
  elif short_key == sync_module.ETCD_KEY_TOPOLOGY_LAYOUT:
    await sync_module.sync_topology_layout_to_mongo(data)


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
  elif short_key in (
    sync_module.ETCD_KEY_ROLES,
    sync_module.ETCD_KEY_USERS,
    sync_module.ETCD_KEY_APPLICATIONS,
    sync_module.ETCD_KEY_API_KEYS,
    sync_module.ETCD_KEY_TOPOLOGY_LAYOUT,
  ):
    logger.warning(f"跳过对 {short_key} 的批量清空，等待显式 PUT 收敛")
  else:
    logger.info(f"未处理的 Etcd DELETE: {short_key}")


async def watch_etcd_task(client: aetcd.Client | None = None):
  """
  增量更新订阅：监听 Etcd 变更并同步到 MongoDB（断线自动重连）
  """
  backoff = 1.0
  while True:
    try:
      if client is None:
        client = await get_etcd_client(dedicated=True)
      encoded_prefix = ETCD_PREFIX.encode()
      logger.info(
        "Etcd watch 已启动（roles / users / region / servers / applications / "
        "api_keys / ai_config / topology_layout）"
      )
      async for event in await client.watch_prefix(encoded_prefix):
        backoff = 1.0
        key = event.kv.key.decode("utf-8")
        if _is_runtime_etcd_key(key):
          continue
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
        if client is not None:
          await client.close()
      except Exception:
        pass
      client = None
      try:
        client = await get_etcd_client(dedicated=True)
      except Exception as ce:
        logger.warning(f"Etcd 客户端重建失败: {ce}")


def _reconcile_lock_key() -> bytes:
  region = str(settings.REGION).strip().lower() or "unknown"
  return f"/storagent/locks/etcd-reconcile/{region}".encode()


async def reconcile_etcd_once() -> dict[str, str]:
  """Run one bounded Etcd-to-Mongo reconciliation pass."""
  client = None
  lock = None
  acquired = False
  try:
      client = await get_etcd_client()
      lock = client.lock(
        _reconcile_lock_key(),
        ttl=max(int(getattr(settings, "SYNC_RECONCILE_LOCK_TTL_SECONDS", 45) or 45), 5),
      )
      acquired = await lock.acquire(timeout=0)
      if not acquired:
        from src.core import metrics as metrics_mod
        metrics_mod.incr("sync_reconcile_skipped_total")
        return {"status": "skipped", "reason": "already-running"}
      if sync_module.is_sync_authority_region():
        await sync_module.publish_roles(client=client)
        await sync_module.publish_local_users(client=client)
        await sync_module.bootstrap_topology_layout(client=client)
        await sync_module.backfill_application_quotas(client=client)
      await sync_module.pull_all_and_sync(client=client, user_lock_timeout=0)
      from src.core import metrics as metrics_mod
      metrics_mod.incr("sync_reconcile_runs_total")
      metrics_mod.set_gauge("sync_last_success_timestamp_seconds", time.time())
      return {"status": "succeeded"}
  except Exception as e:
      from src.core import metrics as metrics_mod
      metrics_mod.incr("sync_reconcile_failures_total")
      metrics_mod.set_gauge("sync_last_failure_timestamp_seconds", time.time())
      logger.warning(f"Etcd 周期全量校准失败: {e}")
      raise
  finally:
    if acquired and lock is not None:
      try:
        await lock.release()
      except Exception:
        pass
    if client is not None:
      try:
        await client.close()
      except Exception:
        pass


async def reconcile_etcd_task():
  """Legacy in-process loop; retained for deployments without Celery."""
  interval = max(float(settings.SYNC_RECONCILE_INTERVAL_SECONDS), 5.0)
  while True:
    try:
      await reconcile_etcd_once()
    except asyncio.CancelledError:
      logger.info("Etcd 周期全量校准已停止")
      raise
    except Exception:
      pass
    await asyncio.sleep(interval)


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
  lease=None,
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
      put_operation = (
        client.transactions.put(full_key, plain)
        if lease is None
        else client.transactions.put(full_key, plain, lease=lease)
      )

      if mod_rev is None:
        # 创建：仅当 create_revision == 0（键不存在）
        status, _ = await client.transaction(
          compare=[client.transactions.create(full_key) == 0],
          success=[put_operation],
          failure=[],
        )
      else:
        status, _ = await client.transaction(
          compare=[client.transactions.mod(full_key) == mod_rev],
          success=[put_operation],
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
