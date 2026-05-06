import aetcd
import asyncio
from loguru import logger
from src.configs.configs import settings
from fastapi import APIRouter, Depends, Request
from json import loads as json_loads
from json import dumps as json_dumps

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

async def watch_etcd_task(client: aetcd.Client):
  """
  增量更新订阅
  """
  try:
    # 使用异步迭代器监听前缀
    encoded_prefix = ETCD_PREFIX.encode()
    logger.info(f"Watching Etcd.")
    async for event in await client.watch_prefix(encoded_prefix):
      key = event.kv.key.decode('utf-8')
      value = event.kv.value.decode('utf-8')
      logger.info(f"Key: {key}, Value: {value}")
  except asyncio.CancelledError:
    logger.error("Etcd watch task cancelled")

async def get_etcd(request: Request) -> aetcd.Client:
  """
  依赖注入: 获取 Etcd 客户端
  """
  return await get_etcd_client()

async def push_to_etcd(
  key: str,
  value: dict,
  client: aetcd.Client = Depends(get_etcd)):
  """
  发送数据到 Etcd
  """
  plain_value = json_dumps(value)
  await client.put(f"{ETCD_PREFIX}{key}".encode(), plain_value.encode())

async def pull_from_etcd_by_prefix(
  prefix: str,
  client: aetcd.Client = Depends(get_etcd)):
  """
  从 Etcd 拉取数据
  """
  response = await client.get_prefix(f"{ETCD_PREFIX}{prefix}".encode())
  data = {}
  for kv in response.kvs:
    value = kv.value.decode('utf-8')
    json_value = json_loads(value)
    data[kv.key.decode('utf-8')] = json_value
  return data

async def pull_from_etcd_by_key(
  key: str,
  client: aetcd.Client = Depends(get_etcd)) -> dict:
  """
  从 Etcd 拉取数据
  """
  response = await client.get(f"{ETCD_PREFIX}{key}".encode())
  if not response:
    return {}
  value = response.value.decode('utf-8')
  json_value = json_loads(value)
  return json_value