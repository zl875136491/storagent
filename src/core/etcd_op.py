import aetcd
import asyncio
from loguru import logger
from src.configs.configs import settings

async def get_etcd_client() -> aetcd.Client:
  client = aetcd.Client(
    host=settings.ETCD_HOST,
    port=settings.ETCD_PORT,
    username=settings.ETCD_USERNAME,
    password=settings.ETCD_PASSWORD
  )
  return client

async def watch_etcd_task(client: aetcd.Client):
  try:
    # 使用异步迭代器监听前缀
    encoded_prefix = "/config/".encode()
    logger.info(f"Watching Etcd.")
    async for event in await client.watch_prefix(encoded_prefix):
      key = event.kv.key.decode('utf-8')
      value = event.kv.value.decode('utf-8')
      logger.info(f"Key: {key}, Value: {value}")
  except asyncio.CancelledError:
    logger.error("Etcd watch task cancelled")