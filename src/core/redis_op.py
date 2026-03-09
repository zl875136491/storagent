# Redis 发布
# 将 APIKey 发到 Redis 中, 供所有的 Storageo 节点订阅

import redis
from src.configs.configs import settings

class RedisOp:
  def __init__(self):
    self.redis_client = redis.Redis(
      host=settings.REDIS_HOST,
      port=settings.REDIS_PORT,
      db=settings.REDIS_DB,
      password=settings.REDIS_PASSWORD,
    )
  
  async def __aenter__(self):
    return self
  
  async def __aexit__(self, exc_type, exc_value, traceback):
    await self.redis_client.close()
  
  async def publish_api_key(self, api_key: str):
    """
    发布 APIKey 到 Redis 中
    """
    await self.redis_client.publish(settings.REDIS_API_KEY_CHANNEL, api_key)