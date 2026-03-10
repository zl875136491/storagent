# Redis 发布
# 将 APIKey 发到 Redis 中, 供所有的 Storageo 节点订阅

import json
import redis
from datetime import datetime
from typing import Callable, AsyncIterator
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
    self.redis_client.close()
  
  async def publish_api_key_create_patch(
    self,
    api_key: str,
    app_name: str,
    expired_at: datetime):
    """
    发布 APIKey 创建变更
    """
    expired_at_str = expired_at.isoformat()
    message = {
      "type": "api_key",
      "action": "create",
      "api_key": api_key,
      "app": app_name,
      "expired_at": expired_at_str,
    }
    message = json.dumps(message)
    self.redis_client.xadd(
      name=settings.REDIS_API_KEY_CHANNEL,
      fields={"message": message},
      maxlen=1000,
      # approximate=True,
    )

  async def publish_api_key_delete_patch(
    self,
    api_key: str) -> None:
    """
    发布 APIKey 删除变更
    """
    message = {
      "type": "api_key",
      "action": "delete",
      "api_key": api_key,
      "expired_at": "",
      "app": "",
    }
    message = json.dumps(message)
    self.redis_client.xadd(
      name=settings.REDIS_API_KEY_CHANNEL,
      fields={"message": message},
      maxlen=1000,
      # approximate=True,
    )
  
  async def subscribe_file_meta_patch(
    self,
    callback: Callable[[str, str, str, str], None]) -> AsyncIterator[dict]:
    """
    订阅文件元数据变更
    """
    async for message in self.redis_client.xread(
      streams={settings.REDIS_FILE_CHANNEL: "0-0"},
      block=0,
    ):
      message = json.loads(message["message"])
      yield message