"""v2 auth Service for inherited authentication operations."""
from src.modules.auth import service as v1_service


async def call(endpoint, *args, **kwargs):
  return await endpoint(*args, **kwargs)
