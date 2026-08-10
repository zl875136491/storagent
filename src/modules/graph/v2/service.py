"""v2 graph Service for unchanged topology operations."""
from src.modules.graph import service as v1_service


async def call(endpoint, *args, **kwargs):
  return await endpoint(*args, **kwargs)
