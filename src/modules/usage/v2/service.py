"""v2 usage Service for unchanged administrative reporting."""
from src.modules.usage import service as v1_service


async def call(endpoint, *args, **kwargs):
  return await endpoint(*args, **kwargs)
