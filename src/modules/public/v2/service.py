"""v2 public Service: unchanged public behavior delegates to v1 Service."""
from src.modules.public import service as v1_service


async def call(endpoint, *args, **kwargs):
  """Keep the v2 route from importing v1 Service directly."""
  return await endpoint(*args, **kwargs)
