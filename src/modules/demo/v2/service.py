"""v2 console demo Service for the inherited opaque API-key flow."""
from src.modules.demo import route as v1_route


async def call(endpoint, *args, **kwargs):
  return await endpoint(*args, **kwargs)
