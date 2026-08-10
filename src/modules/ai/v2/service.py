"""v2 AI Service; implementation remains disabled or unchanged upstream."""
from src.modules.ai import service as v1_service


async def call(endpoint, *args, **kwargs):
  return await endpoint(*args, **kwargs)
