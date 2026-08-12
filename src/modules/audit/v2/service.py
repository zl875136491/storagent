"""Version 2 delegation boundary for inherited audit operations."""
from collections.abc import Callable
from typing import Any


async def delegate(source_endpoint: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
  """Invoke the existing v1 audit service through the explicit v2 boundary."""
  return await source_endpoint(*args, **kwargs)
