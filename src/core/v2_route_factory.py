"""Build versioned route declarations for unchanged v1 contracts."""
from __future__ import annotations

from functools import wraps
from typing import Callable, Iterable

from fastapi import APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.routing import APIRoute
from pydantic import TypeAdapter
from starlette.responses import Response

from src.core.middleware import current_request_id


def clone_router(
  source_router: APIRouter,
  delegate: Callable,
  *,
  excluded_paths: Iterable[str] = (),
) -> APIRouter:
  """Clone a v1 router while making the v2 Service the call boundary."""
  excluded = set(excluded_paths)
  router = APIRouter()
  for source in source_router.routes:
    if not isinstance(source, APIRoute) or source.path in excluded:
      continue
    source_endpoint = source.endpoint
    response_adapter = TypeAdapter(source.response_model) if source.response_model else None

    @wraps(source_endpoint)
    async def endpoint(*args, __source=source_endpoint, __adapter=response_adapter, **kwargs):
      result = await delegate(__source, *args, **kwargs)
      # v2 gives every JSON success a stable envelope. Streaming and binary
      # endpoints remain native responses; RequestContextMiddleware supplies
      # their X-Request-Id header without buffering the body.
      if isinstance(result, Response):
        return result
      # Preserve the v1 response model's custom encoders (notably Mongo
      # ObjectId values) before enclosing the result in the v2 envelope. v1
      # services commonly return Beanie Documents, so validation must support
      # attribute-based input just like FastAPI's response validation does.
      data = (
        __adapter.dump_python(
          __adapter.validate_python(result, from_attributes=True),
          mode="json",
        )
        if __adapter is not None
        else jsonable_encoder(result)
      )
      # Most inherited v1 schemas already use an internal `data` field. v2's
      # envelope replaces that legacy top level instead of producing data.data.
      if isinstance(data, dict) and "data" in data:
        data = data["data"]
      return {"data": data, "request_id": current_request_id.get()}

    router.add_api_route(
      source.path, endpoint, methods=source.methods,
      response_model=None, status_code=source.status_code,
      tags=source.tags, dependencies=source.dependencies,
      summary=source.summary, description=source.description,
      response_description=source.response_description, responses=source.responses,
      deprecated=source.deprecated,
      response_model_include=source.response_model_include,
      response_model_exclude=source.response_model_exclude,
      response_model_by_alias=source.response_model_by_alias,
      response_model_exclude_unset=source.response_model_exclude_unset,
      response_model_exclude_defaults=source.response_model_exclude_defaults,
      response_model_exclude_none=source.response_model_exclude_none,
      include_in_schema=source.include_in_schema, response_class=source.response_class,
      name=f"v2_{source.name}", callbacks=source.callbacks,
    )
  return router
