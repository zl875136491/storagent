"""v2 console demo route declarations.

The console still sends only the JWT and opaque API-key object reference.
These routes resolve the real key server-side and delegate to v2 file Services.
"""

from fastapi import Depends, Query, Request

from src.core.v2_route_factory import clone_router
from src.modules.demo.v2 import service
from src.modules.demo.route import router as v1_router
from src.modules.demo import route as v1_demo_route
from src.modules.files.v2 import service as files_v2_service, schema as files_v2_schema


_context = v1_demo_route._context

router = clone_router(v1_router, service.call)


@router.get("/files/objects", response_model=files_v2_schema.ObjectListResponse)
async def demo_objects(
  request: Request,
  prefix: str = Query(""),
  state: str = Query("active"),
  limit: int = Query(100, ge=1, le=1000),
  cursor: str | None = Query(None),
  app_context: dict = Depends(_context),
):
  return await files_v2_service.list_objects(
    request, app_context["app_name"], prefix=prefix, state=state, limit=limit, cursor=cursor,
  )


@router.delete("/files/objects/{object_id}", response_model=files_v2_schema.ObjectMutationResponse)
async def demo_delete_object(request: Request, object_id: str, app_context: dict = Depends(_context)):
  return await files_v2_service.delete(request, app_context["app_name"], object_id)


@router.post("/files/objects/{object_id}/restore", response_model=files_v2_schema.ObjectMutationResponse)
async def demo_restore_object(request: Request, object_id: str, app_context: dict = Depends(_context)):
  return await files_v2_service.restore(request, app_context["app_name"], object_id)


@router.post("/files/objects/{object_id}/share", response_model=files_v2_schema.ShareCreateResponse)
async def demo_share_object(
  request: Request,
  object_id: str,
  payload: files_v2_schema.ShareCreateRequest,
  app_context: dict = Depends(_context),
):
  return await files_v2_service.create_share(
    request, app_context["app_name"], object_id, payload,
  )
