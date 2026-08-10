"""v2 storage route declarations."""
from fastapi import APIRouter, Request

from src.core.v2_route_factory import clone_router
from src.modules.storage import route as v1_route
from src.modules.storage.v2 import service

router = clone_router(
  v1_route.router,
  service.call,
  excluded_paths={"/objects/one-time-download"},
)


@router.get(
  "/objects/one-time-download",
  name="v2_one_time_share_exchange",
  response_model=None,
)
async def bootstrap():
  return await service.share_bootstrap()


@router.post("/objects/one-time-download", response_model=None)
async def redeem(request: Request):
  return await service.redeem_share(request)
