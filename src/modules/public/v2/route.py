"""v2 public route declarations for the inherited public contract."""
from src.core.v2_route_factory import clone_router
from src.modules.public.v2 import service
from src.modules.public.route import router as v1_router

router = clone_router(v1_router, service.call)
