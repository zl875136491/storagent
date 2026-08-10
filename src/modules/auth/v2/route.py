"""v2 auth route declarations."""
from src.core.v2_route_factory import clone_router
from src.modules.auth.v2 import service
from src.modules.auth.route import router as v1_router

router = clone_router(v1_router, service.call)
