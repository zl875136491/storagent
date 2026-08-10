"""v2 usage route declarations."""
from src.core.v2_route_factory import clone_router
from src.modules.usage.v2 import service
from src.modules.usage.route import router as v1_router

router = clone_router(v1_router, service.call)
