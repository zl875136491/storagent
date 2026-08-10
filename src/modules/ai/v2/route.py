"""v2 AI route declarations."""
from src.core.v2_route_factory import clone_router
from src.modules.ai.v2 import service
from src.modules.ai.route import router as v1_router

router = clone_router(v1_router, service.call)
