"""v2 graph route declarations."""
from src.core.v2_route_factory import clone_router
from src.modules.graph.v2 import service
from src.modules.graph.route import router as v1_router

router = clone_router(v1_router, service.call)
