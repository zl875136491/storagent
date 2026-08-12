"""Version 2 audit route declarations.

The audit contract is inherited unchanged. The clone keeps its route/service
boundary versioned while v2 response envelopes are applied centrally.
"""
from src.core.v2_route_factory import clone_router
from src.modules.audit.route import router as audit_v1_router
from src.modules.audit.v2 import service


router = clone_router(audit_v1_router, service.delegate)
