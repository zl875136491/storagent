"""Audit route declarations and query guardrails."""
import inspect
from datetime import timedelta

import pytest
from fastapi import FastAPI

from src.api import register_api
from src.modules.audit import route, service
from src.utils.helpers import utc_now


def test_audit_routes_require_admin_dependency():
  assert "current_user" in inspect.signature(route.list_audit_events).parameters
  assert "current_user" in inspect.signature(route.audit_event_options).parameters

def test_audit_routes_are_available_in_v2():
  app = FastAPI()
  register_api(app)
  paths = {route.path for route in app.routes}
  assert "/api/v2/audit/events" in paths
  assert "/api/v2/audit/options" in paths



@pytest.mark.asyncio
async def test_audit_query_rejects_invalid_time_range():
  now = utc_now()
  with pytest.raises(ValueError, match="开始时间"):
    await service.list_events(start_at=now, end_at=now)

  with pytest.raises(ValueError, match="最多查询"):
    await service.list_events(start_at=now - timedelta(days=366), end_at=now)
