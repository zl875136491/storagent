from datetime import timedelta

import pytest
from beanie.odm.fields import Link
from bson import DBRef, ObjectId

from src.core.auth import get_current_app
from src.modules.files import route as files_route
from src.modules.public.model import Application
from src.utils.helpers import utc_now


def test_object_stat_uses_post_body():
  route = next(item for item in files_route.router.routes if item.path == "/object/stat")
  assert route.methods == {"POST"}
  assert "payload" in route.endpoint.__annotations__


@pytest.mark.asyncio
async def test_api_key_application_link_is_resolved(monkeypatch):
  app_id = ObjectId()
  linked_app = Link(DBRef("application", app_id), Application)
  api_key = type("Key", (), {
    "deleted": False,
    "expired_at": utc_now() + timedelta(days=1),
    "application": linked_app,
  })()
  app = Application.model_construct(id=app_id, name="system-test", enabled=True)

  async def read_key(_key):
    return api_key

  async def read_app(requested_id):
    assert requested_id == app_id
    return app

  monkeypatch.setattr("src.modules.public.crud.read_api_key_by_key", read_key)
  monkeypatch.setattr("src.modules.public.crud.read_application_by_id", read_app)
  assert await get_current_app("secret") == "system-test"
