from datetime import timedelta

import pytest
from beanie.odm.fields import Link
from bson import DBRef, ObjectId

from src.core.auth import get_current_app
from src.modules.files import route as files_route, service as files_service
from src.modules.public.model import Application, Region
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


@pytest.mark.asyncio
async def test_object_stat_does_not_dereference_server_region_link(monkeypatch):
  unresolved_region = Link(DBRef("region", ObjectId()), Region)
  server = type("Server", (), {"region": unresolved_region})()
  stat = type("Stat", (), {
    "size": 12,
    "etag": "etag",
    "content_type": "application/octet-stream",
    "last_modified": utc_now(),
  })()

  async def stat_object_local(_app_name, _object_key):
    return stat, server

  monkeypatch.setattr(files_service.files_locate, "stat_object_local", stat_object_local)
  monkeypatch.setattr(files_service.settings, "REGION", "beijing")

  result = await files_service.stat_object("system-test", "object-key")

  assert result.region == "beijing"
  assert result.local is True


@pytest.mark.asyncio
async def test_stream_download_omits_zero_length_for_minio(monkeypatch):
  class FakeResponse:
    def __init__(self):
      self._chunks = [b"demo-content", b""]

    def read(self, _size):
      return self._chunks.pop(0)

    def close(self):
      pass

    def release_conn(self):
      pass

  class FakeClient:
    def __init__(self):
      self.calls = []

    def get_object(self, *args, **kwargs):
      self.calls.append((args, kwargs))
      return FakeResponse()

  client = FakeClient()
  stat = type("Stat", (), {"content_type": "image/jpeg"})()
  server = type("Server", (), {"host": "minio.local", "minio_port": 9000})()

  async def stat_object_local(_bucket, _object_key):
    return stat, server

  async def record_transfer(_context, _action, _bytes):
    pass

  monkeypatch.setattr(files_service.files_locate, "stat_object_local", stat_object_local)
  monkeypatch.setattr(
    files_service.storage_crud,
    "plain_minio_credentials",
    lambda _server: ("access", "secret"),
  )
  monkeypatch.setattr(files_service, "get_minio_client", lambda *_args: client)
  monkeypatch.setattr(files_service, "record_transfer", record_transfer)

  response = await files_service.download_chunk(
    {"app_name": "demo-app"}, "images/0eva.jpeg", offset=0, length=0,
  )
  payload = b"".join([chunk async for chunk in response.body_iterator])

  assert payload == b"demo-content"
  assert client.calls == [(("demo-app", "images/0eva.jpeg"), {"offset": 0})]
