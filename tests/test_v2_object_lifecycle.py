from datetime import timedelta
from types import SimpleNamespace

import pytest

from src.modules.files.v2 import service
from src.utils.helpers import utc_now


@pytest.mark.asyncio
async def test_delete_sets_recovery_window_and_releases_logical_quota(monkeypatch):
  now = utc_now()
  original = SimpleNamespace(
    object_id="object-1",
    object_key="reports/daily.csv",
    size_bytes=4096,
    state="active",
    deletion_generation=3,
    deleted_at=None,
    restore_until=None,
  )
  transitioned = []
  quota_releases = []

  async def read_object(app_name, object_id):
    assert (app_name, object_id) == ("demo", "object-1")
    return original

  async def transition(app_name, object_id, *, from_states, changes):
    assert (app_name, object_id) == ("demo", "object-1")
    assert from_states == ("active",)
    transitioned.append(changes)
    return SimpleNamespace(
      **{
        **original.__dict__,
        **changes,
      }
    )

  class Lock:
    async def __aenter__(self):
      return "quota-client"

    async def __aexit__(self, *_args):
      return False

  async def release(app_name, size_bytes, client):
    quota_releases.append((app_name, size_bytes, client))

  monkeypatch.setattr(service.crud, "read_object_by_id", read_object)
  monkeypatch.setattr(service.crud, "transition_object_state", transition)
  monkeypatch.setattr(service.quota, "application_quota_lock", lambda _app: Lock())
  monkeypatch.setattr(service.quota, "mark_object_deleted_locked", release)
  monkeypatch.setattr(service.settings, "OBJECT_RECOVERY_PERIOD_DAYS", 7)

  response = await service.delete(
    SimpleNamespace(state=SimpleNamespace(request_id="request-1")),
    "demo",
    "object-1",
  )

  changes = transitioned[0]
  assert changes["state"] == "soft_deleted"
  assert changes["archive_after"] == changes["restore_until"]
  assert changes["restore_until"] - now >= timedelta(days=7) - timedelta(seconds=1)
  assert changes["deletion_generation"] == 4
  assert quota_releases == [("demo", 4096, "quota-client")]
  assert response.data.state == "soft_deleted"
  assert response.data.object_id == "object-1"
