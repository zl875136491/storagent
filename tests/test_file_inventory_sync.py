"""File inventory Celery sync skips overlapping runs and stale Beat passes."""
from datetime import timedelta
from types import SimpleNamespace

import pytest

from src.modules.storage import inventory_sync
from src.utils.helpers import utc_now


class _Server:
  def __init__(self, name="local"):
    self.id = name
    self.name = name


@pytest.mark.asyncio
async def test_sync_skips_when_lease_is_held(monkeypatch):
  async def denied(**_kwargs):
    return False

  monkeypatch.setattr(inventory_sync, "try_acquire_lease", denied)
  result = await inventory_sync.sync_file_inventory_once(trigger="manual", actor="ops", task_id="t1")
  assert result["status"] == "skipped"
  assert result["reason"] == "already-running"


@pytest.mark.asyncio
async def test_beat_skips_fresh_index(monkeypatch):
  now = utc_now()
  server = _Server("minio-a")
  meta = SimpleNamespace(fetched_at=now - timedelta(minutes=10))
  calls = {"sync": 0}

  async def acquire(**_kwargs):
    return True

  async def release(**_kwargs):
    return None

  async def servers():
    return [server]

  async def read_meta(_server_id):
    return meta

  async def sync(_server):
    calls["sync"] += 1
    raise AssertionError("fresh beat must not list MinIO")

  monkeypatch.setattr(inventory_sync, "try_acquire_lease", acquire)
  monkeypatch.setattr(inventory_sync, "release_lease", release)
  monkeypatch.setattr(inventory_sync.storage_crud, "read_minio_server_list", servers)
  monkeypatch.setattr(inventory_sync.inventory, "_read_meta", read_meta)
  monkeypatch.setattr(inventory_sync.inventory, "_sync_from_minio", sync)
  monkeypatch.setattr(inventory_sync.settings, "FILE_INVENTORY_SYNC_INTERVAL_SECONDS", 21600)

  result = await inventory_sync.sync_file_inventory_once(trigger="beat", task_id="beat-1")
  assert result["status"] == "succeeded"
  assert result["skipped"] == 1
  assert result["servers"][0]["reason"] == "fresh"
  assert calls["sync"] == 0


@pytest.mark.asyncio
async def test_manual_sync_ignores_freshness(monkeypatch):
  now = utc_now()
  server = _Server("minio-a")
  snapshot = SimpleNamespace(object_count=12, total_size=34)

  async def acquire(**_kwargs):
    return True

  async def release(**_kwargs):
    return None

  async def servers():
    return [server]

  async def read_meta(_server_id):
    return SimpleNamespace(fetched_at=now)

  async def sync(_server):
    return snapshot

  monkeypatch.setattr(inventory_sync, "try_acquire_lease", acquire)
  monkeypatch.setattr(inventory_sync, "release_lease", release)
  monkeypatch.setattr(inventory_sync.storage_crud, "read_minio_server_list", servers)
  monkeypatch.setattr(inventory_sync.inventory, "_read_meta", read_meta)
  monkeypatch.setattr(inventory_sync.inventory, "_sync_from_minio", sync)

  result = await inventory_sync.sync_file_inventory_once(trigger="manual", actor="ops", task_id="m1")
  assert result["status"] == "succeeded"
  assert result["synced"] == 1
  assert result["servers"][0]["object_count"] == 12


@pytest.mark.asyncio
async def test_bootstrap_skips_existing_index(monkeypatch):
  async def acquire(**_kwargs):
    return True

  async def release(**_kwargs):
    return None

  async def servers():
    return [_Server("minio-a")]

  async def read_meta(_server_id):
    return SimpleNamespace(fetched_at=utc_now() - timedelta(days=2))

  async def boom(_server):
    raise AssertionError("bootstrap must not relist when an index exists")

  monkeypatch.setattr(inventory_sync, "try_acquire_lease", acquire)
  monkeypatch.setattr(inventory_sync, "release_lease", release)
  monkeypatch.setattr(inventory_sync.storage_crud, "read_minio_server_list", servers)
  monkeypatch.setattr(inventory_sync.inventory, "_read_meta", read_meta)
  monkeypatch.setattr(inventory_sync.inventory, "_sync_from_minio", boom)

  result = await inventory_sync.sync_file_inventory_once(trigger="bootstrap", task_id="b1")
  assert result["servers"][0]["reason"] == "already-indexed"
