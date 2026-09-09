"""Periodic Etcd reconcile should not stampede user locks or duplicate publish."""
import pytest

from src.core import etcd_op


class _Lock:
  def __init__(self, acquired=True):
    self.acquired = acquired
    self.releases = 0

  async def acquire(self, timeout=0):
    self.timeout = timeout
    return self.acquired

  async def release(self):
    self.releases += 1
    return True


class _Client:
  def __init__(self, lock):
    self._lock = lock
    self.closed = 0

  def lock(self, key, ttl=45):
    self.key = key
    self.ttl = ttl
    return self._lock

  async def close(self):
    self.closed += 1


@pytest.mark.asyncio
async def test_reconcile_skips_publish_on_non_authority(monkeypatch):
  calls = []
  lock = _Lock()
  client = _Client(lock)

  async def get_client():
    return client

  async def publish_roles(**_kwargs):
    calls.append("roles")

  async def publish_users(**_kwargs):
    calls.append("users")

  async def bootstrap(**_kwargs):
    calls.append("bootstrap")

  async def backfill(**_kwargs):
    calls.append("backfill")

  async def pull(*, client, user_lock_timeout=30):
    calls.append(("pull", user_lock_timeout, client))

  monkeypatch.setattr(etcd_op.settings, "REGION", "nuc-docker-b")
  monkeypatch.setattr(etcd_op.settings, "SYNC_AUTHORITY_REGION", "nuc-docker-a")
  monkeypatch.setattr(etcd_op.settings, "SYNC_RECONCILE_LOCK_TTL_SECONDS", 45)
  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(etcd_op.sync_module, "publish_roles", publish_roles)
  monkeypatch.setattr(etcd_op.sync_module, "publish_local_users", publish_users)
  monkeypatch.setattr(etcd_op.sync_module, "bootstrap_topology_layout", bootstrap)
  monkeypatch.setattr(etcd_op.sync_module, "backfill_application_quotas", backfill)
  monkeypatch.setattr(etcd_op.sync_module, "pull_all_and_sync", pull)

  result = await etcd_op.reconcile_etcd_once()
  assert result == {"status": "succeeded"}
  assert calls == [("pull", 0, client)]
  assert lock.timeout == 0
  assert lock.releases == 1


@pytest.mark.asyncio
async def test_reconcile_authority_publishes_then_pulls(monkeypatch):
  calls = []
  lock = _Lock()
  client = _Client(lock)

  async def get_client():
    return client

  async def publish_roles(**_kwargs):
    calls.append("roles")

  async def publish_users(**_kwargs):
    calls.append("users")

  async def bootstrap(**_kwargs):
    calls.append("bootstrap")

  async def backfill(**_kwargs):
    calls.append("backfill")

  async def pull(*, client, user_lock_timeout=30):
    calls.append(("pull", user_lock_timeout))

  monkeypatch.setattr(etcd_op.settings, "REGION", "nuc-docker-a")
  monkeypatch.setattr(etcd_op.settings, "SYNC_AUTHORITY_REGION", "nuc-docker-a")
  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(etcd_op.sync_module, "publish_roles", publish_roles)
  monkeypatch.setattr(etcd_op.sync_module, "publish_local_users", publish_users)
  monkeypatch.setattr(etcd_op.sync_module, "bootstrap_topology_layout", bootstrap)
  monkeypatch.setattr(etcd_op.sync_module, "backfill_application_quotas", backfill)
  monkeypatch.setattr(etcd_op.sync_module, "pull_all_and_sync", pull)

  result = await etcd_op.reconcile_etcd_once()
  assert result == {"status": "succeeded"}
  assert calls == ["roles", "users", "bootstrap", "backfill", ("pull", 0)]


@pytest.mark.asyncio
async def test_reconcile_skips_when_region_lock_busy(monkeypatch):
  calls = []
  lock = _Lock(acquired=False)
  client = _Client(lock)

  async def get_client():
    return client

  async def pull(**_kwargs):
    calls.append("pull")

  monkeypatch.setattr(etcd_op.settings, "REGION", "nuc-docker-a")
  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(etcd_op.sync_module, "pull_all_and_sync", pull)

  result = await etcd_op.reconcile_etcd_once()
  assert result == {"status": "skipped", "reason": "already-running"}
  assert calls == []
  assert lock.releases == 0
