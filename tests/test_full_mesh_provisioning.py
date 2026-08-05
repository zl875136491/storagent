import asyncio
import inspect
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.core import sync as sync_module
from src.core import minio_op
from src.modules.public import service as public_service
from src.modules.storage import crud as storage_crud
from src.modules.storage import service as storage_service


def _rule(from_server: str, to_server: str, *, enabled: bool = True) -> dict:
  return {
    "from": from_server,
    "to": to_server,
    "rule_id": f"{from_server}-{to_server}",
    "status": {
      "status": "success",
      "rule_status": "Enabled" if enabled else "Disabled",
      "priority": 1,
      "delete_marker_replication": "Enabled",
      "existing_object_replication": "Enabled",
      "source_selection_criteria": "Enabled",
    },
  }


def _full_mesh(sites: list[str]) -> list[dict]:
  return [
    _rule(from_server, to_server)
    for from_server in sites
    for to_server in sites
    if from_server != to_server
  ]


@pytest.mark.asyncio
async def test_unconfigured_replication_is_an_empty_read(monkeypatch):
  async def not_configured(_cmd):
    return False, "Unable to list replication configuration: replication configuration not set."

  monkeypatch.setattr(minio_op, "_run_cmd", not_configured)

  status_ok, statuses, status_error = (
    await minio_op.get_bucket_replicate_status_result("beijing", "new-app")
  )
  entries_ok, entries, entries_error = (
    await minio_op.get_bucket_replicate_entries_result("beijing", "new-app")
  )

  assert (status_ok, statuses, status_error) == (True, {}, "")
  assert (entries_ok, entries, entries_error) == (True, [], "")


def test_full_mesh_policy_detects_missing_duplicate_disabled_and_read_errors():
  sites = ["beijing", "hangzhou", "shenzhen"]
  rules = _full_mesh(sites)
  complete = storage_service.build_full_mesh_policy(sites, rules)
  assert complete["complete"] is True
  assert complete["expected_rule_count"] == 6
  assert complete["healthy_rule_count"] == 6

  missing = storage_service.build_full_mesh_policy(sites, rules[:-1])
  assert missing["complete"] is False
  assert missing["missing_rules"] == [{"from": "shenzhen", "to": "hangzhou"}]

  duplicate = storage_service.build_full_mesh_policy(sites, rules + [rules[0]])
  assert duplicate["complete"] is False
  assert duplicate["duplicate_rules"][0]["count"] == 2

  disabled_rules = [dict(rule) for rule in rules]
  disabled_rules[0] = _rule("beijing", "hangzhou", enabled=False)
  disabled = storage_service.build_full_mesh_policy(sites, disabled_rules)
  assert disabled["complete"] is False
  assert disabled["unhealthy_rules"] == [{"from": "beijing", "to": "hangzhou"}]

  unreadable = storage_service.build_full_mesh_policy(
    sites,
    rules,
    read_errors={"beijing": "timeout"},
  )
  assert unreadable["complete"] is False
  assert unreadable["read_errors"] == {"beijing": "timeout"}


@pytest.mark.asyncio
async def test_setup_full_mesh_refuses_unmapped_rules(monkeypatch):
  sites = ["beijing", "shenzhen"]
  policy = storage_service.build_full_mesh_policy(
    sites,
    [],
    unmapped_rule_count=1,
  )

  async def infos(_bucket):
    return {"replicates": [], "policy": policy}

  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", infos)
  with pytest.raises(sync_module.ReplicationPolicyError) as exc_info:
    await sync_module.setup_bucket_replication("system-test", sites)
  assert exc_info.value.policy["unmapped_rule_count"] == 1


@pytest.mark.asyncio
async def test_setup_full_mesh_skips_existing_and_uses_deterministic_priorities(monkeypatch):
  sites = ["shenzhen", "beijing", "hangzhou"]
  rules = [_rule("beijing", "hangzhou"), _rule("hangzhou", "beijing")]
  calls = []

  async def infos(_bucket):
    return {
      "replicates": list(rules),
      "policy": storage_service.build_full_mesh_policy(sites, list(rules)),
    }

  async def create_rule(
    from_server,
    to_server,
    _bucket,
    *,
    priority,
    enabled,
    replicate_options,
  ):
    calls.append((from_server, to_server, priority))
    assert enabled is True
    assert replicate_options == [
      "delete",
      "delete-marker",
      "existing-objects",
      "metadata-sync",
    ]
    rules.append(_rule(from_server, to_server))
    return True, "ok"

  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", infos)
  from src.core import minio_op
  monkeypatch.setattr(minio_op, "create_bucket_replicate", create_rule)

  policy = await sync_module.setup_bucket_replication(
    "system-test",
    sites,
    readback_attempts=1,
    readback_delay=0,
  )

  assert policy["complete"] is True
  assert ("beijing", "hangzhou", 1) not in calls
  assert ("hangzhou", "beijing", 1) not in calls
  assert len(calls) == 4
  for from_server, to_server, priority in calls:
    assert priority == sync_module.replication_priority(sites, from_server, to_server)
  shenzhen_priorities = sorted(
    priority for source, _target, priority in calls if source == "shenzhen"
  )
  assert shenzhen_priorities == [1, 2]


@pytest.mark.asyncio
async def test_setup_full_mesh_rejects_incomplete_readback(monkeypatch):
  sites = ["beijing", "shenzhen"]
  rules = [_rule("beijing", "shenzhen")]

  async def infos(_bucket):
    return {
      "replicates": list(rules),
      "policy": storage_service.build_full_mesh_policy(sites, list(rules)),
    }

  async def create_rule(*_args, **_kwargs):
    return True, "ok"

  from src.core import minio_op
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", infos)
  monkeypatch.setattr(minio_op, "create_bucket_replicate", create_rule)

  with pytest.raises(sync_module.ReplicationPolicyError) as exc_info:
    await sync_module.setup_bucket_replication(
      "system-test",
      sites,
      readback_attempts=1,
      readback_delay=0,
    )
  assert exc_info.value.policy["missing_rules"] == [
    {"from": "shenzhen", "to": "beijing"}
  ]


@pytest.mark.asyncio
async def test_application_replication_lock_reports_contention(monkeypatch):
  class Lock:
    async def acquire(self, timeout):
      assert timeout == 0
      return False

    async def release(self):
      pytest.fail("unacquired lock must not be released")

  class Client:
    closed = False

    def lock(self, key, ttl):
      assert key.endswith(b"/system-test")
      assert ttl >= 30
      return Lock()

    async def close(self):
      self.closed = True

  client = Client()

  async def get_client():
    return client

  from src.core import etcd_op
  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)

  with pytest.raises(sync_module.ReplicationLockBusyError):
    async with sync_module.application_replication_lock("system-test", timeout=0):
      pytest.fail("contended lock must not enter")
  assert client.closed is True


def test_application_sync_serializes_provisioning_state():
  now = public_service.utc_now()
  app = SimpleNamespace(
    shown_name="测试应用",
    description="test",
    enabled=False,
    provisioning_status="failed",
    provisioning_error="rule missing",
    provisioning_updated_at=now,
    quota_bytes=100 * 1024 ** 3,
    author=SimpleNamespace(username="owner", name="Owner"),
    approver=None,
    enabled_at=None,
    updated_at=now,
  )
  entry = sync_module.application_to_etcd_entry(app)
  assert entry["provisioning_status"] == "failed"
  assert entry["provisioning_error"] == "rule missing"
  assert entry["provisioning_updated_at"] == now.isoformat()
  assert entry["quota_bytes"] == 100 * 1024 ** 3
  assert "setup_bucket_replication" not in inspect.getsource(
    sync_module.upsert_application_from_etcd
  )


@pytest.mark.asyncio
async def test_bulk_create_minio_bucket_resumes_partial_write(monkeypatch):
  existing = SimpleNamespace(region=SimpleNamespace(id="beijing"))
  servers = [
    SimpleNamespace(region=SimpleNamespace(id="beijing")),
    SimpleNamespace(region=SimpleNamespace(id="shenzhen")),
  ]
  saved = []

  class Query:
    async def to_list(self):
      return [existing]

  class FakeMinioBucket:
    name = ""

    def __init__(self, **values):
      self.__dict__.update(values)

    @classmethod
    def find(cls, *_args, **_kwargs):
      return Query()

    async def save(self):
      saved.append(self)

  async def read_servers():
    return servers

  monkeypatch.setattr(storage_crud, "MinioBucket", FakeMinioBucket)
  monkeypatch.setattr(storage_crud, "read_minio_server_list", read_servers)

  buckets = await storage_crud.bulk_create_minio_bucket(
    SimpleNamespace(name="system-test")
  )

  assert buckets == [existing, saved[0]]
  assert len(saved) == 1
  assert saved[0].region.id == "shenzhen"


class _FakeApplication:
  def __init__(self):
    self.id = "app-id"
    self.name = "test-app"
    self.enabled = False
    self.enabled_at = None
    self.approver = None
    self.provisioning_status = "pending"
    self.provisioning_error = ""
    self.provisioning_updated_at = None
    self.updated_at = public_service.utc_now()
    self.saved_states = []

  async def save(self):
    self.saved_states.append((self.enabled, self.provisioning_status))


async def _collect_sse(generator) -> list[dict]:
  events = []
  async for chunk in generator:
    events.append(json.loads(chunk.decode().removeprefix("data: ").strip()))
  return events


def _patch_authorization_dependencies(monkeypatch, app, provision):
  @asynccontextmanager
  async def lock(_name):
    yield

  async def read_app(_application_id):
    return app

  async def publish(_app):
    return None

  async def server_names():
    return ["beijing", "shenzhen"]

  async def bucket_exists(_server, _bucket):
    return True

  async def enable_versioning(_server, _bucket):
    return True, "ok"

  async def bulk_create(_app):
    return None

  async def ensure_quotas(_bucket, quota_bytes, servers):
    assert quota_bytes == 100 * 1024 ** 3
    assert servers == ["beijing", "shenzhen"]

  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_app)
  monkeypatch.setattr(sync_module, "application_replication_lock", lock)
  monkeypatch.setattr(sync_module, "publish_application", publish)
  monkeypatch.setattr(sync_module, "setup_bucket_replication", provision)
  monkeypatch.setattr(sync_module, "ensure_bucket_quotas", ensure_quotas)
  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(public_service.storage_crud, "bulk_create_minio_bucket", bulk_create)
  monkeypatch.setattr(public_service.minio_op, "check_server_bucket_existed", bucket_exists)
  monkeypatch.setattr(public_service.minio_op, "enable_bucket_versioning", enable_versioning)


@pytest.mark.asyncio
async def test_authorization_enables_only_after_full_mesh_verification(monkeypatch):
  app = _FakeApplication()

  async def provision(_bucket, _servers):
    assert app.enabled is False
    assert app.provisioning_status == "provisioning"
    return {"complete": True, "expected_rule_count": 2, "actual_rule_count": 2}

  _patch_authorization_dependencies(monkeypatch, app, provision)
  events = await _collect_sse(
    public_service.enable_application("app-id", SimpleNamespace(username="admin"))
  )

  assert app.enabled is True
  assert app.provisioning_status == "ready"
  assert app.saved_states[0] == (False, "provisioning")
  assert app.saved_states[-1] == (True, "ready")
  assert events[-1]["status"] == "success"


@pytest.mark.asyncio
async def test_authorization_failure_stays_disabled_and_retryable(monkeypatch):
  app = _FakeApplication()

  async def provision(_bucket, _servers):
    raise sync_module.ReplicationPolicyError("missing rule")

  _patch_authorization_dependencies(monkeypatch, app, provision)
  events = await _collect_sse(
    public_service.enable_application("app-id", SimpleNamespace(username="admin"))
  )

  assert app.enabled is False
  assert app.provisioning_status == "failed"
  assert app.provisioning_error == "missing rule"
  assert all(enabled is False for enabled, _status in app.saved_states)
  assert events[-1]["status"] == "failed"
  assert "重试" in events[-1]["message"]


@pytest.mark.asyncio
async def test_authority_reconcile_repairs_degraded_enabled_application(monkeypatch):
  app = _FakeApplication()
  app.enabled = True
  app.provisioning_status = "degraded"
  app.provisioning_error = "missing"
  app.quota_bytes = 1
  repaired = asyncio.Event()
  published_quotas = []

  @asynccontextmanager
  async def lock(_name, timeout=0):
    assert timeout == 0
    yield

  @asynccontextmanager
  async def quota_lock(_name):
    yield object()

  async def applications():
    return [app]

  async def server_names():
    return ["beijing", "shenzhen"]

  async def setup(_bucket, _servers):
    repaired.set()
    return {"complete": True}

  async def publish(_app):
    published_quotas.append(_app.quota_bytes)

  async def ensure_quotas(_bucket, _quota_bytes, _servers):
    assert _quota_bytes == 100 * 1024 ** 3
    return None

  async def quota_limit(_app_name, *, client=None):
    return 100 * 1024 ** 3

  monkeypatch.setattr(sync_module.settings, "REGION", "beijing")
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(sync_module, "application_replication_lock", lock)
  from src.modules.files import quota as files_quota
  from src.modules.public import service as public_service
  monkeypatch.setattr(files_quota, "application_quota_lock", quota_lock)
  monkeypatch.setattr(public_service, "get_application_quota_limit", quota_limit)
  monkeypatch.setattr(sync_module, "setup_bucket_replication", setup)
  monkeypatch.setattr(sync_module, "ensure_bucket_quotas", ensure_quotas)
  monkeypatch.setattr(sync_module, "publish_application", publish)
  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud
  monkeypatch.setattr(public_crud, "read_application_list", applications)
  monkeypatch.setattr(storage_crud, "read_minio_server_names", server_names)

  task = asyncio.create_task(sync_module.reconcile_replication_policies_task())
  await asyncio.wait_for(repaired.wait(), timeout=1)
  for _ in range(10):
    if app.provisioning_status == "ready":
      break
    await asyncio.sleep(0)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task

  assert app.provisioning_status == "ready"
  assert app.provisioning_error == ""
  assert app.quota_bytes == 100 * 1024 ** 3
  assert published_quotas == [100 * 1024 ** 3]


@pytest.mark.asyncio
async def test_authority_reconcile_preserves_newer_quota_when_policy_fails(monkeypatch):
  app = _FakeApplication()
  app.enabled = True
  app.provisioning_status = "ready"
  app.quota_bytes = 1
  published = asyncio.Event()
  published_quotas = []

  @asynccontextmanager
  async def lock(_name, timeout=0):
    assert timeout == 0
    yield

  @asynccontextmanager
  async def quota_lock(_name):
    yield object()

  async def applications():
    return [app]

  async def server_names():
    return ["beijing", "shenzhen"]

  async def setup(_bucket, _servers):
    raise sync_module.ReplicationPolicyError("missing rule")

  async def quota_limit(_app_name, *, client=None):
    return 200 * 1024 ** 3

  async def ensure_quotas(*_args, **_kwargs):
    pytest.fail("hard quota reconciliation must wait for a valid replication policy")

  async def publish(_app):
    published_quotas.append(_app.quota_bytes)
    published.set()

  monkeypatch.setattr(sync_module.settings, "REGION", "beijing")
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(sync_module, "application_replication_lock", lock)
  from src.modules.files import quota as files_quota
  from src.modules.public import service as public_service
  monkeypatch.setattr(files_quota, "application_quota_lock", quota_lock)
  monkeypatch.setattr(public_service, "get_application_quota_limit", quota_limit)
  monkeypatch.setattr(sync_module, "setup_bucket_replication", setup)
  monkeypatch.setattr(sync_module, "ensure_bucket_quotas", ensure_quotas)
  monkeypatch.setattr(sync_module, "publish_application", publish)
  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud
  monkeypatch.setattr(public_crud, "read_application_list", applications)
  monkeypatch.setattr(storage_crud, "read_minio_server_names", server_names)

  task = asyncio.create_task(sync_module.reconcile_replication_policies_task())
  await asyncio.wait_for(published.wait(), timeout=1)
  task.cancel()
  with pytest.raises(asyncio.CancelledError):
    await task

  assert app.provisioning_status == "degraded"
  assert app.provisioning_error == "missing rule"
  assert app.quota_bytes == 200 * 1024 ** 3
  assert published_quotas == [200 * 1024 ** 3]
