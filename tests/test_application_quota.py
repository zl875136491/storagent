import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace
from bson import ObjectId

import pytest
from pydantic import ValidationError

from src.core import etcd_op, minio_op
from src.core import sync as sync_module
from src.core.exception import CustomException, ErrorDesc
from src.modules.files import quota as files_quota
from src.modules.files import schema as files_schema
from src.modules.files import service as files_service
from src.modules.public import schema as public_schema
from src.modules.public import service as public_service
from src.utils.helpers import utc_now


class _EtcdCompare:
  def __init__(self, kind, key):
    self.kind = kind
    self.key = key
    self.expected = None

  def __eq__(self, expected):
    self.expected = expected
    return self


class _EtcdOperation:
  def __init__(self, kind, key, value=None, lease=None):
    self.kind = kind
    self.key = key
    self.value = value
    self.lease = lease


class _EtcdTransactions:
  def create(self, key):
    return _EtcdCompare("create", key)

  def mod(self, key):
    return _EtcdCompare("mod", key)

  def put(self, key, value, lease=None):
    return _EtcdOperation("put", key, value, lease)

  def delete(self, key):
    return _EtcdOperation("delete", key)


class _EtcdLease:
  def __init__(self, ttl):
    self.ttl = ttl
    self.revoked = False

  async def revoke(self):
    self.revoked = True


class _EtcdLock:
  def __init__(self, lock):
    self._lock = lock
    self._acquired = False

  async def acquire(self, timeout):
    try:
      await asyncio.wait_for(self._lock.acquire(), timeout=timeout)
    except TimeoutError:
      return False
    self._acquired = True
    return True

  async def refresh(self):
    return None

  async def is_acquired(self):
    return self._acquired

  async def release(self):
    if self._acquired:
      self._lock.release()
      self._acquired = False


class _EtcdState:
  def __init__(self):
    self.items = {}
    self.revision = 0
    self.transaction_lock = asyncio.Lock()
    self.named_locks = {}
    self.lease_ttls = []


class _FakeEtcd:
  def __init__(self, state):
    self.state = state
    self.transactions = _EtcdTransactions()

  def lock(self, key, ttl):
    del ttl
    lock = self.state.named_locks.setdefault(key, asyncio.Lock())
    return _EtcdLock(lock)

  async def lease(self, ttl):
    self.state.lease_ttls.append(ttl)
    return _EtcdLease(ttl)

  async def get(self, key):
    item = self.state.items.get(key)
    if not item:
      return None
    return SimpleNamespace(
      value=item["value"],
      mod_revision=item["mod_revision"],
    )

  async def delete(self, key):
    self.state.items.pop(key, None)

  async def put(self, key, value, lease=None):
    async with self.state.transaction_lock:
      previous = self.state.items.get(key)
      self.state.revision += 1
      self.state.items[key] = {
        "value": value,
        "create_revision": (
          previous["create_revision"]
          if previous else self.state.revision
        ),
        "mod_revision": self.state.revision,
        "lease_ttl": lease.ttl if lease else None,
      }

  async def transaction(self, compare, success=None, failure=None):
    async with self.state.transaction_lock:
      valid = True
      for condition in compare:
        item = self.state.items.get(condition.key)
        if condition.kind == "create":
          actual = item["create_revision"] if item else 0
        else:
          actual = item["mod_revision"] if item else 0
        valid = valid and actual == condition.expected
      for operation in (success if valid else failure) or []:
        if operation.kind == "delete":
          self.state.revision += 1
          self.state.items.pop(operation.key, None)
          continue
        if operation.kind != "put":
          continue
        previous = self.state.items.get(operation.key)
        self.state.revision += 1
        self.state.items[operation.key] = {
          "value": operation.value,
          "create_revision": (
            previous["create_revision"]
            if previous else self.state.revision
          ),
          "mod_revision": self.state.revision,
          "lease_ttl": operation.lease.ttl if operation.lease else None,
        }
      return valid, []

  async def close(self):
    return None

  async def get_prefix(self, prefix):
    raw_prefix = prefix if isinstance(prefix, bytes) else str(prefix).encode()
    kvs = []
    for key, item in self.state.items.items():
      raw_key = key if isinstance(key, bytes) else str(key).encode()
      if not raw_key.startswith(raw_prefix):
        continue
      value = item["value"]
      if isinstance(value, str):
        value = value.encode()
      kvs.append(SimpleNamespace(key=raw_key, value=value))
    return SimpleNamespace(kvs=kvs)


@pytest.fixture
def quota_etcd(monkeypatch):
  state = _EtcdState()
  files_quota.reset_application_fallback_sessions()

  async def get_client():
    return _FakeEtcd(state)

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  return state


async def _read_quota_key(state, key):
  client = _FakeEtcd(state)
  return await etcd_op.pull_from_etcd_by_key(key, client=client)


async def _seed_compact_admission(
  state,
  *,
  app_name="test-app",
  quota_bytes=100,
  active_usage_bytes=0,
  compact_reserved_bytes=0,
  compact_reservation_count=0,
  logical_usage_initialized=True,
):
  client = _FakeEtcd(state)
  await etcd_op.push_to_etcd(
    f"quota/admission/apps/{app_name}",
    {
      "version": 1,
      "quota_bytes": quota_bytes,
      "observed_usage_bytes": active_usage_bytes,
      "active_usage_bytes": active_usage_bytes,
      "logical_usage_initialized": logical_usage_initialized,
      "observed_usage_updated_at": utc_now().isoformat(),
      "legacy_reserved_bytes": 0,
      "legacy_reservation_count": 0,
      "compact_reserved_bytes": compact_reserved_bytes,
      "compact_reservation_count": compact_reservation_count,
      "reserved_bytes": compact_reserved_bytes,
      "reservation_count": compact_reservation_count,
    },
    client=client,
  )


def _quota_key_item(state, key):
  return state.items[f"{etcd_op.ETCD_PREFIX}{key}".encode()]


async def _reserve_and_activate(
  *,
  app_name="test-app",
  api_key_id="key-owner",
  object_key="object-1",
  upload_id="upload-1",
  declared_size_bytes=10,
):
  async def quota_loader(_client):
    return 100

  async def usage_loader():
    return 0

  reservation = await files_quota.reserve_upload(
    app_name=app_name,
    api_key_id=api_key_id,
    object_key=object_key,
    source_server="beijing",
    declared_size_bytes=declared_size_bytes,
    quota_loader=quota_loader,
    usage_loader=usage_loader,
  )
  return await files_quota.activate_reservation(reservation, upload_id)


async def _prepare_completing_upload(quota_etcd):
  await _reserve_and_activate(declared_size_bytes=10)
  client = _FakeEtcd(quota_etcd)
  part = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=10,
  )
  await files_quota.commit_part(client, part, 1, "part-etag")
  prepared = await files_quota.prepare_completion(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    parts=[(1, "part-etag")],
  )
  assert prepared.recovering is False
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "completing"


async def _set_reservation_expiry(
  quota_etcd,
  object_key: str,
  expires_at: str,
  *,
  update_session: bool = True,
):
  client = _FakeEtcd(quota_etcd)

  def state_mutator(raw):
    state = files_quota._normalize_state(raw)
    state["reservations"][object_key]["expires_at"] = expires_at
    return state

  await etcd_op.merge_update_etcd_key(
    "quota/apps/test-app",
    state_mutator,
    client=client,
  )
  if update_session:
    await etcd_op.merge_update_etcd_key(
      f"quota/uploads/test-app/{object_key}",
      lambda raw: {**raw, "expires_at": expires_at},
      client=client,
    )


def _patch_cleanup_minio(monkeypatch, minio_client):
  from src.modules.storage import crud as storage_crud

  async def read_server(server_name):
    assert server_name == "beijing"
    return SimpleNamespace(host="127.0.0.1", minio_port=9000)

  monkeypatch.setattr(storage_crud, "read_minio_server_by_region_name", read_server)
  monkeypatch.setattr(
    storage_crud,
    "plain_minio_credentials",
    lambda _server: ("access", "secret"),
  )
  monkeypatch.setattr(minio_op, "get_minio_client", lambda *_args: minio_client)


@pytest.mark.asyncio
async def test_minio_quota_helpers_use_hard_quota_and_versioned_usage(monkeypatch):
  calls = []

  async def run(args, *, timeout=20.0, record=True):
    calls.append((args, timeout, record))
    if args[:2] == ["quota", "set"]:
      return True, [{"status": "success"}], "", 1.0
    if args[:2] == ["quota", "info"]:
      return True, [{"status": "success", "quota": "100GiB"}], "", 1.0
    return True, [{"status": "success", "size": 1234}], "", 1.0

  monkeypatch.setattr(minio_op, "run_mc_json", run)

  success, error = await minio_op.set_bucket_hard_quota("beijing", "app", 2048)
  assert (success, error) == (True, "")
  assert calls[-1][0] == ["quota", "set", "beijing/app", "--size", "2048B"]

  success, quota, error = await minio_op.get_bucket_hard_quota("beijing", "app")
  assert (success, quota, error) == (True, 100 * 1024 ** 3, "")

  success, usage, error = await minio_op.get_bucket_usage_bytes("beijing", "app")
  assert (success, usage, error) == (True, 1234, "")
  assert calls[-1][0] == ["du", "--recursive", "--versions", "beijing/app"]


@pytest.mark.asyncio
async def test_missing_minio_quota_is_a_valid_unconfigured_state(monkeypatch):
  async def run(_args, *, timeout=20.0, record=True):
    return (
      False,
      [{
        "status": "error",
        "error": {
          "cause": {"error": {"Code": "XMinioAdminNoSuchQuotaConfiguration"}},
        },
      }],
      "The quota configuration does not exist",
      1.0,
    )

  monkeypatch.setattr(minio_op, "run_mc_json", run)
  assert await minio_op.get_bucket_hard_quota("beijing", "app") == (
    True,
    None,
    "",
  )


class _Application:
  def __init__(self, *, enabled=True, quota_bytes=1000, usage=0):
    now = utc_now()
    self.id = "app-id"
    self.name = "test-app"
    self.shown_name = "Test App"
    self.description = "test"
    self.created_at = now
    self.updated_at = now
    self.enabled = enabled
    self.enabled_at = now if enabled else None
    self.provisioning_status = "ready" if enabled else "pending"
    self.provisioning_error = ""
    self.provisioning_updated_at = now
    self.quota_bytes = quota_bytes
    self.quota_usage_bytes = usage
    self.quota_usage_updated_at = None
    self.author = SimpleNamespace(id="owner-id", username="owner", name="Owner")
    self.saved = 0

  async def save(self):
    self.saved += 1


@pytest.mark.asyncio
async def test_application_response_resolves_author_link_before_serialization(monkeypatch):
  app = _Application(enabled=False)
  app.id = ObjectId()

  class AuthorLink:
    async def fetch(self):
      return SimpleNamespace(
        id=ObjectId(),
        username="owner",
        name="Owner",
      )

  app.author = AuthorLink()
  result = await public_service._application_response(app)
  response = public_schema.ApplicationResponse.model_validate(result)
  assert response.author.username == "owner"


@pytest.mark.asyncio
async def test_application_usage_uses_largest_region_and_short_cache(monkeypatch):
  app = _Application()
  calls = []

  async def observed(_app_name):
    return 0

  async def server_names():
    return ["shenzhen", "beijing", "hangzhou"]

  async def usage(server, _bucket, *, timeout):
    calls.append((server, timeout))
    return True, {"beijing": 40, "hangzhou": 80, "shenzhen": 60}[server], ""

  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(public_service.minio_op, "get_bucket_usage_bytes", usage)
  monkeypatch.setattr(files_quota, "get_observed_usage_bytes", observed)

  assert await public_service.refresh_application_quota_usage(app) == 80
  assert app.quota_usage_bytes == 80
  assert app.quota_usage_updated_at is not None
  assert app.saved == 1
  assert await public_service.refresh_application_quota_usage(app) == 80
  assert len(calls) == 3


@pytest.mark.asyncio
async def test_application_usage_strict_mode_rejects_partial_read(monkeypatch):
  app = _Application()

  async def observed(_app_name):
    return 0

  async def server_names():
    return ["beijing", "shenzhen"]

  async def usage(server, _bucket, *, timeout):
    if server == "shenzhen":
      return False, 0, "timeout"
    return True, 12, ""

  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(public_service.minio_op, "get_bucket_usage_bytes", usage)
  monkeypatch.setattr(files_quota, "get_observed_usage_bytes", observed)

  with pytest.raises(CustomException) as exc_info:
    await public_service.refresh_application_quota_usage(
      app,
      force=True,
      require_all=True,
    )
  assert exc_info.value.code == ErrorDesc.MINIO_ACCESS_FAILED.code


@pytest.mark.asyncio
async def test_application_usage_refresh_is_single_flight(monkeypatch):
  app = _Application()
  calls = []
  both_commands_started = asyncio.Event()
  release_commands = asyncio.Event()

  async def observed(_app_name):
    return 0

  async def server_names():
    return ["beijing", "shenzhen"]

  async def usage(server, _bucket, *, timeout):
    del timeout
    calls.append(server)
    if len(calls) == 2:
      both_commands_started.set()
    await release_commands.wait()
    return True, {"beijing": 10, "shenzhen": 20}[server], ""

  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(public_service.minio_op, "get_bucket_usage_bytes", usage)
  monkeypatch.setattr(files_quota, "get_observed_usage_bytes", observed)
  first = asyncio.create_task(
    public_service.refresh_application_quota_usage(app, force=True)
  )
  await asyncio.wait_for(both_commands_started.wait(), timeout=1)
  second = asyncio.create_task(
    public_service.refresh_application_quota_usage(app, force=True)
  )
  await asyncio.sleep(0)
  release_commands.set()

  assert await asyncio.gather(first, second) == [20, 20]
  assert sorted(calls) == ["beijing", "shenzhen"]
  assert app.saved == 1


@pytest.mark.asyncio
async def test_application_usage_commands_have_global_concurrency_cap(monkeypatch):
  app = _Application()
  active = 0
  maximum_active = 0
  calls = []

  async def observed(_app_name):
    return 0

  async def server_names():
    return [f"region-{index}" for index in range(6)]

  async def usage(server, _bucket, *, timeout):
    nonlocal active, maximum_active
    del timeout
    calls.append(server)
    active += 1
    maximum_active = max(maximum_active, active)
    await asyncio.sleep(0.01)
    active -= 1
    return True, 1, ""

  monkeypatch.setattr(
    public_service.settings,
    "APPLICATION_QUOTA_USAGE_MAX_CONCURRENCY",
    2,
  )
  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(public_service.minio_op, "get_bucket_usage_bytes", usage)
  monkeypatch.setattr(files_quota, "get_observed_usage_bytes", observed)

  assert await public_service.refresh_application_quota_usage(app, force=True) == 1
  assert len(calls) == 6
  assert maximum_active == 2


@pytest.mark.asyncio
async def test_application_usage_display_never_drops_below_etcd_observed(monkeypatch):
  app = _Application(usage=20)

  async def observed(_app_name):
    return 100

  async def server_names():
    return ["beijing", "shenzhen"]

  async def usage(_server, _bucket, *, timeout):
    del timeout
    return True, 50, ""

  monkeypatch.setattr(files_quota, "get_observed_usage_bytes", observed)
  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(public_service.minio_op, "get_bucket_usage_bytes", usage)

  assert await public_service.refresh_application_quota_usage(
    app,
    force=True,
    require_all=True,
  ) == 100
  assert app.quota_usage_bytes == 100


@pytest.mark.asyncio
async def test_application_quota_limit_uses_authoritative_etcd_value(monkeypatch):
  async def pull(key, *, client=None):
    assert key == sync_module.ETCD_KEY_APPLICATIONS
    assert client == "shared-client"
    return {"test-app": {"quota_bytes": 321}}

  async def unexpected_local_read(_name):
    pytest.fail("quota admission must not read a potentially stale local value")

  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", pull)
  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_name",
    unexpected_local_read,
  )
  assert await public_service.get_application_quota_limit(
    "test-app",
    client="shared-client",
  ) == 321


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "read-error"])
async def test_application_quota_limit_fails_closed(monkeypatch, failure):
  async def pull(_key, *, client=None):
    del client
    if failure == "read-error":
      raise RuntimeError("etcd unavailable")
    return {}

  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", pull)
  with pytest.raises(CustomException) as exc_info:
    await public_service.get_application_quota_limit("test-app")
  assert exc_info.value.status_code == 503
  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert "上传已安全拒绝" in exc_info.value.reason


@pytest.mark.asyncio
async def test_quota_lock_refresh_failure_cancels_owner_and_returns_503(monkeypatch):
  original_sleep = asyncio.sleep

  class Lock:
    released = False

    async def acquire(self, timeout):
      assert timeout >= 1
      return True

    async def refresh(self):
      raise RuntimeError("lease refresh failed")

    async def is_acquired(self):
      return True

    async def release(self):
      self.released = True

  class Client:
    closed = False

    def __init__(self):
      self.quota_lock = Lock()

    def lock(self, key, ttl):
      assert key.endswith(b"application/test-app")
      assert ttl >= 30
      return self.quota_lock

    async def close(self):
      self.closed = True

  client = Client()

  async def get_client():
    return client

  async def immediate_sleep(_delay):
    await original_sleep(0)

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(files_quota.asyncio, "sleep", immediate_sleep)

  with pytest.raises(CustomException) as exc_info:
    async with files_quota.application_quota_lock("test-app"):
      await original_sleep(10)

  assert exc_info.value.status_code == 503
  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert "锁续租失败" in exc_info.value.reason
  assert client.quota_lock.released is True
  assert client.closed is True
  assert asyncio.current_task() not in files_quota._LOCK_FAILURES


def _patch_quota_update_base(
  monkeypatch,
  app,
  *,
  usage=10,
  active_reserved=0,
):
  @asynccontextmanager
  async def lock(_name):
    yield

  @asynccontextmanager
  async def quota_guard(_name, observed_usage_bytes=None, *, usage_loader=None):
    del observed_usage_bytes, usage_loader
    yield usage, active_reserved

  async def read_app(_application_id):
    return app

  async def refresh(_app, *, force=False, require_all=False):
    return usage

  async def server_names():
    return ["beijing", "hangzhou", "shenzhen"]

  authoritative = {
    "shown_name": app.shown_name,
    "description": app.description,
    "enabled": app.enabled,
    "quota_bytes": app.quota_bytes,
    "author_username": app.author.username,
  }

  async def read_authoritative(_app_name):
    return dict(authoritative)

  async def write_authoritative(_app_name, quota_bytes):
    authoritative["quota_bytes"] = quota_bytes
    return dict(authoritative)

  async def project(_app_name, entry):
    app.enabled = bool(entry.get("enabled", False))
    app.quota_bytes = int(entry["quota_bytes"])
    return app, False

  monkeypatch.setattr(public_service.sync_module, "application_replication_lock", lock)
  monkeypatch.setattr(files_quota, "quota_update_guard", quota_guard)
  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_app)
  monkeypatch.setattr(public_service, "refresh_application_quota_usage", refresh)
  monkeypatch.setattr(public_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(
    public_service,
    "_read_authoritative_application",
    read_authoritative,
  )
  monkeypatch.setattr(
    public_service,
    "_write_authoritative_application_quota",
    write_authoritative,
  )
  monkeypatch.setattr(
    public_service.sync_module,
    "upsert_application_from_etcd",
    project,
  )
  return authoritative


@pytest.mark.asyncio
async def test_quota_update_rolls_back_already_changed_regions(monkeypatch):
  app = _Application(quota_bytes=100)
  _patch_quota_update_base(monkeypatch, app)
  calls = []

  async def get_quota(server, _bucket, *, timeout):
    return True, 100, ""

  async def set_quota(server, _bucket, value, *, timeout):
    calls.append((server, value))
    if server == "hangzhou" and value == 200:
      return False, "write failed"
    return True, ""

  monkeypatch.setattr(public_service.minio_op, "get_bucket_hard_quota", get_quota)
  monkeypatch.setattr(public_service.minio_op, "set_bucket_hard_quota", set_quota)

  with pytest.raises(CustomException) as exc_info:
    await public_service.update_application_quota(
      "app-id",
      200,
      SimpleNamespace(username="admin"),
    )
  assert exc_info.value.code == ErrorDesc.MINIO_ACCESS_FAILED.code
  assert calls == [
    ("beijing", 200),
    ("hangzhou", 200),
    ("beijing", 100),
    ("hangzhou", 100),
  ]
  assert app.quota_bytes == 100


@pytest.mark.asyncio
async def test_quota_update_lost_lock_after_first_minio_write_rolls_back(
  monkeypatch,
):
  app = _Application(quota_bytes=100)
  original_updated_at = app.updated_at
  _patch_quota_update_base(monkeypatch, app)
  set_calls = []
  published = []

  async def get_quota(_server, _bucket, *, timeout):
    del timeout
    return True, 100, ""

  async def set_quota(server, _bucket, value, *, timeout):
    del timeout
    set_calls.append((server, value))
    if (server, value) == ("beijing", 200):
      raise files_quota.QuotaLockLostCancellation()
    return True, ""

  async def publish(application):
    published.append(application.quota_bytes)

  monkeypatch.setattr(public_service.minio_op, "get_bucket_hard_quota", get_quota)
  monkeypatch.setattr(public_service.minio_op, "set_bucket_hard_quota", set_quota)
  monkeypatch.setattr(public_service.sync_module, "publish_application", publish)

  with pytest.raises(files_quota.QuotaLockLostCancellation):
    await public_service.update_application_quota(
      "app-id",
      200,
      SimpleNamespace(username="admin"),
    )

  assert set_calls == [("beijing", 200), ("beijing", 100)]
  assert app.quota_bytes == 100
  assert app.updated_at == original_updated_at
  assert app.saved == 0
  assert published == []


@pytest.mark.asyncio
async def test_quota_update_lost_lock_after_authoritative_commit_keeps_minio(
  monkeypatch,
):
  app = _Application(quota_bytes=100)
  _patch_quota_update_base(monkeypatch, app)
  set_calls = []
  ownership_checks = 0

  async def get_quota(_server, _bucket, *, timeout):
    del timeout
    return True, 100, ""

  async def set_quota(server, _bucket, value, *, timeout):
    del timeout
    set_calls.append((server, value))
    return True, ""

  def check_ownership():
    nonlocal ownership_checks
    ownership_checks += 1
    if ownership_checks == 2:
      raise files_quota.QuotaLockLostCancellation()

  monkeypatch.setattr(public_service.minio_op, "get_bucket_hard_quota", get_quota)
  monkeypatch.setattr(public_service.minio_op, "set_bucket_hard_quota", set_quota)
  monkeypatch.setattr(files_quota, "raise_if_quota_lock_lost", check_ownership)

  with pytest.raises(CustomException) as exc_info:
    await public_service.update_application_quota(
      "app-id",
      200,
      SimpleNamespace(username="admin"),
    )

  assert set_calls == [
    ("beijing", 200),
    ("hangzhou", 200),
    ("shenzhen", 200),
  ]
  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert "已写入跨节点权威配置" in exc_info.value.reason


@pytest.mark.asyncio
async def test_quota_update_rejects_value_below_current_usage(monkeypatch):
  app = _Application(quota_bytes=100)
  _patch_quota_update_base(monkeypatch, app, usage=81)

  with pytest.raises(CustomException) as exc_info:
    await public_service.update_application_quota(
      "app-id",
      80,
      SimpleNamespace(username="admin"),
    )
  assert exc_info.value.code == ErrorDesc.INVALID_PARAMS.code
  assert app.quota_bytes == 100


@pytest.mark.asyncio
async def test_quota_update_includes_active_upload_reservations(monkeypatch):
  app = _Application(quota_bytes=100)
  _patch_quota_update_base(
    monkeypatch,
    app,
    usage=60,
    active_reserved=30,
  )

  with pytest.raises(CustomException) as exc_info:
    await public_service.update_application_quota(
      "app-id",
      80,
      SimpleNamespace(username="admin"),
    )
  assert exc_info.value.code == ErrorDesc.INVALID_PARAMS.code
  assert "90 字节" in exc_info.value.reason
  assert app.quota_bytes == 100


@pytest.mark.asyncio
async def test_quota_update_persists_after_all_regions_succeed(monkeypatch):
  app = _Application(quota_bytes=100)
  authoritative = _patch_quota_update_base(monkeypatch, app, usage=25)

  async def get_quota(_server, _bucket, *, timeout):
    return True, 100, ""

  async def set_quota(_server, _bucket, _value, *, timeout):
    return True, ""

  monkeypatch.setattr(public_service.minio_op, "get_bucket_hard_quota", get_quota)
  monkeypatch.setattr(public_service.minio_op, "set_bucket_hard_quota", set_quota)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)

  result = await public_service.update_application_quota(
    "app-id",
    200,
    SimpleNamespace(username="admin"),
  )
  assert app.quota_bytes == 200
  assert authoritative["quota_bytes"] == 200
  assert result["quota_bytes"] == 200
  assert result["quota_usage_bytes"] == 25
  assert result["quota_usage_ratio"] == 0.125


@pytest.mark.asyncio
async def test_quota_update_uses_authoritative_enabled_state(monkeypatch):
  app = _Application(enabled=False, quota_bytes=100)
  authoritative = _patch_quota_update_base(monkeypatch, app, usage=25)
  authoritative.update({
    "enabled": True,
    "provisioning_status": "ready",
    "author_username": "owner",
  })
  set_calls = []

  async def get_quota(server, _bucket, *, timeout):
    del timeout
    return True, 100, ""

  async def set_quota(server, _bucket, value, *, timeout):
    del timeout
    set_calls.append((server, value))
    return True, ""

  monkeypatch.setattr(public_service.minio_op, "get_bucket_hard_quota", get_quota)
  monkeypatch.setattr(public_service.minio_op, "set_bucket_hard_quota", set_quota)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)

  await public_service.update_application_quota(
    "app-id",
    200,
    SimpleNamespace(username="admin"),
  )

  assert set_calls == [
    ("beijing", 200),
    ("hangzhou", 200),
    ("shenzhen", 200),
  ]
  assert authoritative["enabled"] is True
  assert authoritative["provisioning_status"] == "ready"
  assert authoritative["author_username"] == "owner"
  assert authoritative["quota_bytes"] == 200


@pytest.mark.asyncio
async def test_multipart_init_prechecks_declared_size(monkeypatch, quota_etcd):
  await _seed_compact_admission(
    quota_etcd,
    active_usage_bytes=90,
  )

  class Client:
    def _create_multipart_upload(self, *_args, **_kwargs):
      pytest.fail("MinIO upload must not start when quota admission fails")

  async def unexpected_client():
    pytest.fail("MinIO client must not be opened when quota admission fails")

  async def admission_policy():
    return 100

  monkeypatch.setattr(files_service, "_get_minio_client_with_server", unexpected_client)
  monkeypatch.setattr(files_service, "gen_object_key", lambda: "object-over-limit")
  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)

  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_init(
      {"app_name": "test-app", "api_key_id": "key-owner"},
      "application/octet-stream",
      size_bytes=11,
    )
  assert exc_info.value.status_code == 413
  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code
  assert exc_info.value.reason == "APP 存储超出限额，请联系管理员处理"


@pytest.mark.asyncio
async def test_multipart_init_uses_etcd_aggregate_without_minio_scan(monkeypatch, quota_etcd):
  await _seed_compact_admission(
    quota_etcd,
    active_usage_bytes=10,
  )

  class Client:
    def _create_multipart_upload(self, *_args, **_kwargs):
      return "upload-aggregate"

  async def local_client():
    return "beijing", Client()

  async def warning(*_args, **_kwargs):
    return None

  async def admission_policy():
    return 100

  monkeypatch.setattr(files_service, "_get_minio_client_with_server", local_client)
  monkeypatch.setattr(files_service, "gen_object_key", lambda: "object-aggregate")
  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)
  monkeypatch.setattr(files_service, "_upload_quota_warning", warning)

  result = await files_service.multipart_init(
    {"app_name": "test-app", "api_key_id": "key-owner"},
    "application/octet-stream",
    size_bytes=20,
  )

  assert result.upload_id == "upload-aggregate"
  state = await _read_quota_key(quota_etcd, "quota/admission/apps/test-app")
  assert state["reserved_bytes"] == 20
  assert state["reservation_count"] == 1
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-aggregate",
  )
  assert session["status"] == "active"
  assert session["quota_generation"] == 2


@pytest.mark.asyncio
async def test_multipart_init_counts_active_reservation_without_minio_scan(
  monkeypatch,
  quota_etcd,
):
  await _seed_compact_admission(
    quota_etcd,
    compact_reserved_bytes=100,
    compact_reservation_count=1,
  )

  class Client:
    def _create_multipart_upload(self, *_args, **_kwargs):
      pytest.fail("MinIO upload must not start when active reservations fill quota")

  async def unexpected_client():
    pytest.fail("MinIO client must not be opened when active reservations fill quota")

  async def admission_policy():
    return 100

  monkeypatch.setattr(files_service, "_get_minio_client_with_server", unexpected_client)
  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)

  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_init(
      {"app_name": "test-app", "api_key_id": "new-key"},
      "application/octet-stream",
      size_bytes=1,
    )

  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code


@pytest.mark.asyncio
async def test_multipart_init_rejects_uninitialized_aggregate_before_minio(monkeypatch):
  # The request must fail closed when the authority has not seeded the compact
  # admission key. No legacy migration or MinIO lookup is allowed here.
  async def get_client():
    return _FakeEtcd(_EtcdState())

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)

  async def unexpected_client():
    pytest.fail("MinIO client must not be opened before aggregate readiness")

  monkeypatch.setattr(files_service, "_get_minio_client_with_server", unexpected_client)

  async def admission_policy():
    return 100

  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)

  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_init(
      {"app_name": "test-app", "api_key_id": "key-owner"},
      "application/octet-stream",
      size_bytes=1,
    )

  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert "尚未初始化" in exc_info.value.reason


@pytest.mark.asyncio
async def test_multipart_init_uses_application_quota_when_aggregate_missing(
  monkeypatch,
  quota_etcd,
):
  files_quota.reset_application_fallback_sessions()

  class ApplicationQuota:
    quota_bytes = 1000
    quota_usage_bytes = 10

  async def read_app(_name):
    return ApplicationQuota()

  class Client:
    def _create_multipart_upload(self, *_args, **_kwargs):
      return "upload-mongo-fallback"

  async def local_client():
    return "beijing", Client()

  async def warning(*_args, **_kwargs):
    return None

  async def admission_policy():
    return 100

  monkeypatch.setattr(
    "src.modules.public.crud.read_application_by_name",
    read_app,
  )
  monkeypatch.setattr(files_service, "_get_minio_client_with_server", local_client)
  monkeypatch.setattr(files_service, "gen_object_key", lambda: "object-mongo-fallback")
  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)
  monkeypatch.setattr(files_service, "_upload_quota_warning", warning)

  result = await files_service.multipart_init(
    {"app_name": "test-app", "api_key_id": "key-owner"},
    "application/octet-stream",
    size_bytes=20,
  )

  assert result.upload_id == "upload-mongo-fallback"
  session = files_quota._FALLBACK_SESSIONS[
    "quota/uploads/test-app/object-mongo-fallback"
  ]
  assert session["quota_generation"] == 3
  assert session["status"] == "active"


@pytest.mark.asyncio
async def test_multipart_init_falls_back_to_application_quota_when_etcd_hangs(
  monkeypatch,
):
  files_quota.reset_application_fallback_sessions()
  monkeypatch.setattr(files_quota.settings, "QUOTA_ETCD_ADMISSION_TIMEOUT_SECONDS", 0.05)

  class HangingEtcd:
    async def get(self, _key):
      await asyncio.sleep(30)

    async def close(self):
      return None

    async def transaction(self, **_kwargs):
      await asyncio.sleep(30)
      return False, []

    async def lease(self, _ttl):
      await asyncio.sleep(30)

  async def get_client():
    return HangingEtcd()

  class ApplicationQuota:
    quota_bytes = 1000
    quota_usage_bytes = 10

  async def read_app(_name):
    return ApplicationQuota()

  class Client:
    def _create_multipart_upload(self, *_args, **_kwargs):
      return "upload-timeout-fallback"

  async def local_client():
    return "beijing", Client()

  async def warning(*_args, **_kwargs):
    return None

  async def admission_policy():
    return 100

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(
    "src.modules.public.crud.read_application_by_name",
    read_app,
  )
  monkeypatch.setattr(files_service, "_get_minio_client_with_server", local_client)
  monkeypatch.setattr(files_service, "gen_object_key", lambda: "object-timeout-fallback")
  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)
  monkeypatch.setattr(files_service, "_upload_quota_warning", warning)

  started = asyncio.get_running_loop().time()
  result = await files_service.multipart_init(
    {"app_name": "test-app", "api_key_id": "key-owner"},
    "application/octet-stream",
    size_bytes=20,
  )
  elapsed = asyncio.get_running_loop().time() - started

  assert result.upload_id == "upload-timeout-fallback"
  assert elapsed < 2
  reservation = files_quota._reservation_from_dict(
    "test-app",
    files_quota._FALLBACK_SESSIONS["quota/uploads/test-app/object-timeout-fallback"],
  )
  assert reservation.quota_generation == 3


@pytest.mark.asyncio
async def test_multipart_init_fallback_still_enforces_application_quota(monkeypatch):
  files_quota.reset_application_fallback_sessions()
  monkeypatch.setattr(files_quota.settings, "QUOTA_ETCD_ADMISSION_TIMEOUT_SECONDS", 0.05)

  class HangingEtcd:
    async def get(self, _key):
      await asyncio.sleep(30)

    async def close(self):
      return None

  async def get_client():
    return HangingEtcd()

  class ApplicationQuota:
    quota_bytes = 100
    quota_usage_bytes = 90

  async def read_app(_name):
    return ApplicationQuota()

  async def unexpected_client():
    pytest.fail("MinIO must not start when application quota is already full")

  async def admission_policy():
    return 100

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(
    "src.modules.public.crud.read_application_by_name",
    read_app,
  )
  monkeypatch.setattr(files_service, "_get_minio_client_with_server", unexpected_client)
  monkeypatch.setattr(files_service, "_admission_block_percent", admission_policy)

  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_init(
      {"app_name": "test-app", "api_key_id": "key-owner"},
      "application/octet-stream",
      size_bytes=20,
    )

  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code


@pytest.mark.asyncio
async def test_application_list_returns_cached_quota_without_live_refresh(monkeypatch):
  first = _Application(usage=25)
  first.name = "alpha"
  first.quota_usage_updated_at = None
  second = _Application(usage=40)
  second.name = "beta"
  second.quota_usage_updated_at = None

  async def read_list():
    return [first, second]

  async def boom(*_args, **_kwargs):
    raise AssertionError("application list must not refresh live quota usage")

  monkeypatch.setattr(public_service.public_crud, "read_application_list", read_list)
  monkeypatch.setattr(public_service, "refresh_application_quota_usage", boom)

  result = await public_service.get_application_list()
  assert [item["name"] for item in result["data"]] == ["alpha", "beta"]
  assert result["data"][0]["quota_usage_bytes"] == 25
  assert result["data"][1]["quota_usage_bytes"] == 40
  assert result["data"][0]["quota_usage_ratio"] == 0.025


@pytest.mark.asyncio
async def test_compact_admission_does_not_read_legacy_state(monkeypatch, quota_etcd):
  await _seed_compact_admission(
    quota_etcd,
    app_name="compact-app",
    active_usage_bytes=10,
  )
  original_read = files_quota._read_dict_with_rev

  async def guarded_read(key, client):
    assert not key.startswith("quota/apps/"), (
      "compact admission must not read the legacy APP document"
    )
    return await original_read(key, client)

  monkeypatch.setattr(files_quota, "_read_dict_with_rev", guarded_read)

  async def fail_quota_loader(_client):
    pytest.fail("compact admission must not load quota from applications")

  reservation = await files_quota.reserve_upload(
    app_name="compact-app",
    api_key_id="key-owner",
    object_key="object-compact",
    source_server="beijing",
    declared_size_bytes=20,
    quota_loader=fail_quota_loader,
    use_compact_admission=True,
  )

  assert reservation.quota_generation == 2
  assert reservation.admission_usage_bytes == 10
  state = await _read_quota_key(
    quota_etcd,
    "quota/admission/apps/compact-app",
  )
  assert state["reserved_bytes"] == 20
  assert state["reservation_count"] == 1


@pytest.mark.asyncio
async def test_compact_admission_missing_state_fails_without_legacy_or_quota_reads(
  monkeypatch,
  quota_etcd,
):
  del quota_etcd

  async def fail_legacy_read(*_args, **_kwargs):
    pytest.fail("compact admission must not migrate the legacy APP document")

  async def fail_quota_read(*_args, **_kwargs):
    pytest.fail("compact admission must not load quota from applications")

  monkeypatch.setattr(files_quota, "_read_dict", fail_legacy_read)

  with pytest.raises(CustomException) as exc_info:
    await files_quota.reserve_upload(
      app_name="not-migrated",
      api_key_id="key-owner",
      object_key="object-not-migrated",
      source_server="beijing",
      declared_size_bytes=1,
      quota_loader=fail_quota_read,
      use_compact_admission=True,
    )

  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert "聚合尚未初始化" in exc_info.value.reason


@pytest.mark.asyncio
async def test_compact_admission_missing_limit_fails_closed_without_quota_loader(
  quota_etcd,
):
  await _seed_compact_admission(
    quota_etcd,
    app_name="no-limit",
    quota_bytes=0,
  )

  async def fail_quota_loader(_client):
    pytest.fail("compact admission must not fetch quota in the request path")

  with pytest.raises(CustomException) as exc_info:
    await files_quota.reserve_upload(
      app_name="no-limit",
      api_key_id="key-owner",
      object_key="object-no-limit",
      source_server="beijing",
      declared_size_bytes=1,
      quota_loader=fail_quota_loader,
      use_compact_admission=True,
    )

  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert "聚合尚未初始化" in exc_info.value.reason


@pytest.mark.asyncio
async def test_compact_admission_cas_prevents_oversell(quota_etcd):
  await _seed_compact_admission(quota_etcd, app_name="concurrent-app")

  async def quota_loader(_client):
    return 100

  async def reserve(index):
    return await files_quota.reserve_upload(
      app_name="concurrent-app",
      api_key_id="key-owner",
      object_key=f"object-{index}",
      source_server="beijing",
      declared_size_bytes=60,
      quota_loader=quota_loader,
      use_compact_admission=True,
    )

  results = await asyncio.gather(
    reserve(1),
    reserve(2),
    return_exceptions=True,
  )
  assert sum(
    isinstance(result, files_quota.UploadReservation) for result in results
  ) == 1
  assert sum(
    isinstance(result, CustomException)
    and result.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code
    for result in results
  ) == 1
  state = await _read_quota_key(
    quota_etcd,
    "quota/admission/apps/concurrent-app",
  )
  assert state["reserved_bytes"] == 60
  assert state["reservation_count"] == 1


@pytest.mark.asyncio
async def test_compact_admission_applies_global_block_threshold(quota_etcd):
  await _seed_compact_admission(
    quota_etcd,
    app_name="block-threshold-app",
    active_usage_bytes=70,
  )

  with pytest.raises(CustomException) as exc_info:
    await files_quota.reserve_upload(
      app_name="block-threshold-app",
      api_key_id="key-owner",
      object_key="object-block-threshold",
      source_server="beijing",
      declared_size_bytes=11,
      block_percent=80,
      use_compact_admission=True,
    )

  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code


@pytest.mark.asyncio
async def test_compact_finalize_updates_usage_once(quota_etcd):
  await _seed_compact_admission(
    quota_etcd,
    app_name="finalize-app",
    active_usage_bytes=5,
  )

  async def quota_loader(_client):
    return 100

  reservation = await files_quota.reserve_upload(
    app_name="finalize-app",
    api_key_id="key-owner",
    object_key="object-finalize",
    source_server="beijing",
    declared_size_bytes=20,
    quota_loader=quota_loader,
    use_compact_admission=True,
  )
  reservation = await files_quota.activate_reservation(reservation, "upload-1")
  client = _FakeEtcd(quota_etcd)
  await files_quota.finalize_completed_session(client, reservation)
  await files_quota.finalize_completed_session(client, reservation)

  state = await _read_quota_key(
    quota_etcd,
    "quota/admission/apps/finalize-app",
  )
  assert state["active_usage_bytes"] == 25
  assert state["reserved_bytes"] == 0
  assert state["reservation_count"] == 0
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/finalize-app/object-finalize",
  )
  assert session["quota_finalized"] is True


@pytest.mark.asyncio
async def test_compact_admission_reclaims_expired_sessions_at_cap(
  monkeypatch,
  quota_etcd,
):
  monkeypatch.setattr(
    files_quota.settings,
    "APPLICATION_QUOTA_MAX_ACTIVE_RESERVATIONS",
    1,
  )
  await _seed_compact_admission(
    quota_etcd,
    app_name="cap-app",
    compact_reserved_bytes=10,
    compact_reservation_count=1,
  )
  client = _FakeEtcd(quota_etcd)
  expired = (utc_now() - timedelta(seconds=1)).isoformat()
  await etcd_op.push_to_etcd(
    "quota/uploads/cap-app/object-stale",
    {
      "version": 1,
      "quota_generation": 2,
      "app_name": "cap-app",
      "api_key_id": "stale-key",
      "object_key": "object-stale",
      "upload_id": "",
      "source_server": "beijing",
      "declared_size_bytes": 10,
      "content_type": "application/octet-stream",
      "status": "initializing",
      "parts": {},
      "quota_finalized": False,
      "created_at": expired,
      "expires_at": expired,
    },
    client=client,
  )

  reservation = await files_quota.reserve_upload(
    app_name="cap-app",
    api_key_id="fresh-key",
    object_key="object-fresh",
    source_server="beijing",
    declared_size_bytes=8,
    use_compact_admission=True,
  )

  assert reservation.object_key == "object-fresh"
  state = await _read_quota_key(quota_etcd, "quota/admission/apps/cap-app")
  assert state["reservation_count"] == 1
  assert state["reserved_bytes"] == 8


@pytest.mark.asyncio
async def test_quota_aggregate_refresh_is_authority_only_and_batched(monkeypatch):
  app = _Application(quota_bytes=100)
  app.name = "aggregate-app"
  calls = []

  async def count_enabled():
    return 1

  async def candidates(limit):
    assert limit == 50
    return [app]

  async def refresh(application, *, force, require_all):
    calls.append((application.name, force, require_all))
    return 42

  async def reconcile(app_name, observed, *, quota_bytes):
    assert (app_name, observed, quota_bytes) == ("aggregate-app", 42, 100)
    return {"usage_bytes": 42, "initialized": True}

  monkeypatch.setattr(public_service.settings, "REGION", "beijing")
  monkeypatch.setattr(public_service.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(public_service.public_crud, "count_enabled_applications", count_enabled)
  monkeypatch.setattr(public_service.public_crud, "read_quota_refresh_candidates", candidates)
  monkeypatch.setattr(public_service, "refresh_application_quota_usage", refresh)
  monkeypatch.setattr(files_quota, "reconcile_usage_aggregate", reconcile)

  result = await public_service.refresh_application_quota_aggregates_once()

  assert result == {
    "status": "completed",
    "processed": 1,
    "succeeded": 1,
    "failed": 0,
    "skipped": 0,
    "deferred": 0,
  }
  assert calls == [("aggregate-app", True, True)]
  assert app.saved == 1


@pytest.mark.asyncio
async def test_quota_aggregate_refresh_skips_non_authority(monkeypatch):
  async def unexpected(*_args, **_kwargs):
    pytest.fail("non-authority region must not scan application usage")

  monkeypatch.setattr(public_service.settings, "REGION", "shenzhen")
  monkeypatch.setattr(public_service.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(public_service.public_crud, "count_enabled_applications", unexpected)
  monkeypatch.setattr(public_service.public_crud, "read_quota_refresh_candidates", unexpected)

  result = await public_service.refresh_application_quota_aggregates_once()

  assert result["status"] == "skipped"
  assert result["processed"] == 0


@pytest.mark.parametrize("payload", [{}, {"size_bytes": 0}, {"size_bytes": -1}])
def test_multipart_init_requires_positive_size_bytes(payload):
  with pytest.raises(ValidationError) as exc_info:
    files_schema.MultipartInitRequest.model_validate(payload)
  assert any(error["loc"] == ("size_bytes",) for error in exc_info.value.errors())


@pytest.mark.asyncio
async def test_concurrent_cross_region_reservations_cannot_oversell(quota_etcd):
  async def quota_loader(_client):
    return 100

  async def usage_loader():
    await asyncio.sleep(0)
    return 0

  async def reserve(index):
    try:
      return await files_quota.reserve_upload(
        app_name="test-app",
        api_key_id=f"key-{index}",
        object_key=f"object-{index}",
        source_server=f"region-{index}",
        declared_size_bytes=30,
        quota_loader=quota_loader,
        usage_loader=usage_loader,
      )
    except CustomException as error:
      return error

  results = await asyncio.gather(*(reserve(index) for index in range(5)))
  accepted = [item for item in results if isinstance(item, files_quota.UploadReservation)]
  rejected = [item for item in results if isinstance(item, CustomException)]

  assert len(accepted) == 3
  assert len(rejected) == 2
  assert all(
    error.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code
    for error in rejected
  )
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert sum(
    item["declared_size_bytes"]
    for item in state["reservations"].values()
  ) == 90


@pytest.mark.asyncio
async def test_usage_refresh_cannot_lower_confirmed_logical_usage(quota_etcd):
  client = _FakeEtcd(quota_etcd)
  await etcd_op.merge_update_etcd_key(
    "quota/apps/test-app",
    lambda _raw: {
      "version": 1,
      "observed_usage_bytes": 100,
      "observed_usage_updated_at": (
        utc_now() - timedelta(days=1)
      ).isoformat(),
      "reservations": {},
    },
    client=client,
  )

  async def quota_loader(_client):
    return 100

  async def lagging_region_usage():
    return 50

  with pytest.raises(CustomException) as exc_info:
    await files_quota.reserve_upload(
      app_name="test-app",
      api_key_id="new-key",
      object_key="new-object",
      source_server="beijing",
      declared_size_bytes=1,
      quota_loader=quota_loader,
      usage_loader=lagging_region_usage,
    )

  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["observed_usage_bytes"] == 100
  assert state["reservations"] == {}


@pytest.mark.asyncio
async def test_observed_usage_helper_closes_only_its_own_client(
  monkeypatch,
  quota_etcd,
):
  seed = _FakeEtcd(quota_etcd)
  await etcd_op.merge_update_etcd_key(
    "quota/apps/test-app",
    lambda _raw: {
      "version": 1,
      "observed_usage_bytes": 123,
      "observed_usage_updated_at": utc_now().isoformat(),
      "reservations": {},
    },
    client=seed,
  )

  class TrackingClient(_FakeEtcd):
    def __init__(self, state):
      super().__init__(state)
      self.closed = False

    async def close(self):
      self.closed = True

  owned = TrackingClient(quota_etcd)

  async def get_client():
    return owned

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  assert await files_quota.get_observed_usage_bytes("test-app") == 123
  assert owned.closed is True

  shared = TrackingClient(quota_etcd)
  assert await files_quota.get_observed_usage_bytes(
    "test-app",
    client=shared,
  ) == 123
  assert shared.closed is False


@pytest.mark.asyncio
async def test_application_active_reservation_limit_rejects_admission(
  monkeypatch,
  quota_etcd,
):
  async def quota_loader(_client):
    return 1000

  async def usage_loader():
    return 0

  monkeypatch.setattr(
    files_quota.settings,
    "APPLICATION_QUOTA_MAX_ACTIVE_RESERVATIONS",
    2,
  )
  for index in range(2):
    await files_quota.reserve_upload(
      app_name="test-app",
      api_key_id=f"key-{index}",
      object_key=f"object-{index}",
      source_server=f"region-{index}",
      declared_size_bytes=10,
      quota_loader=quota_loader,
      usage_loader=usage_loader,
    )

  with pytest.raises(CustomException) as exc_info:
    await files_quota.reserve_upload(
      app_name="test-app",
      api_key_id="key-over-limit",
      object_key="object-over-limit",
      source_server="region-over-limit",
      declared_size_bytes=10,
      quota_loader=quota_loader,
      usage_loader=usage_loader,
    )
  assert exc_info.value.code == ErrorDesc.STATUS_ERR.code
  assert "活动上传任务过多" in exc_info.value.reason
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert set(state["reservations"]) == {"object-0", "object-1"}


@pytest.mark.asyncio
async def test_expired_cleanup_failure_retains_and_counts_reservation(
  monkeypatch,
  quota_etcd,
):
  await _reserve_and_activate(declared_size_bytes=60)
  expired = (utc_now() - timedelta(seconds=1)).isoformat()
  await _set_reservation_expiry(quota_etcd, "object-1", expired)

  class Client:
    abort_calls = 0

    def _abort_multipart_upload(self, *_args):
      self.abort_calls += 1
      raise RuntimeError("temporary MinIO failure")

  minio_client = Client()
  _patch_cleanup_minio(monkeypatch, minio_client)

  async def quota_loader(_client):
    return 100

  async def usage_loader():
    return 0

  with pytest.raises(CustomException) as exc_info:
    await files_quota.reserve_upload(
      app_name="test-app",
      api_key_id="new-key",
      object_key="new-object",
      source_server="shenzhen",
      declared_size_bytes=50,
      quota_loader=quota_loader,
      usage_loader=usage_loader,
    )

  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code
  assert minio_client.abort_calls == 1
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert set(state["reservations"]) == {"object-1"}
  retained = state["reservations"]["object-1"]
  assert retained["status"] == "cleanup_pending"
  assert retained["declared_size_bytes"] == 60
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["upload_id"] == "upload-1"


@pytest.mark.asyncio
async def test_expired_cleanup_no_such_upload_removes_session_and_reservation(
  monkeypatch,
  quota_etcd,
):
  await _reserve_and_activate(declared_size_bytes=60)
  expired = (utc_now() - timedelta(seconds=1)).isoformat()
  await _set_reservation_expiry(quota_etcd, "object-1", expired)

  class NoSuchUploadError(Exception):
    code = "NoSuchUpload"

  class Client:
    abort_calls = 0

    def _abort_multipart_upload(self, *_args):
      self.abort_calls += 1
      raise NoSuchUploadError("The specified upload does not exist")

  minio_client = Client()
  _patch_cleanup_minio(monkeypatch, minio_client)

  async def quota_loader(_client):
    return 100

  async def usage_loader():
    return 0

  created = await files_quota.reserve_upload(
    app_name="test-app",
    api_key_id="new-key",
    object_key="new-object",
    source_server="shenzhen",
    declared_size_bytes=50,
    quota_loader=quota_loader,
    usage_loader=usage_loader,
  )

  assert created.object_key == "new-object"
  assert minio_client.abort_calls == 1
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert set(state["reservations"]) == {"new-object"}
  assert await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  ) == {}


@pytest.mark.asyncio
async def test_expired_initializing_reservation_without_upload_id_is_safe_to_remove(
  monkeypatch,
  quota_etcd,
):
  async def quota_loader(_client):
    return 100

  async def usage_loader():
    return 0

  await files_quota.reserve_upload(
    app_name="test-app",
    api_key_id="old-key",
    object_key="initializing-object",
    source_server="beijing",
    declared_size_bytes=60,
    quota_loader=quota_loader,
    usage_loader=usage_loader,
  )
  expired = (utc_now() - timedelta(seconds=1)).isoformat()
  await _set_reservation_expiry(
    quota_etcd,
    "initializing-object",
    expired,
    update_session=False,
  )

  async def unexpected_server(_server_name):
    pytest.fail("initializing reservation has no MinIO upload ID to abort")

  from src.modules.storage import crud as storage_crud
  monkeypatch.setattr(storage_crud, "read_minio_server_by_region_name", unexpected_server)
  created = await files_quota.reserve_upload(
    app_name="test-app",
    api_key_id="new-key",
    object_key="new-object",
    source_server="shenzhen",
    declared_size_bytes=50,
    quota_loader=quota_loader,
    usage_loader=usage_loader,
  )

  assert created.object_key == "new-object"
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert set(state["reservations"]) == {"new-object"}


@pytest.mark.asyncio
async def test_cleanup_cas_does_not_abort_concurrently_renewed_reservation(
  monkeypatch,
  quota_etcd,
):
  await _reserve_and_activate(declared_size_bytes=60)
  expired = (utc_now() - timedelta(seconds=1)).isoformat()
  # Cleanup observes the APP reservation as expired while the independently
  # locked part path still has a valid session and can renew it.
  await _set_reservation_expiry(
    quota_etcd,
    "object-1",
    expired,
    update_session=False,
  )
  cleanup_observed = asyncio.Event()
  continue_cleanup = asyncio.Event()

  async def quota_loader(_client):
    return 100

  async def usage_loader():
    cleanup_observed.set()
    await continue_cleanup.wait()
    return 0

  async def unexpected_server(_server_name):
    pytest.fail("a renewed reservation must not be aborted by stale cleanup")

  from src.modules.storage import crud as storage_crud
  monkeypatch.setattr(storage_crud, "read_minio_server_by_region_name", unexpected_server)
  admission = asyncio.create_task(files_quota.reserve_upload(
    app_name="test-app",
    api_key_id="new-key",
    object_key="new-object",
    source_server="shenzhen",
    declared_size_bytes=50,
    quota_loader=quota_loader,
    usage_loader=usage_loader,
  ))
  await asyncio.wait_for(cleanup_observed.wait(), timeout=1)

  await files_quota.prepare_part(
    _FakeEtcd(quota_etcd),
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=1,
  )
  continue_cleanup.set()

  with pytest.raises(CustomException) as exc_info:
    await admission
  assert exc_info.value.code == ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED.code
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["reservations"]["object-1"]["status"] == "active"
  assert "new-object" not in state["reservations"]


@pytest.mark.asyncio
async def test_cleanup_pending_reservation_cannot_be_renewed(quota_etcd):
  reservation = await _reserve_and_activate()
  client = _FakeEtcd(quota_etcd)

  def mark_cleanup(raw):
    state = files_quota._normalize_state(raw)
    state["reservations"]["object-1"]["status"] = "cleanup_pending"
    return state

  await etcd_op.merge_update_etcd_key(
    "quota/apps/test-app",
    mark_cleanup,
    client=client,
  )
  before = await _read_quota_key(quota_etcd, "quota/apps/test-app")

  with pytest.raises(CustomException) as exc_info:
    await files_quota._touch_state_reservation(
      client,
      reservation,
      files_quota._new_expiry(),
    )

  assert exc_info.value.code == ErrorDesc.STATUS_ERR.code
  assert "过期清理" in exc_info.value.reason
  after = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert after["reservations"]["object-1"] == before["reservations"]["object-1"]


@pytest.mark.asyncio
async def test_same_part_retry_replaces_previous_size(quota_etcd):
  await _reserve_and_activate(declared_size_bytes=10)
  client = _FakeEtcd(quota_etcd)

  first = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=8,
  )
  await files_quota.commit_part(client, first, 1, "etag-first")
  retry = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=10,
  )

  assert retry.previous["size_bytes"] == 8
  assert retry.previous["etag"] == "etag-first"
  await files_quota.commit_part(client, retry, 1, "etag-retry")
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["parts"]["1"] == {
    "size_bytes": 10,
    "etag": "etag-retry",
    "status": "uploaded",
    "operation_id": retry.operation_id,
  }


@pytest.mark.asyncio
async def test_failed_part_retry_restores_previous_metadata(quota_etcd):
  await _reserve_and_activate(declared_size_bytes=10)
  client = _FakeEtcd(quota_etcd)
  first = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=8,
  )
  await files_quota.commit_part(client, first, 1, "etag-first")
  retry = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=10,
  )
  await files_quota.rollback_part(client, retry, 1)

  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["parts"]["1"] == retry.previous


@pytest.mark.asyncio
async def test_part_total_cannot_exceed_declared_size(quota_etcd):
  await _reserve_and_activate(declared_size_bytes=10)
  client = _FakeEtcd(quota_etcd)
  first = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=8,
  )
  await files_quota.commit_part(client, first, 1, "etag-1")

  with pytest.raises(CustomException) as exc_info:
    await files_quota.prepare_part(
      client,
      app_name="test-app",
      api_key_id="key-owner",
      object_key="object-1",
      upload_id="upload-1",
      part_number=2,
      size_bytes=3,
    )
  assert exc_info.value.code == ErrorDesc.INVALID_PARAMS.code
  assert "超过初始化声明" in exc_info.value.reason


@pytest.mark.asyncio
async def test_different_parts_merge_without_losing_metadata(quota_etcd):
  await _reserve_and_activate(declared_size_bytes=10)
  client = _FakeEtcd(quota_etcd)

  prepared = await asyncio.gather(*(
    files_quota.prepare_part(
      client,
      app_name="test-app",
      api_key_id="key-owner",
      object_key="object-1",
      upload_id="upload-1",
      part_number=part_number,
      size_bytes=5,
    )
    for part_number in (1, 2)
  ))
  await asyncio.gather(*(
    files_quota.commit_part(client, item, part_number, f"etag-{part_number}")
    for item, part_number in zip(prepared, (1, 2))
  ))

  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert set(session["parts"]) == {"1", "2"}
  assert sum(item["size_bytes"] for item in session["parts"].values()) == 10


@pytest.mark.asyncio
async def test_complete_and_abort_release_active_reservations(quota_etcd):
  await _reserve_and_activate(
    object_key="completed-object",
    upload_id="completed-upload",
    declared_size_bytes=10,
  )
  await _reserve_and_activate(
    object_key="aborted-object",
    upload_id="aborted-upload",
    declared_size_bytes=20,
  )
  client = _FakeEtcd(quota_etcd)
  part = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="completed-object",
    upload_id="completed-upload",
    part_number=1,
    size_bytes=10,
  )
  await files_quota.commit_part(client, part, 1, "etag-completed")
  completed = await files_quota.prepare_completion(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="completed-object",
    upload_id="completed-upload",
    parts=[(1, "etag-completed")],
  )
  await files_quota.record_completed_session(
    client,
    completed.reservation,
    {"etag": "object-etag", "version_id": "v1"},
  )
  await files_quota.finalize_completed_session(client, completed.reservation)

  aborted = await files_quota.prepare_abort(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="aborted-object",
    upload_id="aborted-upload",
  )
  assert aborted.already_aborted is False
  await files_quota.record_aborted_session(client, aborted.reservation)
  await files_quota.finalize_aborted_session(client, aborted.reservation)

  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["observed_usage_bytes"] == 10
  assert state["reservations"] == {}
  assert "completed_tombstones" not in state
  assert "aborted_tombstones" not in state
  completed_session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/completed-object",
  )
  aborted_session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/aborted-object",
  )
  assert completed_session["status"] == "completed"
  assert completed_session["result"] == {
    "etag": "object-etag",
    "version_id": "v1",
  }
  assert aborted_session["status"] == "aborted"
  assert _quota_key_item(
    quota_etcd,
    "quota/uploads/test-app/completed-object",
  )["lease_ttl"] > 0
  assert _quota_key_item(
    quota_etcd,
    "quota/uploads/test-app/aborted-object",
  )["lease_ttl"] > 0


@pytest.mark.asyncio
async def test_complete_and_abort_renew_session_and_state_with_same_expiry(
  quota_etcd,
):
  await _reserve_and_activate(
    object_key="completed-object",
    upload_id="completed-upload",
    declared_size_bytes=10,
  )
  await _reserve_and_activate(
    object_key="aborted-object",
    upload_id="aborted-upload",
    declared_size_bytes=20,
  )
  client = _FakeEtcd(quota_etcd)
  part = await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="completed-object",
    upload_id="completed-upload",
    part_number=1,
    size_bytes=10,
  )
  await files_quota.commit_part(client, part, 1, "etag-completed")
  near_expiry = utc_now() + timedelta(seconds=1)
  for object_key in ("completed-object", "aborted-object"):
    await _set_reservation_expiry(
      quota_etcd,
      object_key,
      near_expiry.isoformat(),
    )

  await files_quota.prepare_completion(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="completed-object",
    upload_id="completed-upload",
    parts=[(1, "etag-completed")],
  )
  await files_quota.prepare_abort(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="aborted-object",
    upload_id="aborted-upload",
  )

  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  for object_key, expected_status in (
    ("completed-object", "completing"),
    ("aborted-object", "aborting"),
  ):
    session = await _read_quota_key(
      quota_etcd,
      f"quota/uploads/test-app/{object_key}",
    )
    state_expiry = state["reservations"][object_key]["expires_at"]
    assert session["status"] == expected_status
    assert session["expires_at"] == state_expiry
    assert files_quota._parse_datetime(state_expiry) > near_expiry


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["complete", "abort"])
async def test_terminal_prepare_stops_before_minio_when_state_renewal_fails(
  monkeypatch,
  quota_etcd,
  operation,
):
  await _reserve_and_activate(declared_size_bytes=10)
  client = _FakeEtcd(quota_etcd)
  request_parts = []
  if operation == "complete":
    part = await files_quota.prepare_part(
      client,
      app_name="test-app",
      api_key_id="key-owner",
      object_key="object-1",
      upload_id="upload-1",
      part_number=1,
      size_bytes=10,
    )
    await files_quota.commit_part(client, part, 1, "part-etag")
    request_parts = [
      files_schema.MultipartPartItem(part_number=1, etag="part-etag"),
    ]
  state_before = await _read_quota_key(quota_etcd, "quota/apps/test-app")

  async def fail_renewal(*_args, **_kwargs):
    raise CustomException(ErrorDesc.SYNC_FAILED, "state renewal failed")

  async def unexpected_minio(_server_name):
    pytest.fail("MinIO must not be accessed after authoritative renewal fails")

  monkeypatch.setattr(files_quota, "_touch_state_reservation", fail_renewal)
  monkeypatch.setattr(files_service, "_get_minio_client_for_server", unexpected_minio)
  context = {"app_name": "test-app", "api_key_id": "key-owner"}

  with pytest.raises(CustomException) as exc_info:
    if operation == "complete":
      await files_service.multipart_complete(
        context,
        "upload-1",
        "object-1",
        request_parts,
      )
    else:
      await files_service.multipart_abort(
        files_schema.MultipartAbortRequest(
          object_key="object-1",
          upload_id="upload-1",
        ),
        context,
      )

  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "active"
  state_after = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state_after["reservations"]["object-1"] == (
    state_before["reservations"]["object-1"]
  )


@pytest.mark.asyncio
async def test_repeated_complete_returns_saved_result_without_double_counting(
  monkeypatch,
  quota_etcd,
):
  await _reserve_and_activate(declared_size_bytes=10)
  quota_client = _FakeEtcd(quota_etcd)
  part = await files_quota.prepare_part(
    quota_client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=10,
  )
  await files_quota.commit_part(quota_client, part, 1, "part-etag")

  class Client:
    complete_calls = 0

    def _complete_multipart_upload(self, *_args, **_kwargs):
      self.complete_calls += 1
      return SimpleNamespace(etag="object-etag", version_id="version-1")

  client = Client()
  client_lookups = []

  async def minio_for_server(server_name):
    client_lookups.append(server_name)
    return client

  monkeypatch.setattr(files_service, "_get_minio_client_for_server", minio_for_server)
  request_parts = [
    files_schema.MultipartPartItem(part_number=1, etag="part-etag"),
  ]
  context = {"app_name": "test-app", "api_key_id": "key-owner"}

  first = await files_service.multipart_complete(
    context,
    "upload-1",
    "object-1",
    request_parts,
  )
  second = await files_service.multipart_complete(
    context,
    "upload-1",
    "object-1",
    request_parts,
  )

  assert first == second
  assert first.etag == "object-etag"
  assert first.version_id == "version-1"
  assert client.complete_calls == 1
  assert client_lookups == ["beijing"]
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["observed_usage_bytes"] == 10
  assert state["reservations"] == {}
  assert "completed_tombstones" not in state
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "completed"
  assert session["result"] == {
    "etag": "object-etag",
    "version_id": "version-1",
  }
  assert _quota_key_item(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )["lease_ttl"] > 0


@pytest.mark.asyncio
async def test_completing_session_recovers_from_existing_object_stat(
  monkeypatch,
  quota_etcd,
):
  await _prepare_completing_upload(quota_etcd)

  class Client:
    stat_calls = 0
    complete_calls = 0

    def stat_object(self, bucket, object_key):
      assert (bucket, object_key) == ("test-app", "object-1")
      self.stat_calls += 1
      return SimpleNamespace(
        size=10,
        etag="stat-etag",
        version_id="stat-version",
      )

    def _complete_multipart_upload(self, *_args, **_kwargs):
      self.complete_calls += 1
      raise AssertionError("recovery must not complete an existing object again")

  client = Client()

  async def minio_for_server(server_name):
    assert server_name == "beijing"
    return client

  monkeypatch.setattr(files_service, "_get_minio_client_for_server", minio_for_server)
  result = await files_service.multipart_complete(
    {"app_name": "test-app", "api_key_id": "key-owner"},
    "upload-1",
    "object-1",
    [files_schema.MultipartPartItem(part_number=1, etag="part-etag")],
  )

  assert result.etag == "stat-etag"
  assert result.version_id == "stat-version"
  assert client.stat_calls == 1
  assert client.complete_calls == 0
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["observed_usage_bytes"] == 10
  assert state["reservations"] == {}
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "completed"
  assert session["result"] == {
    "etag": "stat-etag",
    "version_id": "stat-version",
  }


@pytest.mark.asyncio
async def test_completing_session_retries_complete_when_object_is_absent(
  monkeypatch,
  quota_etcd,
):
  await _prepare_completing_upload(quota_etcd)

  class NoSuchKeyError(Exception):
    code = "NoSuchKey"

  class Client:
    stat_calls = 0
    complete_calls = 0

    def stat_object(self, *_args, **_kwargs):
      self.stat_calls += 1
      raise NoSuchKeyError("object does not exist")

    def _complete_multipart_upload(self, *_args, **_kwargs):
      self.complete_calls += 1
      return SimpleNamespace(etag="retried-etag", version_id="retried-version")

  client = Client()

  async def minio_for_server(_server_name):
    return client

  monkeypatch.setattr(files_service, "_get_minio_client_for_server", minio_for_server)
  result = await files_service.multipart_complete(
    {"app_name": "test-app", "api_key_id": "key-owner"},
    "upload-1",
    "object-1",
    [files_schema.MultipartPartItem(part_number=1, etag="part-etag")],
  )

  assert result.etag == "retried-etag"
  assert result.version_id == "retried-version"
  assert client.stat_calls == 1
  assert client.complete_calls == 1
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["observed_usage_bytes"] == 10
  assert state["reservations"] == {}
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "completed"


@pytest.mark.asyncio
async def test_completing_session_size_mismatch_stays_reserved(
  monkeypatch,
  quota_etcd,
):
  await _prepare_completing_upload(quota_etcd)

  class Client:
    complete_calls = 0

    def stat_object(self, *_args, **_kwargs):
      return SimpleNamespace(size=9, etag="wrong-etag", version_id="wrong-version")

    def _complete_multipart_upload(self, *_args, **_kwargs):
      self.complete_calls += 1
      raise AssertionError("size mismatch must fail closed")

  client = Client()

  async def minio_for_server(_server_name):
    return client

  monkeypatch.setattr(files_service, "_get_minio_client_for_server", minio_for_server)
  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_complete(
      {"app_name": "test-app", "api_key_id": "key-owner"},
      "upload-1",
      "object-1",
      [files_schema.MultipartPartItem(part_number=1, etag="part-etag")],
    )

  assert exc_info.value.code == ErrorDesc.STATUS_ERR.code
  assert "对象大小与上传声明不一致" in exc_info.value.reason
  assert client.complete_calls == 0
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["observed_usage_bytes"] == 0
  assert set(state["reservations"]) == {"object-1"}
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "completing"


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupted_before_retry", [False, True])
async def test_abort_retries_aborting_session_and_converges_on_no_such_upload(
  monkeypatch,
  quota_etcd,
  interrupted_before_retry,
):
  await _reserve_and_activate()
  if interrupted_before_retry:
    quota_client = _FakeEtcd(quota_etcd)
    interrupted = await files_quota.prepare_abort(
      quota_client,
      app_name="test-app",
      api_key_id="key-owner",
      object_key="object-1",
      upload_id="upload-1",
    )
    assert interrupted.already_aborted is False

  class NoSuchUploadError(Exception):
    code = "NoSuchUpload"
    message = "The specified upload does not exist"

  class Client:
    abort_calls = 0

    def _abort_multipart_upload(self, *_args, **_kwargs):
      self.abort_calls += 1
      raise NoSuchUploadError("NoSuchUpload")

  client = Client()
  client_lookups = []

  async def minio_for_server(server_name):
    client_lookups.append(server_name)
    return client

  monkeypatch.setattr(files_service, "_get_minio_client_for_server", minio_for_server)
  body = files_schema.MultipartAbortRequest(
    object_key="object-1",
    upload_id="upload-1",
  )
  context = {"app_name": "test-app", "api_key_id": "key-owner"}

  first = await files_service.multipart_abort(body, context)
  second = await files_service.multipart_abort(body, context)

  assert first == second == {
    "bucket": "test-app",
    "object_key": "object-1",
    "upload_id": "upload-1",
    "aborted": True,
  }
  assert client.abort_calls == 1
  assert client_lookups == ["beijing"]
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert state["reservations"] == {}
  assert "aborted_tombstones" not in state
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "aborted"
  assert _quota_key_item(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )["lease_ttl"] > 0


@pytest.mark.asyncio
async def test_abort_rejects_session_with_uploading_part(quota_etcd):
  await _reserve_and_activate()
  client = _FakeEtcd(quota_etcd)
  await files_quota.prepare_part(
    client,
    app_name="test-app",
    api_key_id="key-owner",
    object_key="object-1",
    upload_id="upload-1",
    part_number=1,
    size_bytes=10,
  )

  with pytest.raises(CustomException) as exc_info:
    await files_quota.prepare_abort(
      client,
      app_name="test-app",
      api_key_id="key-owner",
      object_key="object-1",
      upload_id="upload-1",
    )
  assert exc_info.value.code == ErrorDesc.STATUS_ERR.code
  assert "仍有分片正在上传" in exc_info.value.reason
  session = await _read_quota_key(
    quota_etcd,
    "quota/uploads/test-app/object-1",
  )
  assert session["status"] == "active"
  assert session["parts"]["1"]["status"] == "uploading"
  state = await _read_quota_key(quota_etcd, "quota/apps/test-app")
  assert set(state["reservations"]) == {"object-1"}


@pytest.mark.asyncio
async def test_multipart_mutations_reject_another_api_key(quota_etcd):
  await _reserve_and_activate()
  client = _FakeEtcd(quota_etcd)
  operations = (
    files_quota.prepare_part(
      client,
      app_name="test-app",
      api_key_id="key-attacker",
      object_key="object-1",
      upload_id="upload-1",
      part_number=1,
      size_bytes=10,
    ),
    files_quota.prepare_completion(
      client,
      app_name="test-app",
      api_key_id="key-attacker",
      object_key="object-1",
      upload_id="upload-1",
      parts=[],
    ),
    files_quota.prepare_abort(
      client,
      app_name="test-app",
      api_key_id="key-attacker",
      object_key="object-1",
      upload_id="upload-1",
    ),
  )
  for operation in operations:
    with pytest.raises(CustomException) as exc_info:
      await operation
    assert exc_info.value.code == ErrorDesc.INSUFFICIENT_PERMISSIONS.code


@pytest.mark.asyncio
async def test_multipart_list_parts_rejects_another_api_key(
  monkeypatch,
  quota_etcd,
):
  await _reserve_and_activate()

  async def unexpected_client(*_args, **_kwargs):
    pytest.fail("ownership must be checked before MinIO is accessed")

  monkeypatch.setattr(files_service, "_get_minio_client", unexpected_client)
  monkeypatch.setattr(files_service, "_get_minio_client_for_server", unexpected_client)

  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_list_parts(
      {"app_name": "test-app", "api_key_id": "key-attacker"},
      "object-1",
      "upload-1",
      None,
    )
  assert exc_info.value.code == ErrorDesc.INSUFFICIENT_PERMISSIONS.code


@pytest.mark.asyncio
async def test_ensure_local_bucket_backfills_application_quota(monkeypatch):
  calls = []

  async def local_server(_region):
    return SimpleNamespace(name="beijing")

  async def existed(_server, _bucket):
    return True

  async def versioning(_server, _bucket):
    return True, ""

  async def read_app(_name):
    return SimpleNamespace(quota_bytes=321)

  async def ensure(server, bucket, quota, *, timeout):
    calls.append((server, bucket, quota, timeout))
    return True, ""

  from src.modules.public import crud as public_crud
  from src.modules.storage import crud as storage_crud

  monkeypatch.setattr(storage_crud, "read_minio_server_by_region_name", local_server)
  monkeypatch.setattr(public_crud, "read_application_by_name", read_app)
  monkeypatch.setattr(minio_op, "check_server_bucket_existed", existed)
  monkeypatch.setattr(minio_op, "enable_bucket_versioning", versioning)
  monkeypatch.setattr(minio_op, "ensure_bucket_hard_quota", ensure)
  monkeypatch.setattr(sync_module.settings, "REGION", "beijing")

  await sync_module.ensure_local_buckets_for_app("test-app")
  assert calls == [(
    "beijing",
    "test-app",
    321,
    sync_module.settings.MINIO_OPERATION_TIMEOUT_SECONDS,
  )]


def test_minio_quota_error_maps_to_stable_413():
  error = RuntimeError("XMinioAdminBucketQuotaExceeded: Bucket quota exceeded")
  with pytest.raises(CustomException) as exc_info:
    files_service._raise_minio_write_error(error)
  assert exc_info.value.status_code == 413
  assert exc_info.value.code == 413049
  assert exc_info.value.reason == "APP 存储超出限额，请联系管理员处理"


def test_recent_quota_cache_uses_timezone_aware_timestamp():
  app = _Application(usage=7)
  app.quota_usage_updated_at = utc_now() - timedelta(seconds=1)
  assert public_service._quota_cache_is_fresh(app) is True


@pytest.mark.asyncio
async def test_minio_server_aliases_use_stable_region_names(monkeypatch):
  from src.modules.storage import crud as storage_crud

  async def servers():
    return [
      SimpleNamespace(
        name="北京主存储（展示名）",
        region=SimpleNamespace(name="beijing"),
      ),
      SimpleNamespace(name="legacy-alias", region=None),
    ]

  monkeypatch.setattr(storage_crud, "read_minio_server_list", servers)
  assert await storage_crud.read_minio_server_names() == [
    "beijing",
    "legacy-alias",
  ]
