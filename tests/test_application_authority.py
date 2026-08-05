import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError

from src.core import etcd_op
from src.core import sync as sync_module
from src.core.exception import CustomException, ErrorDesc
from src.modules.public import model as public_model
from src.modules.public import service as public_service


class _EtcdCompare:
  def __init__(self, kind, key):
    self.kind = kind
    self.key = key
    self.expected = None

  def __eq__(self, expected):
    self.expected = expected
    return self


class _EtcdOperation:
  def __init__(self, kind, key, value):
    self.kind = kind
    self.key = key
    self.value = value


class _EtcdTransactions:
  def create(self, key):
    return _EtcdCompare("create", key)

  def mod(self, key):
    return _EtcdCompare("mod", key)

  def put(self, key, value, lease=None):
    del lease
    return _EtcdOperation("put", key, value)


class _EtcdState:
  def __init__(self, *, synchronize_empty_reads=False):
    self.items = {}
    self.revision = 0
    self.transaction_lock = asyncio.Lock()
    self.synchronize_empty_reads = synchronize_empty_reads
    self.empty_read_count = 0
    self.empty_reads_released = asyncio.Event()


class _FakeEtcd:
  def __init__(self, state):
    self.state = state
    self.transactions = _EtcdTransactions()

  async def get(self, key):
    item = self.state.items.get(key)
    if item is None and self.state.synchronize_empty_reads:
      self.state.empty_read_count += 1
      if self.state.empty_read_count == 2:
        self.state.empty_reads_released.set()
      await asyncio.wait_for(self.state.empty_reads_released.wait(), timeout=1)
      # Both contenders must observe the missing initial revision.
      return None
    if item is None:
      return None
    return SimpleNamespace(
      value=item["value"],
      mod_revision=item["mod_revision"],
    )

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
        }
      return valid, []

  async def close(self):
    return None


def _application_entries(state):
  key = f"{etcd_op.ETCD_PREFIX}{sync_module.ETCD_KEY_APPLICATIONS}".encode()
  item = state.items.get(key)
  return json.loads(item["value"].decode()) if item else {}


def _patch_application_creation(monkeypatch, state):
  async def get_client():
    return _FakeEtcd(state)

  async def no_existing(_value):
    return None

  async def project(app_name, entry):
    author = SimpleNamespace(
      username=entry["author_username"],
      name=entry["author_name"],
    )
    return SimpleNamespace(
      id=f"id-{app_name}",
      name=app_name,
      shown_name=entry["shown_name"],
      author=author,
    ), True

  async def response(app):
    return {"name": app.name, "author_username": app.author.username}

  async def notify(_app, _author):
    return None

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_name",
    no_existing,
  )
  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_shown_name",
    no_existing,
  )
  monkeypatch.setattr(
    public_service.sync_module,
    "upsert_application_from_etcd",
    project,
  )
  monkeypatch.setattr(public_service, "_application_response", response)
  monkeypatch.setattr(public_service, "_notify_application_managers", notify)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)


def _assert_single_create_winner(results, entries):
  successes = [result for result in results if not isinstance(result, Exception)]
  failures = [result for result in results if isinstance(result, Exception)]
  assert len(successes) == 1
  assert len(failures) == 1
  assert isinstance(failures[0], CustomException)
  assert failures[0].code == ErrorDesc.NAME_EXISTED.code
  assert len(entries) == 1
  only_entry = next(iter(entries.values()))
  assert successes[0]["author_username"] == only_entry["author_username"]


@pytest.mark.asyncio
async def test_create_application_same_app_id_is_claimed_once(monkeypatch):
  state = _EtcdState(synchronize_empty_reads=True)
  _patch_application_creation(monkeypatch, state)
  alice = SimpleNamespace(username="alice", name="Alice")
  bob = SimpleNamespace(username="bob", name="Bob")

  results = await asyncio.gather(
    public_service.create_application(
      "shared-app",
      "Alice application",
      "created in Beijing",
      alice,
    ),
    public_service.create_application(
      "shared-app",
      "Bob application",
      "created in Shenzhen",
      bob,
    ),
    return_exceptions=True,
  )

  entries = _application_entries(state)
  _assert_single_create_winner(results, entries)
  assert set(entries) == {"shared-app"}


@pytest.mark.asyncio
async def test_create_application_same_shown_name_is_claimed_once(monkeypatch):
  state = _EtcdState(synchronize_empty_reads=True)
  _patch_application_creation(monkeypatch, state)
  alice = SimpleNamespace(username="alice", name="Alice")
  bob = SimpleNamespace(username="bob", name="Bob")

  results = await asyncio.gather(
    public_service.create_application(
      "alice-app",
      "shared application",
      "created in Beijing",
      alice,
    ),
    public_service.create_application(
      "bob-app",
      "shared application",
      "created in Shenzhen",
      bob,
    ),
    return_exceptions=True,
  )

  entries = _application_entries(state)
  _assert_single_create_winner(results, entries)
  assert set(entries).issubset({"alice-app", "bob-app"})
  assert next(iter(entries.values()))["shown_name"] == "shared application"


class _ExistingApplication:
  def __init__(self, *, author, approver=None, enabled=True):
    self.name = "authority-app"
    self.shown_name = "Authority application"
    self.description = "authoritative metadata"
    self.enabled = enabled
    self.enabled_at = datetime(2026, 8, 1, tzinfo=timezone.utc) if enabled else None
    self.provisioning_status = "ready" if enabled else "pending"
    self.provisioning_error = ""
    self.provisioning_updated_at = None
    self.quota_bytes = 100 * 1024 ** 3
    self.author = author
    self.approver = approver
    self.updated_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    self.saved = 0

  async def save(self):
    self.saved += 1


def _authoritative_entry(*, approver_username="reviewer-b"):
  return {
    "shown_name": "Authority application",
    "description": "authoritative metadata",
    "enabled": True,
    "enabled_at": "2026-08-01T00:00:00+00:00",
    "provisioning_status": "ready",
    "provisioning_error": "",
    "provisioning_updated_at": None,
    "quota_bytes": 100 * 1024 ** 3,
    "author_username": "bob",
    "author_name": "Bob",
    "approver_username": approver_username,
  }


@pytest.mark.asyncio
async def test_application_pull_replaces_stale_local_owner(monkeypatch):
  alice = SimpleNamespace(id="user-alice", username="alice", name="Alice")
  bob = SimpleNamespace(id="user-bob", username="bob", name="Bob")
  reviewer = SimpleNamespace(id="reviewer-b", username="reviewer-b", name="Reviewer B")
  app = _ExistingApplication(author=alice, approver=reviewer)

  async def read_application(_app_name):
    return app

  async def resolve_user(username, name=""):
    del name
    return {"bob": bob, "reviewer-b": reviewer}[username]

  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_name",
    read_application,
  )
  monkeypatch.setattr(sync_module, "get_or_create_sync_user", resolve_user)

  projected, _changed = await sync_module.upsert_application_from_etcd(
    app.name,
    _authoritative_entry(),
  )

  assert projected is app
  assert app.author is bob
  assert app.saved == 1


@pytest.mark.asyncio
async def test_application_pull_replaces_and_clears_approver(monkeypatch):
  bob = SimpleNamespace(id="user-bob", username="bob", name="Bob")
  reviewer_a = SimpleNamespace(
    id="reviewer-a",
    username="reviewer-a",
    name="Reviewer A",
  )
  reviewer_b = SimpleNamespace(
    id="reviewer-b",
    username="reviewer-b",
    name="Reviewer B",
  )
  app = _ExistingApplication(author=bob, approver=reviewer_a)

  async def read_application(_app_name):
    return app

  async def resolve_user(username, name=""):
    del name
    return {"bob": bob, "reviewer-b": reviewer_b}[username]

  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_name",
    read_application,
  )
  monkeypatch.setattr(sync_module, "get_or_create_sync_user", resolve_user)

  await sync_module.upsert_application_from_etcd(
    app.name,
    _authoritative_entry(approver_username="reviewer-b"),
  )
  assert app.approver is reviewer_b

  await sync_module.upsert_application_from_etcd(
    app.name,
    _authoritative_entry(approver_username=""),
  )
  assert app.approver is None
  assert app.saved == 2


@pytest.mark.asyncio
async def test_concurrent_local_application_projection_is_idempotent(monkeypatch):
  store = {}
  read_count = 0
  initial_reads_released = asyncio.Event()
  save_lock = asyncio.Lock()
  owner = SimpleNamespace(id="user-bob", username="bob", name="Bob")

  class FakeApplication:
    def __init__(self, **values):
      self.__dict__.update(values)
      self.updated_at = datetime(2026, 8, 1, tzinfo=timezone.utc)

    async def save(self):
      async with save_lock:
        existing = store.get(self.name)
        if existing is not None and existing is not self:
          raise DuplicateKeyError("duplicate application name")
        store[self.name] = self

  async def read_application(app_name):
    nonlocal read_count
    read_count += 1
    if read_count <= 2:
      if read_count == 2:
        initial_reads_released.set()
      await asyncio.wait_for(initial_reads_released.wait(), timeout=1)
      return None
    return store.get(app_name)

  async def resolve_user(_username, _name=""):
    return owner

  monkeypatch.setattr(public_model, "Application", FakeApplication)
  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_name",
    read_application,
  )
  monkeypatch.setattr(sync_module, "get_or_create_sync_user", resolve_user)

  entry = _authoritative_entry(approver_username="")
  results = await asyncio.gather(
    sync_module.upsert_application_from_etcd("authority-app", entry),
    sync_module.upsert_application_from_etcd("authority-app", entry),
  )

  assert set(store) == {"authority-app"}
  assert results[0][0] is store["authority-app"]
  assert results[1][0] is store["authority-app"]


@pytest.mark.asyncio
async def test_stale_application_publish_preserves_authoritative_owner_and_quota(
  monkeypatch,
):
  state = _EtcdState()
  key = f"{etcd_op.ETCD_PREFIX}{sync_module.ETCD_KEY_APPLICATIONS}".encode()
  authoritative = _authoritative_entry(approver_username="")
  authoritative.update({
    "enabled": False,
    "enabled_at": None,
    "provisioning_status": "pending",
    "quota_bytes": 200 * 1024 ** 3,
    "origin_region": "beijing",
  })
  state.revision = 1
  state.items[key] = {
    "value": json.dumps({"authority-app": authoritative}).encode(),
    "create_revision": 1,
    "mod_revision": 1,
  }

  alice = SimpleNamespace(id="user-alice", username="alice", name="Alice")
  reviewer = SimpleNamespace(
    id="reviewer-a",
    username="reviewer-a",
    name="Reviewer A",
  )
  stale = _ExistingApplication(author=alice, approver=reviewer, enabled=True)
  stale.id = "local-app-id"
  stale.provisioning_status = "ready"
  stale.quota_bytes = 100 * 1024 ** 3

  async def get_client():
    return _FakeEtcd(state)

  async def read_application(application_id):
    assert application_id == stale.id
    return stale

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_id",
    read_application,
  )

  await sync_module.publish_application(stale)

  published = _application_entries(state)[stale.name]
  assert published["enabled"] is True
  assert published["provisioning_status"] == "ready"
  assert published["author_username"] == "bob"
  assert published["quota_bytes"] == 200 * 1024 ** 3
