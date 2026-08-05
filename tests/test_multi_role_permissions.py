import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from bson import ObjectId

from src.core.exception import CustomException, ErrorDesc
from src.core import sync as sync_module
from src.modules.auth import schema as auth_schema
from src.modules.auth import service as auth_service
from src.modules.auth import crud as auth_crud
from src.modules.auth import oa as oa_service
from src.modules.public import service as public_service
from src.utils.helpers import get_full_permissions


class FakeUser(SimpleNamespace):
  async def save(self):
    return None


def role(name: str, permissions: list[str], role_id: str):
  return SimpleNamespace(
    id=ObjectId(),
    name=name,
    is_admin=name == "管理员",
    permissions=permissions,
  )


@asynccontextmanager
async def unlocked_role_update(*_args, **_kwargs):
  yield


@pytest.fixture(autouse=True)
def no_remote_identity_refresh(monkeypatch):
  async def refresh(_username):
    return None

  monkeypatch.setattr(sync_module, "refresh_user_identity_from_etcd", refresh)


def test_system_manage_inherits_new_permissions_and_role_payload_is_compatible():
  permissions = get_full_permissions(["system_manage"])
  assert "application_quota_manage" in permissions
  assert "storage_operations_manage" in permissions

  assert auth_schema.UpdateUserRoleRequest(role="用户管理员").role_names == ["用户管理员"]
  assert auth_schema.UpdateUserRoleRequest(
    roles=["用户", "应用管理员", "应用管理员"],
  ).role_names == ["用户", "应用管理员"]


def test_etcd_user_cas_never_removes_the_last_superadmin():
  users = {
    "alice": {
      "role_names": ["用户", "管理员"],
      "roles_version": 0,
      "updated_at": "2026-08-05T01:00:00+00:00",
      "origin_region": "beijing",
    },
    "bob": {
      "role_names": ["用户", "管理员"],
      "roles_version": 0,
      "updated_at": "2026-08-05T01:00:00+00:00",
      "origin_region": "beijing",
    },
  }
  sync_module._merge_user_entry(users, "alice", {
    **users["alice"],
    "role_names": ["用户"],
    "roles_version": 1,
    "updated_at": "2026-08-05T02:00:00+00:00",
  }, fields={"roles"})
  with pytest.raises(sync_module.LastSuperadminError):
    sync_module._merge_user_entry(users, "bob", {
      **users["bob"],
      "role_names": ["用户"],
      "roles_version": 1,
      "updated_at": "2026-08-05T02:00:00+00:00",
    }, fields={"roles"})
  assert users["bob"]["role_names"] == ["用户", "管理员"]


@pytest.mark.asyncio
async def test_restart_publish_keeps_etcd_last_admin_for_following_pull(monkeypatch):
  local_entry = {
    "role_names": ["用户"],
    "roles_version": 1,
    "updated_at": "2026-08-05T03:00:00+00:00",
    "origin_region": "beijing",
  }
  etcd_admin_entry = {
    "role_names": ["用户", "管理员"],
    "roles_version": 2,
    "updated_at": "2026-08-05T02:00:00+00:00",
    "origin_region": "beijing",
  }
  captured = {}

  async def local_users():
    return [SimpleNamespace(id="alice-id", username="alice", is_sync=False)]

  @asynccontextmanager
  async def identity_lock(*_args, **_kwargs):
    yield None

  async def read_user(_user_id):
    return SimpleNamespace(id="alice-id", username="alice", is_sync=False)

  async def merge(key, mutator, client=None):
    assert key == sync_module.ETCD_KEY_USERS
    current = {"alice": dict(etcd_admin_entry)}
    captured.update(mutator(current))
    return captured

  monkeypatch.setattr("src.modules.auth.crud.list_local_users", local_users)
  monkeypatch.setattr("src.modules.auth.crud.read_user_by_id", read_user)
  monkeypatch.setattr(sync_module, "user_role_update_lock", identity_lock)
  monkeypatch.setattr(sync_module, "user_to_etcd_entry", lambda _user: dict(local_entry))
  monkeypatch.setattr("src.core.etcd_op.merge_update_etcd_key", merge)

  await sync_module.publish_local_users(client=SimpleNamespace())

  assert captured["alice"]["role_names"] == etcd_admin_entry["role_names"]
  assert captured["alice"]["roles_version"] == 2
  assert "管理员" in sync_module._entry_role_names(captured["alice"])


@pytest.mark.asyncio
async def test_user_role_lock_allows_only_one_concurrent_holder():
  entered = asyncio.Event()
  release_holder = asyncio.Event()

  class FakeLock:
    locked = False
    releases = 0

    async def acquire(self, timeout):
      assert timeout == 0
      if self.locked:
        return False
      self.locked = True
      return True

    async def refresh(self):
      return None

    async def is_acquired(self):
      return self.locked

    async def release(self):
      self.locked = False
      self.releases += 1

  fake_lock = FakeLock()

  class FakeClient:
    keys = []

    def lock(self, key, ttl):
      self.keys.append((key, ttl))
      return fake_lock

  client = FakeClient()

  async def holder():
    async with sync_module.user_role_update_lock("alice", client=client, timeout=0):
      entered.set()
      await release_holder.wait()

  task = asyncio.create_task(holder())
  await entered.wait()
  with pytest.raises(sync_module.UserRoleUpdateLockBusyError):
    async with sync_module.user_role_update_lock("alice", client=client, timeout=0):
      pytest.fail("contending role update must not enter")
  release_holder.set()
  await task

  assert client.keys == [
    (b"/storagent/locks/user-role-update/alice", 30),
    (b"/storagent/locks/user-role-update/alice", 30),
  ]
  assert fake_lock.releases == 1


@pytest.mark.asyncio
async def test_user_identity_lock_refresh_failure_cancels_holder(monkeypatch):
  real_sleep = asyncio.sleep

  async def immediate_sleep(_delay):
    await real_sleep(0)

  class FakeLock:
    released = False

    async def acquire(self, timeout):
      return True

    async def refresh(self):
      raise RuntimeError("lease refresh failed")

    async def is_acquired(self):
      return True

    async def release(self):
      self.released = True

  fake_lock = FakeLock()
  client = SimpleNamespace(lock=lambda *_args, **_kwargs: fake_lock)
  monkeypatch.setattr(sync_module.asyncio, "sleep", immediate_sleep)

  with pytest.raises(sync_module.UserIdentityUpdateLockLostError):
    async with sync_module.user_role_update_lock(
      "alice",
      client=client,
      timeout=0,
    ):
      await asyncio.Event().wait()

  assert fake_lock.released is True


@pytest.mark.asyncio
async def test_role_update_lock_contention_returns_status_error(monkeypatch):
  target = SimpleNamespace(id="target", username="alice", is_sync=False)
  actor = SimpleNamespace(id="manager", roles=[])

  async def read_user(_user_id):
    return target

  @asynccontextmanager
  async def busy_lock(*_args, **_kwargs):
    raise sync_module.UserRoleUpdateLockBusyError("busy")
    yield

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_id", read_user)
  monkeypatch.setattr(sync_module, "user_role_update_lock", busy_lock)

  with pytest.raises(CustomException) as exc_info:
    await auth_service.update_user_role_for_admin("target", ["用户"], actor)
  assert exc_info.value.code == ErrorDesc.STATUS_ERR.code


@pytest.mark.asyncio
async def test_role_update_rolls_back_when_identity_lock_is_lost(monkeypatch):
  basic = role("用户", [], "basic")
  app_admin = role("应用管理员", ["application_manage"], "app-admin")
  user_admin = role("用户管理员", ["user_manage"], "user-admin")
  superadmin = role("管理员", ["system_manage"], "superadmin")
  target = FakeUser(
    id="target",
    username="alice",
    name="Alice",
    roles=[basic],
    permissions=[],
    roles_version=4,
    is_sync=False,
    created_at=None,
    updated_at=None,
  )
  actor = FakeUser(id="manager", username="manager", roles=[basic, user_admin])

  class LosingGuard:
    checks = 0

    async def ensure_owned(self):
      self.checks += 1
      if self.checks == 2:
        raise sync_module.UserIdentityUpdateLockLostError("lost")

  @asynccontextmanager
  async def losing_lock(*_args, **_kwargs):
    yield LosingGuard()

  async def read_user(_user_id):
    return target

  async def get_basic():
    return basic

  async def get_admin():
    return superadmin

  async def get_role(role_name):
    return {"应用管理员": app_admin}.get(role_name)

  async def must_not_publish(*_args, **_kwargs):
    pytest.fail("lost lock must prevent Etcd commit")

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_id", read_user)
  monkeypatch.setattr(auth_service.user_crud, "get_basic_role", get_basic)
  monkeypatch.setattr(auth_service.user_crud, "get_admin_role", get_admin)
  monkeypatch.setattr(auth_service.user_crud, "get_role_by_name", get_role)
  monkeypatch.setattr(sync_module, "user_role_update_lock", losing_lock)
  monkeypatch.setattr(sync_module, "publish_user", must_not_publish)

  with pytest.raises(CustomException) as exc_info:
    await auth_service.update_user_role_for_admin(
      "target",
      ["应用管理员"],
      actor,
    )

  assert exc_info.value.code == ErrorDesc.SYNC_FAILED.code
  assert [item.name for item in target.roles] == ["用户"]
  assert target.roles_version == 4


@pytest.mark.asyncio
async def test_user_manager_assigns_multiple_specialty_roles_and_user_is_retained(monkeypatch):
  now = datetime(2026, 8, 5, tzinfo=timezone.utc)
  basic = role("用户", ["application_view", "region_view"], "basic")
  app_admin = role(
    "应用管理员",
    ["application_manage", "application_quota_manage"],
    "app-admin",
  )
  operations_admin = role(
    "运维管理员",
    ["storage_operations_manage"],
    "operations-admin",
  )
  user_admin = role("用户管理员", ["user_manage"], "user-admin")
  superadmin = role("管理员", ["system_manage"], "superadmin")
  target = FakeUser(
    id="target",
    username="alice",
    name="Alice",
    roles=[basic],
    permissions=list(basic.permissions),
    is_sync=False,
    created_at=now,
    updated_at=now,
  )
  actor = FakeUser(id="manager", roles=[basic, user_admin])
  roles = {
    item.name: item
    for item in (basic, app_admin, operations_admin, user_admin, superadmin)
  }

  async def read_user(_user_id):
    return target

  async def get_role(role_name):
    return roles.get(role_name)

  async def publish_user(_user, **kwargs):
    assert kwargs["fields"] == {"roles"}
    return None

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_id", read_user)
  async def get_basic():
    return basic

  async def get_admin():
    return superadmin

  monkeypatch.setattr(auth_service.user_crud, "get_basic_role", get_basic)
  monkeypatch.setattr(auth_service.user_crud, "get_admin_role", get_admin)
  monkeypatch.setattr(auth_service.user_crud, "get_role_by_name", get_role)
  monkeypatch.setattr("src.core.sync.publish_user", publish_user)
  monkeypatch.setattr(sync_module, "user_role_update_lock", unlocked_role_update)
  monkeypatch.setattr(auth_service, "convert_utc_to_local_str", lambda _value: "time")

  result = await auth_service.update_user_role_for_admin(
    "target",
    ["应用管理员", "运维管理员"],
    actor,
  )

  assert [item.name for item in target.roles] == ["用户", "应用管理员", "运维管理员"]
  assert "application_quota_manage" in target.permissions
  assert "storage_operations_manage" in target.permissions
  auth_schema.UpdateUserRoleResponse.model_validate(result)


@pytest.mark.asyncio
async def test_specialty_user_manager_cannot_escalate_self(monkeypatch):
  basic = role("用户", [], "basic")
  user_admin = role("用户管理员", ["user_manage"], "user-admin")
  superadmin = role("管理员", ["system_manage"], "superadmin")
  actor = FakeUser(
    id="manager",
    username="manager",
    roles=[basic, user_admin],
    permissions=["user_manage"],
    is_sync=False,
  )

  async def read_user(_user_id):
    return actor

  async def get_basic():
    return basic

  async def get_admin():
    return superadmin

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_id", read_user)
  monkeypatch.setattr(auth_service.user_crud, "get_basic_role", get_basic)
  monkeypatch.setattr(auth_service.user_crud, "get_admin_role", get_admin)
  monkeypatch.setattr(sync_module, "user_role_update_lock", unlocked_role_update)

  with pytest.raises(CustomException) as exc_info:
    await auth_service.update_user_role_for_admin(
      "manager",
      ["用户", "用户管理员", "应用管理员"],
      actor,
    )
  assert exc_info.value.code == ErrorDesc.INSUFFICIENT_PERMISSIONS.code


@pytest.mark.asyncio
async def test_application_notification_runs_after_etcd_claim_and_is_best_effort(monkeypatch):
  app = SimpleNamespace(
    id="app-id",
    name="sample-app",
    shown_name="Sample App",
    description="description",
  )
  author = SimpleNamespace(username="creator", name="Creator")
  calls = []

  async def merge(_key, mutator):
    applications = mutator({})
    calls.append("claim")
    return applications

  async def project(_name, _entry):
    calls.append("project")
    return app, True

  async def notify(_app, _author):
    calls.append("notify")
    # Notification failures are handled inside the real helper and never roll back.

  async def application_response(_app):
    return _app

  monkeypatch.setattr("src.core.etcd_op.merge_update_etcd_key", merge)
  monkeypatch.setattr(public_service.sync_module, "upsert_application_from_etcd", project)
  monkeypatch.setattr(public_service, "_notify_application_managers", notify)
  monkeypatch.setattr(public_service, "_application_response", application_response)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)

  result = await public_service.create_application(
    "sample-app",
    "sample app",
    "description",
    author,
  )
  assert result is app
  assert calls == ["claim", "project", "notify"]


@pytest.mark.asyncio
async def test_application_notification_failure_does_not_escape(monkeypatch, caplog):
  recipients = [
    SimpleNamespace(username="manager-a"),
    SimpleNamespace(username="manager-b"),
  ]
  sent = []

  async def list_recipients(_permission):
    return recipients

  async def send(user_id, *_args, **_kwargs):
    sent.append(user_id)
    if user_id == "manager-a":
      return oa_service.OADeliveryResult("failed", "rejected")
    raise RuntimeError("OA unavailable")

  monkeypatch.setattr(auth_crud, "list_users_with_permission", list_recipients)
  monkeypatch.setattr(oa_service, "send_agenda_message", send)

  await public_service._notify_application_managers(
    SimpleNamespace(name="sample-app", shown_name="Sample App"),
    SimpleNamespace(username="creator", name="Creator"),
  )

  assert sent == ["manager-a", "manager-b"]
  assert "应用创建 OA 通知失败" in caplog.text
  assert "应用创建 OA 通知异常" in caplog.text
