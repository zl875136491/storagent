"""管理员角色管理与 API Key 管理员吊销相关单测。"""
from contextlib import asynccontextmanager
import pytest
from types import SimpleNamespace

from src.core.exception import CustomException, ErrorDesc
from src.modules.auth import service as auth_service
from src.modules.public import service as public_service


@pytest.fixture(autouse=True)
def no_role_update_lock(monkeypatch):
  @asynccontextmanager
  async def unlocked(*_args, **_kwargs):
    yield

  monkeypatch.setattr("src.core.sync.user_role_update_lock", unlocked)

  async def no_refresh(_username):
    return None

  monkeypatch.setattr("src.core.sync.refresh_user_identity_from_etcd", no_refresh)


@pytest.mark.asyncio
async def test_update_user_role_promotes_to_admin(monkeypatch):
  admin_role = SimpleNamespace(id="admin-id", name="管理员", is_admin=True)
  basic_role = SimpleNamespace(id="basic-id", name="用户", is_admin=False)
  target = SimpleNamespace(
    id="u1",
    username="alice",
    name="Alice",
    roles=[basic_role],
    is_sync=False,
    created_at=None,
    updated_at=None,
  )

  async def read_user_by_id(_id):
    return target

  async def get_admin_role():
    return admin_role

  async def get_basic_role():
    return basic_role

  async def count_admin_users():
    return 1

  async def update_user_role(user, role):
    user.roles = [role]
    return user

  async def publish_user(_user, **kwargs):
    assert kwargs["fields"] == {"roles"}
    return None

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_id", read_user_by_id)
  monkeypatch.setattr(auth_service.user_crud, "get_admin_role", get_admin_role)
  monkeypatch.setattr(auth_service.user_crud, "get_basic_role", get_basic_role)
  monkeypatch.setattr(auth_service.user_crud, "count_admin_users", count_admin_users)
  monkeypatch.setattr(auth_service.user_crud, "update_user_role", update_user_role)
  monkeypatch.setattr("src.core.sync.publish_user", publish_user)
  monkeypatch.setattr(
    auth_service,
    "convert_utc_to_local_str",
    lambda _v: "t",
  )

  result = await auth_service.update_user_role_for_admin("u1", "管理员")
  assert result["is_admin"] is True
  assert result["role_name"] == "管理员"


@pytest.mark.asyncio
async def test_update_user_role_blocks_last_admin_demotion(monkeypatch):
  admin_role = SimpleNamespace(id="admin-id", name="管理员", is_admin=True)
  basic_role = SimpleNamespace(id="basic-id", name="用户", is_admin=False)
  target = SimpleNamespace(
    id="u1",
    username="zhangle",
    name="ZL",
    roles=[admin_role],
    is_sync=False,
  )

  async def read_user_by_id(_id):
    return target

  async def get_admin_role():
    return admin_role

  async def get_basic_role():
    return basic_role

  async def count_admin_users():
    return 1

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_id", read_user_by_id)
  monkeypatch.setattr(auth_service.user_crud, "get_admin_role", get_admin_role)
  monkeypatch.setattr(auth_service.user_crud, "get_basic_role", get_basic_role)
  monkeypatch.setattr(auth_service.user_crud, "count_admin_users", count_admin_users)

  with pytest.raises(CustomException) as exc:
    await auth_service.update_user_role_for_admin("u1", "用户")
  assert exc.value.code == ErrorDesc.INVALID_PARAMS.code


@pytest.mark.asyncio
async def test_admin_revoke_marks_destory_by_admin(monkeypatch):
  app = SimpleNamespace(id="app1", author=SimpleNamespace(id="owner"))
  key = SimpleNamespace(id="k1", application=SimpleNamespace(id="app1"), deleted=False)
  actor = SimpleNamespace(id="admin", username="zhangle", roles=[SimpleNamespace(id="admin-id")])
  captured = {}

  async def read_api_key_by_id(_id):
    return key if _id == "k1" or True else None

  async def read_application_by_id(_id):
    return app

  async def get_admin_role():
    return SimpleNamespace(id="admin-id")

  async def delete_api_key_by_id(_id, *, destory_by_admin=False):
    captured["destory_by_admin"] = destory_by_admin
    key.deleted = True
    key.destory_by_admin = destory_by_admin
    return True

  async def publish_api_key(_obj):
    return None

  monkeypatch.setattr(public_service.public_crud, "read_api_key_by_id", read_api_key_by_id)
  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_application_by_id)
  monkeypatch.setattr(public_service.public_crud, "delete_api_key_by_id", delete_api_key_by_id)
  monkeypatch.setattr(public_service.sync_module, "publish_api_key", publish_api_key)
  monkeypatch.setattr("src.modules.auth.crud.get_admin_role", get_admin_role)
  monkeypatch.setattr("src.core.audit.audit", lambda *a, **k: None)

  result = await public_service.revoke_api_key("k1", actor)
  assert result["message"] == "API密钥已吊销"
  assert captured["destory_by_admin"] is True


@pytest.mark.asyncio
async def test_owner_revoke_does_not_mark_destory_by_admin(monkeypatch):
  owner = SimpleNamespace(id="owner")
  app = SimpleNamespace(id="app1", author=owner)
  key = SimpleNamespace(id="k1", application=SimpleNamespace(id="app1"), deleted=False)
  actor = SimpleNamespace(id="owner", username="alice", roles=[])
  captured = {}

  async def read_api_key_by_id(_id):
    return key

  async def read_application_by_id(_id):
    return app

  async def get_admin_role():
    return SimpleNamespace(id="admin-id")

  async def delete_api_key_by_id(_id, *, destory_by_admin=False):
    captured["destory_by_admin"] = destory_by_admin
    return True

  async def publish_api_key(_obj):
    return None

  monkeypatch.setattr(public_service.public_crud, "read_api_key_by_id", read_api_key_by_id)
  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_application_by_id)
  monkeypatch.setattr(public_service.public_crud, "delete_api_key_by_id", delete_api_key_by_id)
  monkeypatch.setattr(public_service.sync_module, "publish_api_key", publish_api_key)
  monkeypatch.setattr("src.modules.auth.crud.get_admin_role", get_admin_role)
  monkeypatch.setattr("src.core.audit.audit", lambda *a, **k: None)

  await public_service.revoke_api_key("k1", actor)
  assert captured["destory_by_admin"] is False


@pytest.mark.asyncio
async def test_get_api_key_list_resolves_unfetched_application_link(monkeypatch):
  class UnfetchedLink:
    def __init__(self):
      self.ref = SimpleNamespace(id="app1")

    async def fetch(self):
      return SimpleNamespace(id="app1", name="demo", shown_name="Demo")

  key = SimpleNamespace(
    id="k1",
    key_hint="sk_****abcd",
    application=UnfetchedLink(),
    expired_at="2026-01-01T00:00:00+00:00",
    deleted=False,
    destory_by_admin=False,
  )
  actor = SimpleNamespace(id="admin", roles=[SimpleNamespace(id="admin-id")])

  async def get_admin_role():
    return SimpleNamespace(id="admin-id")

  async def read_all(*, include_admin_destroyed=False):
    del include_admin_destroyed
    return [key]

  monkeypatch.setattr("src.modules.auth.crud.get_admin_role", get_admin_role)
  monkeypatch.setattr(public_service.public_crud, "read_all_api_keys", read_all)

  result = await public_service.get_api_key_list(actor)
  assert result["data"][0]["application"] == {
    "id": "app1",
    "name": "demo",
    "shown_name": "Demo",
  }


@pytest.mark.asyncio
async def test_get_api_key_list_survives_deleted_application_link(monkeypatch):
  class MissingLink:
    def __init__(self):
      self.ref = SimpleNamespace(id="dead-app")

    async def fetch(self):
      return None

  key = SimpleNamespace(
    id="k1",
    key_hint="sk_****abcd",
    application=MissingLink(),
    expired_at="2026-01-01T00:00:00+00:00",
    deleted=False,
    destory_by_admin=False,
  )
  actor = SimpleNamespace(id="admin", roles=[SimpleNamespace(id="admin-id")])

  async def get_admin_role():
    return SimpleNamespace(id="admin-id")

  async def read_all(*, include_admin_destroyed=False):
    del include_admin_destroyed
    return [key]

  async def read_application(_application_id):
    return None

  monkeypatch.setattr("src.modules.auth.crud.get_admin_role", get_admin_role)
  monkeypatch.setattr(public_service.public_crud, "read_all_api_keys", read_all)
  monkeypatch.setattr(
    public_service.public_crud,
    "read_application_by_id",
    read_application,
  )

  result = await public_service.get_api_key_list(actor)
  assert result["data"][0]["application"]["id"] == "dead-app"
  assert result["data"][0]["application"]["shown_name"] == "应用已删除"

