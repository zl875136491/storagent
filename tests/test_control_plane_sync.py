"""Cross-region identity and topology control-plane synchronization."""
import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from pymongo.errors import DuplicateKeyError

from src.core import etcd_op
from src.core import sync as sync_module
from src.modules.storage import crud as storage_crud
from src.core import minio_op
from src.modules.auth import crud as auth_crud
from src.modules.auth.model import Role
from src.modules.graph import service as graph_service
from src.modules.public import crud as public_crud


def _user_entry(updated_at: str, origin: str) -> dict:
  return {
    "name": "Alice",
    "hashed_password_enc": "enc:v1:test",
    "role_names": ["user"],
    "updated_at": updated_at,
    "origin_region": origin,
  }


def test_user_conflict_prefers_newest_then_authority(monkeypatch):
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")
  old = _user_entry("2026-07-30T01:00:00+00:00", "beijing")
  new = _user_entry("2026-07-30T02:00:00+00:00", "shenzhen")
  assert sync_module._prefer_user_entry(old, new) is True
  assert sync_module._prefer_user_entry(new, old) is False

  same_time_remote = _user_entry("2026-07-30T02:00:00+00:00", "shenzhen")
  same_time_authority = _user_entry("2026-07-30T02:00:00+00:00", "beijing")
  assert sync_module._prefer_user_entry(same_time_remote, same_time_authority) is True
  assert sync_module._prefer_user_entry(same_time_authority, same_time_remote) is False


def test_authority_role_definition_replaces_older_same_origin_entry(monkeypatch):
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")
  current = {
    "name": "管理员",
    "permissions": ["system_manage"],
    "origin_region": "beijing",
  }
  upgraded = {
    "name": "管理员",
    "permissions": ["system_manage", "application_quota_manage"],
    "origin_region": "beijing",
  }
  assert sync_module._prefer_authority_entry(current, upgraded) is True


@pytest.mark.asyncio
async def test_region_projection_recovers_duplicate_insert(monkeypatch):
  store = {}
  initial_reads = 0
  release_initial_reads = asyncio.Event()

  async def read_region(name):
    nonlocal initial_reads
    if name in store:
      return store[name]
    initial_reads += 1
    if initial_reads <= 2:
      if initial_reads == 2:
        release_initial_reads.set()
      await asyncio.wait_for(release_initial_reads.wait(), timeout=1)
      return None
    return store.get(name)

  async def create_region(name, shown_name):
    if name in store:
      raise DuplicateKeyError("duplicate region name")
    region = SimpleNamespace(name=name, shown_name=shown_name)
    store[name] = region
    return region

  monkeypatch.setattr(public_crud, "read_region_by_name", read_region)
  monkeypatch.setattr(public_crud, "create_region", create_region)

  results = await asyncio.gather(
    sync_module._get_or_create_region_from_control_plane("beijing", "Beijing"),
    sync_module._get_or_create_region_from_control_plane("beijing", "Beijing"),
  )

  assert set(store) == {"beijing"}
  assert results[0][0] is store["beijing"]
  assert results[1][0] is store["beijing"]
  assert sum(created for _region, created in results) == 1


@pytest.mark.asyncio
async def test_alias_refresh_reads_shared_server_registry(monkeypatch):
  calls = []

  class FakeClient:
    async def close(self):
      calls.append("close")

  client = FakeClient()
  servers = {
    "beijing": {"host": "minio-a"},
    "shenzhen": {"host": "minio-b"},
  }

  async def get_client():
    calls.append("connect")
    return client

  async def pull(key, *, client):
    calls.append(("pull", key, client))
    return servers

  async def setup_aliases(data):
    calls.append(("aliases", data))

  monkeypatch.setattr(etcd_op, "get_etcd_client", get_client)
  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", pull)
  monkeypatch.setattr(sync_module, "setup_mc_aliases", setup_aliases)

  assert await sync_module.ensure_mc_aliases_from_etcd() == 2
  assert calls == [
    "connect",
    ("pull", sync_module.ETCD_KEY_SERVERS, client),
    ("aliases", servers),
    "close",
  ]


@pytest.mark.asyncio
async def test_alias_refresh_falls_back_to_mongo(monkeypatch):
  calls = []
  servers = [
    SimpleNamespace(
      name="shown",
      host="10.0.0.8",
      minio_port=9000,
      region=SimpleNamespace(name="beijing"),
    )
  ]

  async def list_servers():
    return servers

  def creds(_server):
    return ("ak", "sk")

  async def set_alias(**kwargs):
    calls.append(kwargs)
    return True, "ok"

  monkeypatch.setattr(storage_crud, "read_minio_server_list", list_servers)
  monkeypatch.setattr(storage_crud, "plain_minio_credentials", creds)
  monkeypatch.setattr(minio_op, "set_site_alias", set_alias)

  assert await sync_module.ensure_mc_aliases_from_mongo() == 1
  assert calls == [
    {
      "site_name": "beijing",
      "endpoint": "10.0.0.8:9000",
      "admin_user": "ak",
      "admin_password": "sk",
    }
  ]


def test_user_entry_encrypts_password_hash_and_uses_role_names(monkeypatch):
  monkeypatch.setattr(sync_module.settings, "SECRET_KEY", "unit-test-secret-key")
  monkeypatch.setattr(sync_module.settings, "REGION", "shenzhen")
  now = datetime(2026, 7, 30, tzinfo=timezone.utc)
  user = SimpleNamespace(
    name="Alice",
    hashed_password="$2b$04$hash",
    auth_version=3,
    roles_version=2,
    roles=[SimpleNamespace(name="user"), SimpleNamespace(name="admin")],
    created_at=now,
    updated_at=now,
  )

  entry = sync_module.user_to_etcd_entry(user)
  assert entry["hashed_password_enc"].startswith("enc:v1:")
  assert "$2b$04$hash" not in entry["hashed_password_enc"]
  assert entry["role_names"] == ["admin", "user"]
  assert entry["auth_version"] == 3
  assert entry["roles_version"] == 2
  assert entry["origin_region"] == "shenzhen"


def test_role_publish_cannot_roll_back_password_or_auth_version():
  users = {
    "alice": {
      "name": "Alice",
      "hashed_password_enc": "new-password",
      "auth_version": 4,
      "role_names": ["用户"],
      "roles_version": 2,
      "updated_at": "2026-08-05T02:00:00+00:00",
      "origin_region": "beijing",
    },
  }
  stale_role_writer = {
    **users["alice"],
    "hashed_password_enc": "old-password",
    "auth_version": 3,
    "role_names": ["用户", "应用管理员"],
    "roles_version": 3,
    "updated_at": "2026-08-05T03:00:00+00:00",
    "origin_region": "shenzhen",
  }

  sync_module._merge_user_entry(
    users,
    "alice",
    stale_role_writer,
    fields={"roles"},
  )

  assert users["alice"]["hashed_password_enc"] == "new-password"
  assert users["alice"]["auth_version"] == 4
  assert users["alice"]["role_names"] == ["用户", "应用管理员"]
  assert users["alice"]["roles_version"] == 3


def test_password_publish_preserves_newer_roles_and_reconcile_never_decreases_versions():
  users = {
    "alice": {
      "name": "Alice",
      "hashed_password_enc": "old-password",
      "auth_version": 4,
      "role_names": ["用户", "运维管理员"],
      "roles_version": 7,
      "updated_at": "2026-08-05T02:00:00+00:00",
      "origin_region": "beijing",
    },
  }
  password_writer = {
    **users["alice"],
    "hashed_password_enc": "new-password",
    "auth_version": 5,
    "role_names": ["用户"],
    "roles_version": 6,
  }
  sync_module._merge_user_entry(
    users,
    "alice",
    password_writer,
    fields={"auth"},
  )
  assert users["alice"]["hashed_password_enc"] == "new-password"
  assert users["alice"]["auth_version"] == 5
  assert users["alice"]["role_names"] == ["用户", "运维管理员"]
  assert users["alice"]["roles_version"] == 7

  stale_reconcile = {
    **password_writer,
    "hashed_password_enc": "very-old-password",
    "auth_version": 2,
    "role_names": ["用户"],
    "roles_version": 3,
    "updated_at": "2026-08-05T04:00:00+00:00",
  }
  sync_module._merge_user_entry(users, "alice", stale_reconcile)
  assert users["alice"]["hashed_password_enc"] == "new-password"
  assert users["alice"]["auth_version"] == 5
  assert users["alice"]["role_names"] == ["用户", "运维管理员"]
  assert users["alice"]["roles_version"] == 7


@pytest.mark.asyncio
async def test_etcd_pull_updates_roles_without_decreasing_local_auth_version(monkeypatch):
  monkeypatch.setattr(sync_module.settings, "SECRET_KEY", "unit-test-secret-key")
  now = datetime(2026, 8, 5, tzinfo=timezone.utc)
  basic = SimpleNamespace(name="用户", permissions=[])
  operations = SimpleNamespace(name="运维管理员", permissions=["storage_operations_manage"])

  class FakeUser:
    username = "alice"
    name = "Alice"
    hashed_password = "new-password"
    auth_version = 4
    roles = [basic]
    roles_version = 2
    permissions = []
    is_sync = False
    created_at = now
    updated_at = now

    async def save(self):
      return None

  user = FakeUser()

  async def get_basic():
    return basic

  async def find_role(*_args, **_kwargs):
    return operations

  async def get_permissions(roles):
    return sorted({permission for role in roles for permission in role.permissions})

  async def read_user(_username):
    return user

  monkeypatch.setattr(auth_crud, "get_basic_role", get_basic)
  monkeypatch.setattr(auth_crud, "get_all_permissions", get_permissions)
  monkeypatch.setattr(auth_crud, "read_user_by_username", read_user)
  monkeypatch.setattr(Role, "name", object(), raising=False)
  monkeypatch.setattr(Role, "find_one", find_role)

  await sync_module._sync_user_to_mongo_locked("alice", {
    "name": "Alice",
    "hashed_password_enc": sync_module.encrypt_secret("old-password"),
    "auth_version": 3,
    "role_names": ["用户", "运维管理员"],
    "roles_version": 3,
    "created_at": now.isoformat(),
    "updated_at": now.isoformat(),
  })

  assert user.hashed_password == "new-password"
  assert user.auth_version == 4
  assert [role.name for role in user.roles] == ["用户", "运维管理员"]
  assert user.roles_version == 3


def test_topology_layout_validation_keeps_minio_rules_out_of_payload():
  layout = {
    "_meta": {
      "initialized": True,
      "schema_version": sync_module.TOPOLOGY_LAYOUT_SCHEMA_VERSION,
      "authority_region": "beijing",
    },
    "nodes": {
      "system-test": {
        "beijing": {"position_x": 10, "position_y": 20},
      },
    },
    "edges": {
      "system-test": {
        "beijing": {
          "tianjin": {
            "from_position": "right",
            "to_position": "left",
          },
        },
      },
    },
  }

  nodes, edges = sync_module._validated_topology_records(layout)
  assert nodes[("system-test", "beijing")] == {
    "position_x": 10,
    "position_y": 20,
  }
  assert edges[("system-test", "beijing", "tianjin")] == {
    "from_position": "right",
    "to_position": "left",
  }
  serialized = json.dumps(layout)
  assert "rule_id" not in serialized
  assert "replication" not in serialized


@pytest.mark.asyncio
async def test_non_authority_never_bootstraps_topology(monkeypatch):
  monkeypatch.setattr(sync_module.settings, "REGION", "shenzhen")
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")

  async def should_not_read(*_args, **_kwargs):
    pytest.fail("non-authority region must not initialize topology")

  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", should_not_read)
  assert await sync_module.bootstrap_topology_layout() is False


@pytest.mark.asyncio
async def test_topology_node_publish_uses_shared_cas(monkeypatch):
  captured = {}

  async def merge(key, mutator, client=None):
    layout = {
      "_meta": {
        "initialized": True,
        "schema_version": sync_module.TOPOLOGY_LAYOUT_SCHEMA_VERSION,
        "authority_region": "beijing",
      },
      "nodes": {},
      "edges": {},
    }
    captured["key"] = key
    captured["layout"] = mutator(layout)
    return captured["layout"]

  monkeypatch.setattr(etcd_op, "merge_update_etcd_key", merge)
  monkeypatch.setattr(sync_module.settings, "REGION", "hangzhou")
  await sync_module.publish_topology_node_position(
    "system-test",
    "hangzhou",
    120,
    -20,
  )

  assert captured["key"] == sync_module.ETCD_KEY_TOPOLOGY_LAYOUT
  entry = captured["layout"]["nodes"]["system-test"]["hangzhou"]
  assert entry["position_x"] == 120
  assert entry["position_y"] == -20
  assert entry["origin_region"] == "hangzhou"


@pytest.mark.asyncio
async def test_watch_dispatches_identity_and_topology_keys(monkeypatch):
  calls = []

  async def roles(data):
    calls.append(("roles", data))

  async def users(data):
    calls.append(("users", data))

  async def topology(data):
    calls.append(("topology", data))

  monkeypatch.setattr(sync_module, "sync_roles_to_mongo", roles)
  monkeypatch.setattr(sync_module, "sync_users_to_mongo", users)
  monkeypatch.setattr(sync_module, "sync_topology_layout_to_mongo", topology)

  await etcd_op._handle_etcd_put("/storagent/roles", '{"user": {}}')
  await etcd_op._handle_etcd_put("/storagent/users", '{"alice": {}}')
  await etcd_op._handle_etcd_put(
    "/storagent/topology_layout",
    '{"_meta": {"initialized": true}}',
  )

  assert calls == [
    ("roles", {"user": {}}),
    ("users", {"alice": {}}),
    ("topology", {"_meta": {"initialized": True}}),
  ]


@pytest.mark.asyncio
async def test_authority_backfills_legacy_application_quota(monkeypatch):
  current = {
    "legacy": {"enabled": True, "shown_name": "Legacy"},
    "invalid": {"enabled": True, "quota_bytes": 0},
    "configured": {"enabled": True, "quota_bytes": 123},
  }
  captured = {}

  async def pull(key, client=None):
    assert key == sync_module.ETCD_KEY_APPLICATIONS
    return current

  async def merge(key, mutator, client=None):
    assert key == sync_module.ETCD_KEY_APPLICATIONS
    captured.update(mutator(current))
    return captured

  monkeypatch.setattr(sync_module.settings, "REGION", "beijing")
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", pull)
  monkeypatch.setattr(etcd_op, "merge_update_etcd_key", merge)

  assert await sync_module.backfill_application_quotas(client=object()) == 2
  assert captured["legacy"]["quota_bytes"] == sync_module.DEFAULT_APPLICATION_QUOTA_BYTES
  assert captured["invalid"]["quota_bytes"] == sync_module.DEFAULT_APPLICATION_QUOTA_BYTES
  assert captured["configured"]["quota_bytes"] == 123


@pytest.mark.asyncio
async def test_non_authority_safely_backfills_deterministic_application_quota(monkeypatch):
  current = {"legacy": {"enabled": True}}
  captured = {}

  async def pull(*_args, **_kwargs):
    return current

  async def merge(_key, mutator, client=None):
    captured.update(mutator(current))
    return captured

  monkeypatch.setattr(sync_module.settings, "REGION", "shenzhen")
  monkeypatch.setattr(sync_module.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", pull)
  monkeypatch.setattr(etcd_op, "merge_update_etcd_key", merge)

  assert await sync_module.backfill_application_quotas(client=object()) == 1
  assert captured["legacy"]["quota_bytes"] == sync_module.DEFAULT_APPLICATION_QUOTA_BYTES


@pytest.mark.asyncio
async def test_graph_write_publishes_before_local_save(monkeypatch):
  calls = []

  async def publish(*args):
    calls.append(("publish", args))

  async def save(*args):
    calls.append(("save", args))
    return "saved"

  monkeypatch.setattr(sync_module, "publish_topology_edge_position", publish)
  monkeypatch.setattr(graph_service.graph_crud, "update_bucket_edge_position", save)

  result = await graph_service.set_bucket_edge_position(
    "system-test",
    "beijing",
    "tianjin",
    "right",
    "left",
  )
  assert result == "saved"
  assert [item[0] for item in calls] == ["publish", "save"]


@pytest.mark.asyncio
async def test_api_key_sync_preserves_existing_application_metadata(monkeypatch):
  app = SimpleNamespace(
    name="system-test",
    shown_name="测试系统调用",
    description="Storagent 测试使用的数据",
    enabled=True,
  )
  existing_key = SimpleNamespace(deleted=False)

  async def read_application(name):
    assert name == "system-test"
    return app

  async def read_api_key(key):
    assert key == "sk_test"
    return existing_key

  async def must_not_overwrite(*_args, **_kwargs):
    pytest.fail("API Key sync must not overwrite an existing application")

  monkeypatch.setattr(public_crud, "read_application_by_name", read_application)
  monkeypatch.setattr(
    public_crud,
    "read_api_key_by_key_including_deleted",
    read_api_key,
  )
  monkeypatch.setattr(
    sync_module,
    "upsert_application_from_etcd",
    must_not_overwrite,
  )

  result = await sync_module.upsert_api_key_from_etcd(
    "sk_test",
    {
      "app_name": "system-test",
      "expired_at": "2026-08-01T00:00:00+00:00",
    },
  )

  assert result is existing_key
  assert app.shown_name == "测试系统调用"
  assert app.description == "Storagent 测试使用的数据"
