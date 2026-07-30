"""Cross-region identity and topology control-plane synchronization."""
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.core import etcd_op
from src.core import sync as sync_module
from src.modules.graph import service as graph_service


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


def test_user_entry_encrypts_password_hash_and_uses_role_names(monkeypatch):
  monkeypatch.setattr(sync_module.settings, "SECRET_KEY", "unit-test-secret-key")
  monkeypatch.setattr(sync_module.settings, "REGION", "shenzhen")
  now = datetime(2026, 7, 30, tzinfo=timezone.utc)
  user = SimpleNamespace(
    name="Alice",
    hashed_password="$2b$04$hash",
    roles=[SimpleNamespace(name="user"), SimpleNamespace(name="admin")],
    created_at=now,
    updated_at=now,
  )

  entry = sync_module.user_to_etcd_entry(user)
  assert entry["hashed_password_enc"].startswith("enc:v1:")
  assert "$2b$04$hash" not in entry["hashed_password_enc"]
  assert entry["role_names"] == ["admin", "user"]
  assert entry["origin_region"] == "shenzhen"


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
