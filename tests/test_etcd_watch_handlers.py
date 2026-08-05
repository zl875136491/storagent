"""Etcd watch PUT/DELETE 分支与应用投影行为。"""
import inspect
import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.core import etcd_op
from src.core import sync as sync_module


def test_watch_handlers_exist():
  assert callable(etcd_op._handle_etcd_put)
  assert callable(etcd_op._handle_etcd_delete)
  src = inspect.getsource(etcd_op.watch_etcd_task)
  assert 'kind == "DELETE"' in src


def test_watch_ignores_runtime_lock_and_quota_keys():
  assert etcd_op._is_runtime_etcd_key("/storagent/locks/quota/application/demo")
  assert etcd_op._is_runtime_etcd_key("/storagent/quota/apps/demo")
  assert etcd_op._is_runtime_etcd_key("/storagent/quota/uploads/demo/object")
  assert not etcd_op._is_runtime_etcd_key("/storagent/applications")


@pytest.mark.asyncio
async def test_upsert_application_supports_disable(monkeypatch):
  author = SimpleNamespace(id="author-id")

  class Application:
    shown_name = "Demo"
    description = ""
    enabled = True
    enabled_at = datetime(2026, 8, 5, tzinfo=timezone.utc)
    provisioning_status = "ready"
    provisioning_error = ""
    provisioning_updated_at = None
    quota_bytes = 100
    approver = None

    def __init__(self):
      self.author = author
      self.saved = 0

    async def save(self):
      self.saved += 1

  application = Application()

  async def read_application(_name):
    return application

  async def read_user(_username, _name=""):
    return author

  monkeypatch.setattr(
    "src.modules.public.crud.read_application_by_name",
    read_application,
  )
  monkeypatch.setattr(sync_module, "get_or_create_sync_user", read_user)

  result, changed = await sync_module.upsert_application_from_etcd(
    "demo",
    {
      "shown_name": "Demo",
      "enabled": False,
      "author_username": "owner",
      "quota_bytes": 100,
    },
  )

  assert result is application
  assert changed is True
  assert application.enabled is False
  assert application.enabled_at is None
  assert application.saved == 1


def test_unpublish_helpers_exist():
  from src.core import sync as sync_module
  assert callable(sync_module.unpublish_region)
  assert callable(sync_module.unpublish_server)


def test_ai_config_sync_helpers_exist():
  from src.core import sync as sync_module
  assert sync_module.ETCD_KEY_AI_CONFIG == "ai_config"
  assert callable(sync_module.publish_ai_config)
  assert callable(sync_module.sync_ai_config_to_mongo)


@pytest.mark.asyncio
async def test_server_watch_syncs_inventory_without_enabling_site_replication(monkeypatch):
  calls = []

  async def sync_servers(data):
    calls.append(("sync", data))
    return ["beijing"]

  async def setup_aliases(data):
    calls.append(("aliases", data))

  monkeypatch.setattr(etcd_op.sync_module, "sync_servers_to_mongo", sync_servers)
  monkeypatch.setattr(etcd_op.sync_module, "setup_mc_aliases", setup_aliases)

  payload = {"beijing": {"host": "10.32.129.241"}}
  await etcd_op._handle_etcd_put(
    f"{etcd_op.ETCD_PREFIX}{etcd_op.sync_module.ETCD_KEY_SERVERS}",
    json.dumps(payload),
  )

  assert calls == [("sync", payload), ("aliases", payload)]
  assert not hasattr(etcd_op.sync_module, "join_site_replication_for_new_servers")
