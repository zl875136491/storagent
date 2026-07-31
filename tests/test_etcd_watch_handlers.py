"""Etcd watch PUT/DELETE 分支可导入且应用停用逻辑存在。"""
import inspect
import json

import pytest

from src.core import etcd_op
from src.core.sync import upsert_application_from_etcd


def test_watch_handlers_exist():
  assert callable(etcd_op._handle_etcd_put)
  assert callable(etcd_op._handle_etcd_delete)
  src = inspect.getsource(etcd_op.watch_etcd_task)
  assert 'kind == "DELETE"' in src


def test_upsert_application_source_supports_disable():
  src = inspect.getsource(upsert_application_from_etcd)
  assert "elif not enabled and app_obj.enabled" in src


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
