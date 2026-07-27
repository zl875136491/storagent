"""Etcd watch PUT/DELETE 分支可导入且应用停用逻辑存在。"""
import inspect

from src.core import etcd_op
from src.core.sync import upsert_application_from_etcd


def test_watch_handlers_exist():
  assert callable(etcd_op._handle_etcd_put)
  assert callable(etcd_op._handle_etcd_delete)
  src = inspect.getsource(etcd_op.watch_etcd_task)
  assert 'kind == "DELETE"' in src


def test_upsert_application_source_supports_disable():
  src = inspect.getsource(upsert_application_from_etcd)
  assert 'data.get("enabled") is False' in src


def test_unpublish_helpers_exist():
  from src.core import sync as sync_module
  assert callable(sync_module.unpublish_region)
  assert callable(sync_module.unpublish_server)


def test_ai_config_sync_helpers_exist():
  from src.core import sync as sync_module
  assert sync_module.ETCD_KEY_AI_CONFIG == "ai_config"
  assert callable(sync_module.publish_ai_config)
  assert callable(sync_module.sync_ai_config_to_mongo)
