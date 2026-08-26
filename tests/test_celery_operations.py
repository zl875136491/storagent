"""Contracts for the read-only Celery operations module."""
from datetime import timedelta

import pytest

from src.api import register_api
from src.modules.celery import service


def test_celery_routes_are_registered_for_both_versions():
  from fastapi import FastAPI

  app = FastAPI()
  register_api(app)
  paths = {route.path for route in app.routes}
  for prefix in ("/api/v1/celery", "/api/v2/celery"):
    assert prefix + "/overview" in paths
    assert prefix + "/history" in paths


def test_task_catalog_covers_all_worker_tasks():
  names = {item.name for item in service.task_catalog()}
  assert names == {
    "storagent.audit.persist",
    "storagent.auth.cleanup_expired_tokens",
    "storagent.capacity.snapshot",
    "storagent.etcd.execute",
    "storagent.etcd.reconcile",
    "storagent.files.archive_expired_objects",
    "storagent.maintenance.recover_queued_tasks",
    "storagent.public.refresh_quota_aggregates",
    "storagent.replication.reconcile_policies",
    "storagent.storage.execute_operation",
    "storagent.storage.monitor_cluster_health",
  }


def test_runtime_task_never_exposes_args_or_kwargs():
  task = service._task_from_runtime(
    {
      "id": "task-1",
      "name": "storagent.storage.execute_operation",
      "args": "['secret-value']",
      "kwargs": {"api_key": "secret-value"},
      "delivery_info": {"routing_key": "celery"},
    },
    worker="storagent-test@worker-1",
    status="STARTED",
  )
  assert task is not None
  assert task.id == "task-1"
  assert "secret-value" not in task.model_dump_json()


@pytest.mark.asyncio
async def test_overview_reports_disabled_celery_without_network(monkeypatch):
  monkeypatch.setattr(service.settings, "CELERY_ENABLED", False)
  result = await service.get_overview()
  assert result.broker.enabled is False
  assert result.broker.reachable is False
  assert result.workers == []


@pytest.mark.asyncio
async def test_overview_uses_short_cache_for_broker_inspection(monkeypatch):
  service.clear_overview_cache()
  calls = 0

  async def build():
    nonlocal calls
    calls += 1
    return service.schema.CeleryOverviewResponse(
      generated_at=service.utc_now(),
      broker=service.schema.CeleryBrokerStatus(enabled=True, reachable=True),
    )

  monkeypatch.setattr(service.settings, "CELERY_ENABLED", True)
  monkeypatch.setattr(service.settings, "CELERY_OVERVIEW_CACHE_SECONDS", 30)
  monkeypatch.setattr(service, "_build_overview", build)

  first = await service.get_overview()
  second = await service.get_overview()

  assert calls == 1
  assert first is second


def test_legacy_history_hides_unredacted_result_and_error_payloads():
  item = service._history_item({
    "task_id": "legacy-1",
    "task_name": "storagent.storage.execute_operation",
    "status": "FAILURE",
    "result": {"access_key": "secret-value"},
    "traceback": "RuntimeError: token=secret-value",
  })

  assert item.result_summary == "历史记录未暴露任务返回内容"
  assert item.error == "历史记录已隐藏未脱敏的失败详情"
  assert "secret-value" not in item.model_dump_json()


def test_queue_worker_count_is_region_queue_specific(monkeypatch):
  monkeypatch.setattr(service.settings, "REGION", "beijing")
  monkeypatch.setattr(service.settings, "CELERY_TASK_QUEUE_PREFIX", "storagent")
  monkeypatch.setattr(service.settings, "CELERY_TASK_PROTOCOL_VERSION", 2)
  workers = [
    service.schema.CeleryWorkerStatus(
      name="beijing@one", status="online", queue="storagent.beijing.v2",
    ),
    service.schema.CeleryWorkerStatus(
      name="tianjin@one", status="online", queue="storagent.tianjin.v2",
    ),
  ]

  queues = service._queue_statuses([], [], [], workers)

  assert [(item.name, item.worker_count) for item in queues] == [
    ("storagent.beijing.v2", 1),
  ]


def test_worker_heartbeat_view_hides_legacy_and_replaced_instances(monkeypatch):
  monkeypatch.setattr(service.settings, "CELERY_TASK_QUEUE_PREFIX", "storagent")
  monkeypatch.setattr(service.settings, "CELERY_WORKER_STALE_AFTER_SECONDS", 90)
  now = service.utc_now()
  rows = [
    {
      "worker": "legacy@host",
      "region": "beijing",
      "status": "offline",
      "last_seen": now,
    },
    {
      "worker": "beijing@old-host",
      "region": "beijing",
      "queue": "storagent.beijing.v2",
      "task_protocol": "2",
      "status": "online",
      "last_seen": now - timedelta(seconds=91),
    },
    {
      "worker": "beijing@current-host",
      "region": "beijing",
      "queue": "storagent.beijing.v2",
      "task_protocol": "2",
      "status": "online",
      "last_seen": now,
    },
    {
      "worker": "tianjin@last-host",
      "region": "tianjin",
      "queue": "storagent.tianjin.v2",
      "task_protocol": "2",
      "status": "online",
      "last_seen": now - timedelta(seconds=91),
    },
  ]

  compatible = service._compatible_heartbeat_rows(
    rows,
    online_workers={"beijing@current-host"},
  )

  assert [row["worker"] for row in compatible] == [
    "beijing@current-host",
    "tianjin@last-host",
  ]
