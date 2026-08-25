"""Contracts for the read-only Celery operations module."""
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
