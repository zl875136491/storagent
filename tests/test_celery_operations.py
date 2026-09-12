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
    assert prefix + "/tasks/run" in paths


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
    "storagent.storage.sync_file_inventory",
  }
  inventory = next(item for item in service.task_catalog() if item.name.endswith("sync_file_inventory"))
  assert inventory.manual_run_allowed is True
  assert inventory.schedule_seconds == 21600
  assert not any(item.manual_run_allowed for item in service.task_catalog() if item.name != inventory.name)


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
  assert task.display_name == "存储运维任务执行"
  assert "secret-value" not in task.model_dump_json()


def test_runtime_task_accepts_type_uuid_and_nested_request():
  typed = service._task_from_runtime(
    {
      "uuid": "task-2",
      "type": "storagent.etcd.reconcile",
      "delivery_info": {"routing_key": "storagent.beijing.v2"},
    },
    worker="storagent-test@worker-1",
    status="STARTED",
    region="beijing",
  )
  nested = service._task_from_runtime(
    {
      "acknowledged": True,
      "request": {
        "id": "task-3",
        "name": "storagent.storage.monitor_cluster_health",
      },
    },
    worker="storagent-test@worker-1",
    status="STARTED",
  )

  assert typed is not None
  assert typed.id == "task-2"
  assert typed.name == "storagent.etcd.reconcile"
  assert typed.display_name == "Etcd 全量校准"
  assert typed.region == "beijing"
  assert nested is not None
  assert nested.id == "task-3"
  assert nested.display_name == "MinIO 集群自愈巡检"


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
  assert item.display_name == "存储运维任务执行"
  assert "secret-value" not in item.model_dump_json()


def test_history_mongo_filter_failed_only():
  assert service._history_mongo_filter(failed_only=False) == {}
  assert service._history_mongo_filter(failed_only=True) == {"status": "FAILURE"}


@pytest.mark.asyncio
async def test_history_disabled_includes_pagination_fields(monkeypatch):
  monkeypatch.setattr(service.settings, "CELERY_ENABLED", False)
  result = await service.get_history(limit=50, offset=100, failed_only=True)
  assert result.available is False
  assert result.total == 0
  assert result.limit == 50
  assert result.offset == 100


def test_merge_in_progress_history_fills_empty_inspect(monkeypatch):
  monkeypatch.setattr(service.settings, "CELERY_WORKER_STALE_AFTER_SECONDS", 90)
  now = service.utc_now()
  workers = [
    service.schema.CeleryWorkerStatus(name="w1", status="online", active_count=0),
    service.schema.CeleryWorkerStatus(name="w2", status="offline", active_count=0),
  ]
  rows = [
    {
      "task_id": "running-1",
      "task_name": "storagent.etcd.reconcile",
      "status": "STARTED",
      "worker": "w1",
      "updated_at": now,
    },
    {
      "task_id": "stale-1",
      "task_name": "storagent.etcd.reconcile",
      "status": "STARTED",
      "worker": "w2",
      "updated_at": now - timedelta(hours=2),
    },
  ]

  workers, active = service._merge_in_progress_history(workers, [], [], rows)

  assert [item.id for item in active] == ["running-1"]
  assert active[0].display_name == "Etcd 全量校准"
  assert workers[0].active_count == 1
  assert workers[1].active_count == 0


def test_merge_in_progress_history_does_not_duplicate_inspect_ids():
  existing = service.schema.CeleryTaskExecution(
    id="running-1",
    name="storagent.etcd.reconcile",
    display_name="Etcd 全量校准",
    status="STARTED",
    worker="w1",
  )
  workers = [
    service.schema.CeleryWorkerStatus(name="w1", status="online", active_count=1),
  ]
  rows = [
    {
      "task_id": "running-1",
      "task_name": "storagent.etcd.reconcile",
      "status": "STARTED",
      "worker": "w1",
      "updated_at": service.utc_now(),
    },
  ]

  workers, active = service._merge_in_progress_history(workers, [existing], [], rows)

  assert [item.id for item in active] == ["running-1"]
  assert workers[0].active_count == 1


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


@pytest.mark.asyncio
async def test_run_registered_task_rejects_unknown_names():
  from src.core.exception import CustomException, ErrorDesc

  with pytest.raises(CustomException) as raised:
    await service.run_registered_task("storagent.etcd.reconcile", "zhangle")
  assert raised.value.error_desc == ErrorDesc.OPERATION_NOT_ALLOWED


@pytest.mark.asyncio
async def test_run_registered_task_rejects_when_lease_held(monkeypatch):
  from src.core.exception import CustomException, ErrorDesc
  from src.modules.storage import inventory_sync

  monkeypatch.setattr(service.settings, "CELERY_ENABLED", True)

  async def held():
    return True

  async def idle(_name):
    return False

  monkeypatch.setattr(inventory_sync, "is_lease_held", held)
  monkeypatch.setattr(service, "_task_in_progress", idle)

  with pytest.raises(CustomException) as raised:
    await service.run_registered_task("storagent.storage.sync_file_inventory", "zhangle")
  assert raised.value.error_desc == ErrorDesc.TASK_ALREADY_RUNNING


@pytest.mark.asyncio
async def test_run_registered_task_dispatches_manual_inventory(monkeypatch):
  from src.modules.storage import inventory_sync

  monkeypatch.setattr(service.settings, "CELERY_ENABLED", True)

  async def free():
    return False

  async def idle(_name):
    return False

  monkeypatch.setattr(inventory_sync, "is_lease_held", free)
  monkeypatch.setattr(service, "_task_in_progress", idle)
  monkeypatch.setattr(inventory_sync, "enqueue_file_inventory_sync", lambda **_kwargs: "task-manual-1")
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  result = await service.run_registered_task("storagent.storage.sync_file_inventory", "zhangle")
  assert result.task_id == "task-manual-1"
  assert result.status == "queued"
  assert result.display_name == "文件索引同步"
