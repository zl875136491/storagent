"""Read-only Etcd operations status contract tests."""
from types import SimpleNamespace

import pytest

from src.api import register_api
from src.configs.configs import DEFAULT_ETCD_ENDPOINTS
from src.modules.etcd import service


def test_etcd_routes_are_registered_for_both_versions():
  from fastapi import FastAPI
  app = FastAPI()
  register_api(app)
  paths = {route.path for route in app.routes}
  assert "/api/v1/storage/operations/etcd" in paths
  assert "/api/v2/storage/operations/etcd" in paths

  maintenance_suffixes = (
    "/trend",
    "/keyspace",
    "/revision-options",
    "/events",
    "/tasks",
    "/tasks/{task_id}",
    "/compact",
    "/defrag",
    "/alarm-disarm",
    "/snapshot",
    "/restore",
  )
  for prefix in ("/api/v1/storage/operations/etcd", "/api/v2/storage/operations/etcd"):
    for suffix in maintenance_suffixes:
      assert f"{prefix}{suffix}" in paths


@pytest.mark.asyncio
async def test_prefix_stats_use_count_only_range(monkeypatch):
  captured = {}

  class Request:
    count_only = False

  class Stub:
    async def Range(self, request, timeout=None, metadata=None):
      captured["count_only"] = request.count_only
      captured["timeout"] = timeout
      return SimpleNamespace(header=SimpleNamespace(revision=42), count=7)

  class Client:
    kvstub = Stub()
    _timeout = 3
    metadata = (("token", "x"),)

    @staticmethod
    def _build_get_range_request(key, range_end=None):
      captured["key"] = key
      captured["range_end"] = range_end
      return Request()

    async def get(self, _key):
      raise AssertionError("single-key get is only a fallback")

    async def get_range(self, *_args, **_kwargs):
      raise AssertionError("full prefix get_range must not be used")

  revision, count = await service._storagent_prefix_stats(Client())
  assert revision == 42
  assert count == 7
  assert captured["count_only"] is True
  assert captured["key"] == b"/storagent/"
  assert captured["range_end"] == b"/storagent/\xff"


@pytest.mark.asyncio
async def test_check_endpoint_stays_reachable_when_revision_read_fails(monkeypatch):
  class Status:
    version = "3.6.11"
    db_size = 1024
    leader = SimpleNamespace(id="1")
    raft_index = 9
    raft_term = 2
    member_id = "1"

  class Client:
    async def status(self):
      return Status()

    async def members(self):
      if False:
        yield None

    async def list_alarms(self):
      if False:
        yield None

    async def close(self):
      return None

  async def boom(_client):
    raise RuntimeError("Received message larger than max (6470000 vs. 4194304)")

  monkeypatch.setattr(service, "_make_client", lambda host, port: Client())
  monkeypatch.setattr(service, "_store_revision", boom)
  async def no_rss(_host):
    return 0
  monkeypatch.setattr(service, "_read_process_rss_bytes", no_rss)
  result = await service._check_endpoint("etcd-2", "10.32.129.241", 2379)
  assert result.reachable is True
  assert result.status == "healthy"
  assert result.revision == 0


def test_message_too_large_is_not_reported_as_unreachable():
  error = RuntimeError("Received message larger than max (6470000 vs. 4194304)")
  assert service._endpoint_check_failure_reason(error) == "Etcd 响应超过 gRPC 消息上限"


def test_alarm_type_one_is_nospace():
  assert service._alarm_is_nospace(1) is True
  assert service._alarm_is_nospace("1") is True
  assert service._alarm_is_nospace("AlarmType.NOSPACE") is True
  assert service._alarm_is_nospace("CORRUPT") is False


def test_blank_endpoint_setting_uses_complete_default_cluster(monkeypatch):
  monkeypatch.setattr(service.settings, "ETCD_ENDPOINTS", "")
  endpoints = service._endpoint_list()
  assert len(endpoints) == len(DEFAULT_ETCD_ENDPOINTS) == 5
  assert [f"http://{host}:{port}" for _, host, port in endpoints] == list(DEFAULT_ETCD_ENDPOINTS)


@pytest.mark.asyncio
async def test_status_reports_quorum_and_watch_state(monkeypatch):
  service.clear_cache()
  monkeypatch.setattr(service.settings, "ETCD_ENDPOINTS", "http://etcd-a:2379,http://etcd-b:2379,http://etcd-c:2379")

  async def check(name, host, port):
    return service.schema.EtcdEndpointStatus(
      name=name,
      endpoint=f"http://{host}:{port}",
      status="healthy",
      reachable=True,
      is_leader=name == "etcd-1",
      leader_id="1",
      member_id="1" if name == "etcd-1" else name,
      version="3.5.0",
    )

  monkeypatch.setattr(service, "_check_endpoint", check)
  monkeypatch.setattr(service.metrics, "snapshot", lambda: {"counters": {}, "gauges": {}})
  result = await service.get_status(force_refresh=True)
  assert result.status == "healthy"
  assert result.quorum is True
  assert result.configured_endpoint_count == 3
  assert result.reachable_endpoint_count == 3
  assert result.sync.watch_status == "healthy"


@pytest.mark.asyncio
async def test_status_flags_quota_warning_and_nospace(monkeypatch):
  service.clear_cache()
  monkeypatch.setattr(service.settings, "ETCD_ENDPOINTS", "http://etcd-a:2379")
  monkeypatch.setattr(service.settings, "ETCD_QUOTA_BACKEND_BYTES", 1000)
  monkeypatch.setattr(service.settings, "ETCD_QUOTA_WARNING_RATIO", 0.8)
  monkeypatch.setattr(service.settings, "ETCD_QUOTA_CRITICAL_RATIO", 0.9)
  monkeypatch.setattr(service.metrics, "snapshot", lambda: {"counters": {}, "gauges": {}})

  async def warning_member(name, host, port):
    return service.schema.EtcdEndpointStatus(
      name=name,
      endpoint=f"http://{host}:{port}",
      status="healthy",
      reachable=True,
      is_leader=True,
      leader_id="1",
      member_id="1",
      db_size_bytes=850,
    )

  monkeypatch.setattr(service, "_check_endpoint", warning_member)
  warned = await service.get_status(force_refresh=True)
  assert warned.status == "warning"
  assert warned.quota_used_ratio == 0.85
  assert any(alert.code == "etcd_quota_warning" for alert in warned.alerts)

  service.clear_cache()

  async def nospace_member(name, host, port):
    return service.schema.EtcdEndpointStatus(
      name=name,
      endpoint=f"http://{host}:{port}",
      status="healthy",
      reachable=True,
      is_leader=True,
      leader_id="1",
      member_id="1",
      db_size_bytes=100,
      alarms=["AlarmType.NOSPACE"],
    )

  monkeypatch.setattr(service, "_check_endpoint", nospace_member)
  critical = await service.get_status(force_refresh=True)
  assert critical.status == "critical"
  assert any(alert.code == "etcd_nospace" for alert in critical.alerts)
  assert critical.members[0].nospace is True


@pytest.mark.asyncio
async def test_status_marks_quorum_loss_critical(monkeypatch):
  service.clear_cache()
  monkeypatch.setattr(service.settings, "ETCD_ENDPOINTS", "http://etcd-a:2379,http://etcd-b:2379,http://etcd-c:2379")

  async def check(name, host, port):
    return service.schema.EtcdEndpointStatus(
      name=name,
      endpoint=f"http://{host}:{port}",
      status="critical",
      reachable=name == "etcd-1",
      leader_id="",
      error="mock unavailable" if name != "etcd-1" else "",
    )

  monkeypatch.setattr(service, "_check_endpoint", check)
  monkeypatch.setattr(service.metrics, "snapshot", lambda: {"counters": {}, "gauges": {}})
  result = await service.get_status(force_refresh=True)
  assert result.status == "critical"
  assert result.quorum is False
  assert "quorum" in " ".join(result.reasons)


@pytest.mark.asyncio
async def test_etcd_worker_claim_is_atomic_before_side_effect(monkeypatch):
  task = SimpleNamespace(
    id="etcd-task-1",
    kind="keyspace",
    status="queued",
    origin_region="beijing",
    result={},
    error="",
    message="任务已排队",
    started_at=None,
    finished_at=None,
    save_calls=0,
  )
  calls = {}

  async def save():
    task.save_calls += 1

  task.save = save

  class Collection:
    async def find_one_and_update(self, query, update, *, return_document):
      calls["query"] = query
      calls["update"] = update
      task.status = "running"
      return {"claimed": True}

  async def read_task(task_id):
    assert task_id == "etcd-task-1"
    return task

  async def keyspace(actor):
    assert actor == "admin"
    return service.schema.EtcdOperationResponse(
      kind="keyspace",
      status="succeeded",
      message="ok",
      detail={"key_count": 1},
      created_at=service.utc_now(),
    )

  monkeypatch.setattr(service.settings, "REGION", "beijing")
  monkeypatch.setattr(service.EtcdOperationTask, "get", read_task)
  monkeypatch.setattr(service.EtcdOperationTask, "get_motor_collection", lambda: Collection())
  monkeypatch.setattr(service.EtcdOperationTask, "model_validate", lambda _raw: task)
  monkeypatch.setattr(service, "keyspace", keyspace)

  result = await service._execute_task("etcd-task-1", "admin", None, origin_region="beijing")

  assert calls["query"] == {
    "_id": "etcd-task-1",
    "status": "queued",
    "origin_region": {"$in": ["", "beijing"]},
  }
  assert calls["update"]["$set"]["status"] == "running"
  assert result == {"status": "succeeded", "task_id": "etcd-task-1"}
  assert task.status == "succeeded"


@pytest.mark.asyncio
async def test_etcd_worker_rejects_foreign_region_before_execution(monkeypatch):
  task = SimpleNamespace(
    id="etcd-task-foreign",
    kind="defrag",
    status="queued",
    origin_region="tianjin",
    result={},
    error="",
    message="",
    finished_at=None,
  )

  async def save():
    return None

  async def read_task(_task_id):
    return task

  task.save = save
  monkeypatch.setattr(service.settings, "REGION", "beijing")
  monkeypatch.setattr(service.EtcdOperationTask, "get", read_task)

  with pytest.raises(service.EtcdOperationRegionMismatchError):
    await service._execute_task(
      "etcd-task-foreign",
      "admin",
      None,
      origin_region="tianjin",
    )

  assert task.status == "failed"
  assert task.result["recovery_required"] is True
