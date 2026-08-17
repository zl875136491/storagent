"""Read-only Etcd operations status contract tests."""
import pytest

from src.api import register_api
from src.modules.etcd import service


def test_etcd_routes_are_registered_for_both_versions():
  from fastapi import FastAPI
  app = FastAPI()
  register_api(app)
  paths = {route.path for route in app.routes}
  assert "/api/v1/storage/operations/etcd" in paths
  assert "/api/v2/storage/operations/etcd" in paths


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
