"""metrics / audit / SYNC_FAILED 契约。"""
from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.core import metrics as metrics_mod
from src.core.audit import audit
from src.core.exception import CustomException, ErrorDesc
from src.core.middleware import RequestContextMiddleware
from src.modules.health.route import router


def test_sync_failed_http_status():
  exc = CustomException(ErrorDesc.SYNC_FAILED, "etcd down")
  assert exc.code == 503040
  assert exc.status_code == 503


def test_metrics_incr_and_prometheus():
  metrics_mod.incr("test_counter_total", 2)
  text = metrics_mod.render_prometheus("unit")
  assert "storagent_test_counter_total" in text
  assert 'region="unit"' in text
  snap = metrics_mod.snapshot()
  assert snap["counters"]["test_counter_total"] >= 2


def test_audit_increments_counter():
  before = metrics_mod.snapshot()["counters"].get("audit_events_total", 0)
  audit("unit.test", actor="tester", resource="x")
  after = metrics_mod.snapshot()["counters"].get("audit_events_total", 0)
  assert after >= before + 1


def test_metrics_endpoint_and_request_id():
  app = FastAPI()
  app.add_middleware(RequestContextMiddleware)
  app.include_router(router)
  client = TestClient(app)
  resp = client.get("/metrics")
  assert resp.status_code == 200
  assert "storagent_uptime_seconds" in resp.text
  assert "X-Request-Id" in resp.headers

  resp_json = client.get("/metrics?format=json")
  assert resp_json.status_code == 200
  body = resp_json.json()
  assert "counters" in body
  assert body["region"]
