"""/ready 在数据库或 Etcd 未就绪时返回 HTTP 503。"""
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from src.modules.health.route import router


def _app() -> FastAPI:
  app = FastAPI()
  app.include_router(router)
  return app


def test_ready_503_when_db_not_initialized():
  with patch("src.core.database.get_motor_client", return_value=None):
    client = TestClient(_app())
    resp = client.get("/ready")
  assert resp.status_code == 503
  body = resp.json()
  assert body["status"] == "not_ready"
  assert body["reason"] == "database not initialized"


def test_ready_503_when_etcd_fails():
  mock_client = MagicMock()
  mock_client.admin.command = AsyncMock(return_value={"ok": 1})
  mock_etcd = MagicMock()
  mock_etcd.status = AsyncMock(side_effect=RuntimeError("etcd down"))
  mock_etcd.close = AsyncMock()
  with patch("src.core.database.get_motor_client", return_value=mock_client), patch(
    "src.core.etcd_op.get_etcd_client", new=AsyncMock(return_value=mock_etcd)
  ):
    client = TestClient(_app())
    resp = client.get("/ready")
  assert resp.status_code == 503
  assert "etcd" in resp.json()["reason"]


def test_ready_ok_when_ping_succeeds():
  mock_client = MagicMock()
  mock_client.admin.command = AsyncMock(return_value={"ok": 1})
  mock_etcd = MagicMock()
  mock_etcd.status = AsyncMock(return_value=MagicMock())
  mock_etcd.close = AsyncMock()
  with patch("src.core.database.get_motor_client", return_value=mock_client), patch(
    "src.core.etcd_op.get_etcd_client", new=AsyncMock(return_value=mock_etcd)
  ):
    client = TestClient(_app())
    resp = client.get("/ready")
  assert resp.status_code == 200
  assert resp.json()["status"] == "ready"


def test_ready_retries_after_invalid_auth_token():
  mock_client = MagicMock()
  mock_client.admin.command = AsyncMock(return_value={"ok": 1})
  stale = MagicMock()
  stale.status = AsyncMock(side_effect=RuntimeError("etcdserver: invalid auth token"))
  stale.close = AsyncMock()
  fresh = MagicMock()
  fresh.status = AsyncMock(return_value=MagicMock())
  fresh.close = AsyncMock()
  refresh = AsyncMock()
  with patch("src.core.database.get_motor_client", return_value=mock_client), patch(
    "src.core.etcd_op.get_etcd_client", new=AsyncMock(side_effect=[stale, fresh])
  ), patch("src.core.etcd_op.refresh_shared_etcd_client", refresh):
    client = TestClient(_app())
    resp = client.get("/ready")
  assert resp.status_code == 200
  assert resp.json()["status"] == "ready"
  refresh.assert_awaited()


def test_health_always_ok():
  client = TestClient(_app())
  resp = client.get("/health")
  assert resp.status_code == 200
  assert resp.json()["status"] == "ok"
