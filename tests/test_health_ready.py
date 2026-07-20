"""/ready 在数据库未就绪时返回 HTTP 503。"""
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


def test_ready_ok_when_ping_succeeds():
  mock_client = MagicMock()
  mock_client.admin.command = AsyncMock(return_value={"ok": 1})
  with patch("src.core.database.get_motor_client", return_value=mock_client):
    client = TestClient(_app())
    resp = client.get("/ready")
  assert resp.status_code == 200
  assert resp.json()["status"] == "ready"


def test_health_always_ok():
  client = TestClient(_app())
  resp = client.get("/health")
  assert resp.status_code == 200
  assert resp.json()["status"] == "ok"
