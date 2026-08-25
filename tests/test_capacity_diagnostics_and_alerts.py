"""Focused contract tests for quota alerts, capacity planning and diagnostics."""
from types import SimpleNamespace

from fastapi import FastAPI
import pytest

from src.api import register_api
from src.modules.diagnostics import service as diagnostics_service
from src.modules.diagnostics import route as diagnostics_route
from src.modules.public import quota_alert


def test_new_operational_routes_are_registered_for_both_versions():
  app = FastAPI()
  register_api(app)
  paths = {route.path for route in app.routes}
  expected = {
    "/public/quota-alert-rule",
    "/public/application/{application_id}/expansion-requests",
    "/diagnostics/{version}/self-diagnosis",
    "/diagnostics/{version}/probe",
    "/diagnostics/{version}/storage-probe",
    "/diagnostics/{version}/report",
    "/capacity",
  }
  for prefix in ("/api/v1", "/api/v2"):
    assert {prefix + path for path in expected} <= paths


def test_diagnostic_script_is_versioned_and_keeps_key_out_of_report():
  script = diagnostics_service.render_script("v2")
  assert 'API_PREFIX="/api/v2"' in script
  assert '/diagnostics/${API_VERSION}/probe' in script
  assert "APIKey to the diagnostic report" in script
  assert "APIKey 与 ${API_VERSION} 契约验证通过" in script
  assert "应用 APPID" not in script
  assert "APP_NAME=" not in script
  assert "?app_name=" not in script
  assert "backend_port" not in script
  assert "应用后端服务端口" not in script


def test_diagnostic_script_normalizes_region_shorthand_before_dns_and_curl():
  script = diagnostics_service.render_script("v1")
  assert "\"\"|local" in script
  assert "${GATEWAY_ORIGIN}/server/${value}" in script
  assert "normalize_base_url" in script
  assert "基础地址无效" in script
  assert "read -r answer </dev/tty" in script
  assert "curl ... | sh" in script
  assert 'answer=""' in script
  assert "无法读取基础地址" in script
  assert "无法读取 APIKey" in script
  assert "IFS= read -r answer || true" not in script
  assert "ask_secret()" in script
  assert "stty -echo" in script
  assert "APIKey（输入时不显示）" in script
  assert "API_KEY=\"$(trim \"$(ask" not in script


@pytest.mark.asyncio
async def test_diagnostic_probe_uses_api_key_bound_application_context():
  result = await diagnostics_route.probe("v2", _context={"app_name": "parts"})

  assert result == {
    "authenticated": True,
    "api_version": "v2",
  }


@pytest.mark.asyncio
async def test_storage_probe_decrypts_server_credentials_before_minio_access(monkeypatch):
  from src.core import minio_op
  from src.modules.storage import crud as storage_crud

  server = SimpleNamespace(
    host="minio.internal",
    minio_port=9000,
    access_key="enc:v1:encrypted-access-key",
    secret_key="enc:v1:encrypted-secret-key",
    master=True,
  )

  class Response:
    def __init__(self, value):
      self.value = value

    def read(self):
      return self.value

    def close(self):
      return None

    def release_conn(self):
      return None

  class Client:
    written = b""
    removed = []

    def put_object(self, _bucket, _key, data, _length):
      self.written = data.read()

    def get_object(self, _bucket, _key):
      return Response(self.written)

    def remove_object(self, bucket, key):
      self.removed.append((bucket, key))

  client = Client()
  captured_credentials = []

  async def read_servers():
    return [server]

  def plain_credentials(item):
    assert item is server
    return "plain-access-key", "plain-secret-key"

  def get_client(host, port, access_key, secret_key):
    captured_credentials.append((host, port, access_key, secret_key))
    return client

  monkeypatch.setattr(storage_crud, "read_minio_server_list", read_servers)
  monkeypatch.setattr(storage_crud, "plain_minio_credentials", plain_credentials)
  monkeypatch.setattr(minio_op, "get_minio_client", get_client)

  result = await diagnostics_service.storage_probe({"app_name": "parts"}, "diag-1")

  assert result == {"storage": "passed", "object_prefix": ".storagent-diagnostics/"}
  assert captured_credentials == [
    ("minio.internal", 9000, "plain-access-key", "plain-secret-key"),
  ]
  assert client.removed == [("parts", ".storagent-diagnostics/diag-1.txt")]


@pytest.mark.asyncio
async def test_storage_probe_preserves_primary_error_when_cleanup_also_fails(monkeypatch):
  from src.core import minio_op
  from src.core.exception import CustomException, ErrorDesc
  from src.modules.storage import crud as storage_crud

  class CodedError(RuntimeError):
    def __init__(self, code):
      self.code = code
      super().__init__(code)

  server = SimpleNamespace(
    host="minio.internal", minio_port=9000,
    access_key="enc:v1:access", secret_key="enc:v1:secret", master=True,
  )

  class Client:
    cleanup_calls = 0

    def put_object(self, *_args):
      return None

    def get_object(self, *_args):
      raise CodedError("InvalidAccessKeyId")

    def remove_object(self, *_args):
      self.cleanup_calls += 1
      raise TimeoutError("cleanup timed out")

  client = Client()

  async def read_servers():
    return [server]

  monkeypatch.setattr(storage_crud, "read_minio_server_list", read_servers)
  monkeypatch.setattr(storage_crud, "plain_minio_credentials", lambda _item: ("access", "secret"))
  monkeypatch.setattr(minio_op, "get_minio_client", lambda *_args: client)

  with pytest.raises(CustomException) as error:
    await diagnostics_service.storage_probe({"app_name": "parts"}, "diag-2")

  assert error.value.error_desc == ErrorDesc.MINIO_AUTH_FAILED
  assert error.value.reason == {
    "operation": "read_write",
    "category": "authentication",
    "source_code": "InvalidAccessKeyId",
  }
  assert client.cleanup_calls == 1


@pytest.mark.asyncio
async def test_storage_probe_does_not_cleanup_when_upload_fails(monkeypatch):
  from src.core import minio_op
  from src.core.exception import CustomException, ErrorDesc
  from src.modules.storage import crud as storage_crud

  class CodedError(RuntimeError):
    def __init__(self, code):
      self.code = code
      super().__init__(code)

  server = SimpleNamespace(
    host="minio.internal", minio_port=9000,
    access_key="enc:v1:access", secret_key="enc:v1:secret", master=True,
  )

  class Client:
    cleanup_calls = 0

    def put_object(self, *_args):
      raise CodedError("InvalidAccessKeyId")

    def remove_object(self, *_args):
      self.cleanup_calls += 1

  client = Client()

  async def read_servers():
    return [server]

  monkeypatch.setattr(storage_crud, "read_minio_server_list", read_servers)
  monkeypatch.setattr(storage_crud, "plain_minio_credentials", lambda _item: ("access", "secret"))
  monkeypatch.setattr(minio_op, "get_minio_client", lambda *_args: client)

  with pytest.raises(CustomException) as error:
    await diagnostics_service.storage_probe({"app_name": "parts"}, "diag-3")

  assert error.value.error_desc == ErrorDesc.MINIO_AUTH_FAILED
  assert client.cleanup_calls == 0


def test_storage_probe_error_categories_are_actionable():
  from src.core.exception import ErrorDesc, v2_error_response

  class CodedError(RuntimeError):
    def __init__(self, code):
      self.code = code
      super().__init__(code)

  authentication = diagnostics_service._storage_probe_error(
    CodedError("InvalidAccessKeyId"), "write",
  )
  network = diagnostics_service._storage_probe_error(TimeoutError("slow"), "cleanup")

  assert authentication.error_desc == ErrorDesc.MINIO_AUTH_FAILED
  assert v2_error_response(authentication, "req-auth")["error"] == {
    "code": "storage.authentication_failed",
    "message": "Minio 认证或授权失败",
    "retryable": False,
    "details": {
      "operation": "write",
      "category": "authentication",
      "source_code": "InvalidAccessKeyId",
    },
  }
  assert network.error_desc == ErrorDesc.MINIO_NETWORK_UNAVAILABLE
  assert v2_error_response(network, "req-network")["error"]["retryable"] is True


@pytest.mark.asyncio
async def test_collect_snapshot_skips_non_authority(monkeypatch):
  from src.modules.capacity import service

  async def should_not_run():
    raise AssertionError("non-authority must not collect capacity snapshots")

  monkeypatch.setattr(service.settings, "REGION", "shanghai")
  monkeypatch.setattr(service.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(service.operations, "get_cluster_health_overview", should_not_run)
  await service.collect_snapshot()


@pytest.mark.asyncio
async def test_get_planning_reads_etcd_on_non_authority(monkeypatch):
  from src.modules.capacity import service

  published = {
    "generated_at": "2026-08-20T08:00:00+00:00",
    "authority_region": "beijing",
    "data": [{
      "region": "beijing",
      "shown_name": "北京",
      "raw_capacity_bytes": 100,
      "raw_used_bytes": 40,
      "logical_usage_bytes": 20,
      "object_count": 3,
      "archive_bytes": 1,
      "archived_object_count": 1,
      "expected_replica_count": 4,
      "actual_replica_count": 4,
      "waterline_percent": 40.0,
      "daily_growth_bytes": 0,
      "risks": [],
      "trend": [],
    }],
  }
  pushed = {}

  async def pull(key, client=None):
    assert key == service.ETCD_KEY_CAPACITY_PLANNING
    return published

  async def should_not_push(*_args, **_kwargs):
    pushed["called"] = True
    raise AssertionError("non-authority must not overwrite shared planning")

  monkeypatch.setattr(service.settings, "REGION", "shanghai")
  monkeypatch.setattr(service.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr("src.core.etcd_op.pull_from_etcd_by_key", pull)
  monkeypatch.setattr("src.core.etcd_op.push_to_etcd", should_not_push)
  result = await service.get_planning()
  assert result["data"][0]["region"] == "beijing"
  assert "called" not in pushed


@pytest.mark.asyncio
async def test_get_planning_publishes_from_authority(monkeypatch):
  from src.modules.capacity import service

  captured = {}

  async def compute():
    return {"generated_at": "2026-08-20T08:00:00+00:00", "data": []}

  async def push(key, value, client=None):
    captured["key"] = key
    captured["value"] = value

  monkeypatch.setattr(service.settings, "REGION", "beijing")
  monkeypatch.setattr(service.settings, "SYNC_AUTHORITY_REGION", "beijing")
  monkeypatch.setattr(service, "_compute_planning", compute)
  monkeypatch.setattr("src.core.etcd_op.push_to_etcd", push)
  result = await service.get_planning()
  assert result["data"] == []
  assert captured["key"] == service.ETCD_KEY_CAPACITY_PLANNING
  assert captured["value"]["authority_region"] == "beijing"
  assert captured["value"]["data"] == []


def test_alert_level_uses_ordered_thresholds():
  class Rule:
    low_percent = 70
    medium_percent = 85
    high_percent = 90
    block_percent = 100

  assert quota_alert._level(Rule(), 0.69) is None
  assert quota_alert._level(Rule(), 0.70) == "low"
  assert quota_alert._level(Rule(), 0.85) == "medium"
  assert quota_alert._level(Rule(), 0.90) == "high"
  assert quota_alert._level(Rule(), 1.00) == "blocked"
