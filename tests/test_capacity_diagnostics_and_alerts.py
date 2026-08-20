"""Focused contract tests for quota alerts, capacity planning and diagnostics."""
from fastapi import FastAPI
import pytest

from src.api import register_api
from src.modules.diagnostics import service as diagnostics_service
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
