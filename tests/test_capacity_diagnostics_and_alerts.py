"""Focused contract tests for quota alerts, capacity planning and diagnostics."""
from fastapi import FastAPI

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
