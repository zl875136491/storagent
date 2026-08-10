"""稳定业务错误码契约（前端依赖 code 字段）。"""
from fastapi import FastAPI

from src.api import register_api
from src.core.exception import (
  CustomException,
  ErrorDesc,
  _v2_http_error,
  error_response,
  v2_error_response,
)


def test_object_not_found_local_code_and_http_status():
  payload = {
    "bucket": "app",
    "object_key": "a.bin",
    "current_region": "cn-east",
    "available_at": [{"download_url": "https://example/download"}],
  }
  exc = CustomException(ErrorDesc.OBJECT_NOT_FOUND_LOCAL, payload)
  assert exc.code == 404032
  assert exc.status_code == 404
  assert exc.message == "对象在本节点不存在"
  body = error_response(msg=exc.message, data=exc.reason, code=exc.code)
  assert body == {
    "msg": "对象在本节点不存在",
    "data": payload,
    "code": 404032,
  }


def test_object_not_found_cluster_wide():
  exc = CustomException(ErrorDesc.OBJECT_NOT_FOUND, {"available_at": []})
  assert exc.code == 404033
  assert exc.status_code == 404


def test_refresh_token_invalid_uses_402():
  exc = CustomException(ErrorDesc.REFRESH_TOKEN_NOT_VALID)
  assert exc.code == 402008
  assert exc.status_code == 402


def test_ai_upstream_failure_uses_502():
  exc = CustomException(ErrorDesc.AI_UPSTREAM_FAILED, "timeout")
  assert exc.code == 502043
  assert exc.status_code == 502


def test_v2_error_response_uses_stable_string_code_and_request_id():
  exc = CustomException(ErrorDesc.OBJECT_DELETED, {"object_id": "obj_1"})
  assert v2_error_response(exc, "req_1") == {
    "error": {
      "code": "object.deleted",
      "message": "对象已删除",
      "retryable": False,
      "details": {"object_id": "obj_1"},
    },
    "request_id": "req_1",
  }


def test_v2_framework_auth_error_has_stable_code():
  assert _v2_http_error(403, "Not authenticated")[:3] == (
    "auth.api_key.invalid",
    "API-KEY 无效",
    False,
  )


def test_every_v1_relative_path_is_registered_for_v2():
  app = FastAPI()
  register_api(app)
  paths = {route.path for route in app.routes}
  v1 = {path[len("/api/v1"):] for path in paths if path.startswith("/api/v1/")}
  v2 = {path[len("/api/v2"):] for path in paths if path.startswith("/api/v2/")}
  assert v1 <= v2
  assert {
    "/files/objects",
    "/files/objects/{object_id}",
    "/files/objects/{object_id}/restore",
    "/files/objects/{object_id}/share",
  } <= v2
