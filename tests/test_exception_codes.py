"""稳定业务错误码契约（前端依赖 code 字段）。"""
from src.core.exception import CustomException, ErrorDesc, error_response


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
