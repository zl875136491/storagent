"""限流与 RATE_LIMITED 错误码。"""
from unittest.mock import MagicMock

import pytest

from src.core.exception import CustomException, ErrorDesc
from src.core.rate_limit import check_rate_limit, _hits


def test_rate_limited_code():
  exc = CustomException(ErrorDesc.RATE_LIMITED, "slow down")
  assert exc.code == 429041
  assert exc.status_code == 429


def test_check_rate_limit_trips():
  _hits.clear()
  key = "unit:test"
  for _ in range(3):
    check_rate_limit(key, limit=3, window_seconds=60.0)
  with pytest.raises(CustomException) as ei:
    check_rate_limit(key, limit=3, window_seconds=60.0)
  assert ei.value.code == 429041


def test_login_rate_limit_helper_uses_ip():
  from src.core.rate_limit import rate_limit_login
  _hits.clear()
  req = MagicMock()
  req.headers = {}
  req.client.host = "9.9.9.9"
  for _ in range(10):
    rate_limit_login(req)
  with pytest.raises(CustomException):
    rate_limit_login(req)
