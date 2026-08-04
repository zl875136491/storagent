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


def test_one_time_download_rate_limit_caps_forged_forwarded_ips():
  from src.core.rate_limit import rate_limit_one_time_download

  _hits.clear()
  req = MagicMock()
  req.client.host = "9.9.9.9"
  for index in range(600):
    req.headers = {"x-forwarded-for": f"198.51.100.{index}"}
    rate_limit_one_time_download(req)

  req.headers = {"x-forwarded-for": "203.0.113.1"}
  with pytest.raises(CustomException) as exc_info:
    rate_limit_one_time_download(req)
  assert exc_info.value.code == ErrorDesc.RATE_LIMITED.code


def test_one_time_download_rate_limit_caps_single_claimed_ip():
  from src.core.rate_limit import rate_limit_one_time_download

  _hits.clear()
  req = MagicMock()
  req.client.host = "9.9.9.9"
  req.headers = {"x-forwarded-for": "198.51.100.10"}
  for _ in range(60):
    rate_limit_one_time_download(req)

  with pytest.raises(CustomException) as exc_info:
    rate_limit_one_time_download(req)
  assert exc_info.value.code == ErrorDesc.RATE_LIMITED.code
