"""不连接数据库的规则测试。换票和申请流程仍由虚拟存储集成测试覆盖。"""
import os

os.environ.setdefault("VS_MONGO_URL", "mongodb://127.0.0.1:27017")
os.environ.setdefault("VS_REGION", "unit")
os.environ.setdefault("VS_OAUTH_SECRET", "unit-oauth-secret")
os.environ.setdefault("VS_OAUTH_ISSUER", "https://virtual-storage.test")

from types import SimpleNamespace

import pytest

from src.virtual_storage.settings import reset_settings_cache

reset_settings_cache()

from src.virtual_storage.services.applications import validate_public_id
from src.virtual_storage.services.machine_auth import (
  OAuthError,
  _access_token,
  decode_access_token,
  parse_basic,
  reject_secret_in_form,
)
from src.virtual_storage.core.exception import InvalidCredentialError, VirtualStorageError


def test_public_id_accepts_apps_and_rejects_short_or_reserved():
  assert validate_public_id("Apps") == "apps"
  assert validate_public_id("team-1") == "team-1"
  with pytest.raises(VirtualStorageError):
    validate_public_id("app")
  with pytest.raises(VirtualStorageError):
    validate_public_id("api")
  with pytest.raises(VirtualStorageError):
    validate_public_id("-apps")


def test_secret_is_not_accepted_in_the_form():
  with pytest.raises(OAuthError) as caught:
    reject_secret_in_form({"client_secret": "x"})
  assert caught.value.error == "invalid_request"


def test_access_token_lasts_one_hour_and_rejects_a_raw_secret():
  account = SimpleNamespace(public_account_id="apps", app_name="apps", credential_version=1)
  token, expires = _access_token(account, "vs.account.manage")
  assert expires == 3600
  payload = decode_access_token(token)
  assert payload["sub"] == "apps"
  assert payload["typ"] == "vs_account_access"
  assert payload["credential_version"] == 1
  with pytest.raises(InvalidCredentialError):
    decode_access_token("mgmt-secret")


def test_basic_splits_client_id_and_secret():
  import base64
  encoded = base64.b64encode(b"apps:secret-value").decode()
  assert parse_basic(f"Basic {encoded}") == ("apps", "secret-value")
