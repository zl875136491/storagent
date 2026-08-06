"""数据面能力令牌：签发、校验、以及 resolve_data_plane_context 的鉴权分支。"""
import time
from datetime import timedelta
from types import SimpleNamespace

import pytest

from src.core import capability_token
from src.core.auth import resolve_data_plane_context
from src.core.exception import CustomException, ErrorDesc
from src.modules.public.model import Application
from src.utils.helpers import utc_now


API_KEY = "sk-unit-test-shared-secret"


def test_issue_and_verify_round_trip():
  token = capability_token.issue_capability_token(
    API_KEY,
    action="download",
    object_key="a/b.bin",
    expires_in_seconds=60,
  )
  payload = capability_token.verify_capability_token(
    token, API_KEY, action="download", object_key="a/b.bin",
  )
  assert payload["act"] == "download"
  assert payload["key"] == "a/b.bin"
  assert payload["ref"] == capability_token.api_key_ref(API_KEY)


def test_issue_binds_upload_id_for_upload_part():
  token = capability_token.issue_capability_token(
    API_KEY,
    action="upload_part",
    object_key="obj-1",
    expires_in_seconds=60,
    upload_id="upload-1",
  )
  payload = capability_token.verify_capability_token(
    token, API_KEY, action="upload_part", object_key="obj-1", upload_id="upload-1",
  )
  assert payload["uid"] == "upload-1"


def test_verify_rejects_wrong_secret():
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="a", expires_in_seconds=60,
  )
  with pytest.raises(CustomException) as exc_info:
    capability_token.verify_capability_token(
      token, "wrong-secret", action="download", object_key="a",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_INVALID.code


def test_verify_rejects_tampered_object_key():
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="a", expires_in_seconds=60,
  )
  with pytest.raises(CustomException) as exc_info:
    capability_token.verify_capability_token(
      token, API_KEY, action="download", object_key="b",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH.code


def test_verify_rejects_wrong_action():
  token = capability_token.issue_capability_token(
    API_KEY, action="upload_part", object_key="a", expires_in_seconds=60, upload_id="u1",
  )
  with pytest.raises(CustomException) as exc_info:
    capability_token.verify_capability_token(
      token, API_KEY, action="download", object_key="a",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH.code


def test_verify_rejects_mismatched_upload_id():
  token = capability_token.issue_capability_token(
    API_KEY, action="upload_part", object_key="a", expires_in_seconds=60, upload_id="u1",
  )
  with pytest.raises(CustomException) as exc_info:
    capability_token.verify_capability_token(
      token, API_KEY, action="upload_part", object_key="a", upload_id="u2",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH.code


def test_verify_rejects_expired_token(monkeypatch):
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="a", expires_in_seconds=1,
  )
  real_time = time.time
  monkeypatch.setattr(time, "time", lambda: real_time() + 10)
  with pytest.raises(CustomException) as exc_info:
    capability_token.verify_capability_token(
      token, API_KEY, action="download", object_key="a",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_EXPIRED.code


def test_verify_rejects_malformed_token():
  with pytest.raises(CustomException) as exc_info:
    capability_token.verify_capability_token(
      "not-a-valid-token", API_KEY, action="download", object_key="a",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_INVALID.code


def test_peek_ref_matches_issued_token():
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="a", expires_in_seconds=60,
  )
  assert capability_token.peek_capability_token_ref(token) == capability_token.api_key_ref(API_KEY)


def test_peek_ref_rejects_garbage_input():
  with pytest.raises(CustomException) as exc_info:
    capability_token.peek_capability_token_ref("garbage")
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_INVALID.code


def _fake_api_key_obj(*, key_enc: str, application_enabled: bool = True):
  application = Application.model_construct(
    name="app-under-test",
    shown_name="应用",
    enabled=application_enabled,
  )
  return SimpleNamespace(
    key="hash-value",
    key_hint="sk-****",
    key_enc=key_enc,
    deleted=False,
    expired_at=utc_now() + timedelta(days=1),
    application=application,
  )


@pytest.mark.asyncio
async def test_resolve_data_plane_context_prefers_api_key_header(monkeypatch):
  async def read_by_key(_plain_key):
    return _fake_api_key_obj(key_enc="ignored")

  monkeypatch.setattr("src.modules.public.crud.read_api_key_by_key", read_by_key)

  context = await resolve_data_plane_context(
    api_key="the-real-api-key",
    token="should-be-ignored",
    action="download",
    object_key="obj",
  )
  assert context["app_name"] == "app-under-test"
  assert context["auth_mode"] == "api_key"


@pytest.mark.asyncio
async def test_resolve_data_plane_context_accepts_valid_token(monkeypatch):
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="obj-42", expires_in_seconds=60,
  )

  async def read_by_hash(key_hash):
    assert key_hash == capability_token.api_key_ref(API_KEY)
    return _fake_api_key_obj(key_enc="enc-blob")

  monkeypatch.setattr("src.modules.public.crud.read_api_key_by_hash", read_by_hash)
  monkeypatch.setattr("src.core.auth.decrypt_secret", lambda _value: API_KEY)

  context = await resolve_data_plane_context(
    api_key=None,
    token=token,
    action="download",
    object_key="obj-42",
  )
  assert context["app_name"] == "app-under-test"
  assert context["auth_mode"] == "token"


@pytest.mark.asyncio
async def test_resolve_data_plane_context_rejects_token_for_different_object(monkeypatch):
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="obj-42", expires_in_seconds=60,
  )

  async def read_by_hash(_key_hash):
    return _fake_api_key_obj(key_enc="enc-blob")

  monkeypatch.setattr("src.modules.public.crud.read_api_key_by_hash", read_by_hash)
  monkeypatch.setattr("src.core.auth.decrypt_secret", lambda _value: API_KEY)

  with pytest.raises(CustomException) as exc_info:
    await resolve_data_plane_context(
      api_key=None,
      token=token,
      action="download",
      object_key="someone-elses-object",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH.code


@pytest.mark.asyncio
async def test_resolve_data_plane_context_requires_credential():
  with pytest.raises(CustomException) as exc_info:
    await resolve_data_plane_context(
      api_key=None,
      token=None,
      action="download",
      object_key="obj",
    )
  assert exc_info.value.code == ErrorDesc.API_KEY_INVALID.code


@pytest.mark.asyncio
async def test_resolve_data_plane_context_rejects_unknown_token_key(monkeypatch):
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="obj", expires_in_seconds=60,
  )

  async def read_by_hash(_key_hash):
    return None

  monkeypatch.setattr("src.modules.public.crud.read_api_key_by_hash", read_by_hash)

  with pytest.raises(CustomException) as exc_info:
    await resolve_data_plane_context(
      api_key=None,
      token=token,
      action="download",
      object_key="obj",
    )
  assert exc_info.value.code == ErrorDesc.CAPABILITY_TOKEN_INVALID.code


@pytest.mark.asyncio
async def test_resolve_data_plane_context_rejects_disabled_application(monkeypatch):
  token = capability_token.issue_capability_token(
    API_KEY, action="download", object_key="obj", expires_in_seconds=60,
  )

  async def read_by_hash(_key_hash):
    return _fake_api_key_obj(key_enc="enc-blob", application_enabled=False)

  monkeypatch.setattr("src.modules.public.crud.read_api_key_by_hash", read_by_hash)
  monkeypatch.setattr("src.core.auth.decrypt_secret", lambda _value: API_KEY)

  with pytest.raises(CustomException) as exc_info:
    await resolve_data_plane_context(
      api_key=None,
      token=token,
      action="download",
      object_key="obj",
    )
  assert exc_info.value.code == ErrorDesc.APP_NOT_ENABLED.code
