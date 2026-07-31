from types import SimpleNamespace

import pytest
import requests
from pydantic import ValidationError

from src.core import auth
from src.core.exception import CustomException, ErrorDesc
from src.modules.auth import oa, schema
from src.modules.auth import service as auth_service


def test_password_pair_normalizes_itcode_and_requires_confirmation():
  payload = schema.PasswordPairRequest(
    username=" ZhangLe ",
    password="NCzl5000.",
    confirm_password="NCzl5000.",
  )
  assert payload.username == "zhangle"

  with pytest.raises(ValidationError):
    schema.PasswordPairRequest(
      username="zhangle",
      password="NCzl5000.",
      confirm_password="different1",
    )


def test_login_link_url_encodes_values(monkeypatch):
  monkeypatch.setattr(oa.settings, "FRONT_URL", "http://stor.1oa.com.cn/")
  link = oa.build_login_link("user.name", "a+b/c=")
  assert link == (
    "http://stor.1oa.com.cn/login_by_code?"
    "username=user.name&code=a%2Bb%2Fc%3D"
  )


@pytest.mark.asyncio
async def test_oa_delivery_timeout_keeps_link_valid(monkeypatch):
  def timeout(*_args, **_kwargs):
    raise requests.Timeout("response lost")

  monkeypatch.setattr(oa, "_send_once", timeout)
  monkeypatch.setattr(oa.settings, "OA_AUTH_SEND_RETRIES", 1)
  result = await oa.send_oa_auth_message("zhangle", "title", "content", "code")
  assert result.status == "unknown"
  assert result.accepted is True


@pytest.mark.asyncio
async def test_registration_requires_directory_identity_before_challenge(monkeypatch):
  captured = {}

  async def no_user(_username):
    return None

  async def directory(_username):
    return {"user_info": {"l": "张乐"}}

  async def request_challenge(**kwargs):
    captured.update(kwargs)
    return {"message": "sent", "expires_in_seconds": 900, "delivery_status": "sent"}

  monkeypatch.setattr(auth_service.user_crud, "read_user_by_username", no_user)
  monkeypatch.setattr(auth_service, "import_user_from_springboard", directory)
  monkeypatch.setattr(auth_service, "get_password_hash", lambda _value: "bcrypt-hash")
  monkeypatch.setattr(auth_service, "_request_oa_challenge", request_challenge)

  result = await auth_service.request_registration("zhangle", "NCzl5000.")

  assert result["delivery_status"] == "sent"
  assert captured["purpose"] == auth_service.AUTH_PURPOSE_REGISTER
  assert captured["password_hash"] == "bcrypt-hash"
  assert captured["display_name"] == "张乐"


@pytest.mark.asyncio
async def test_password_reset_increments_auth_version_and_returns_new_tokens(monkeypatch):
  class FakeUser:
    username = "zhangle"
    hashed_password = "old-hash"
    auth_version = 2
    is_sync = False
    updated_at = None

    async def save(self):
      return None

  user = FakeUser()
  challenge = SimpleNamespace(
    username="zhangle",
    purpose=auth_service.AUTH_PURPOSE_PASSWORD_RESET,
    password_hash="new-hash",
  )
  published = []

  async def consume(_username, _code):
    return challenge

  async def read_user(_username):
    return user

  async def publish(target):
    published.append(target)

  async def tokens(username, version):
    return {"access_token": username, "refresh_token": str(version), "token_type": "bearer"}

  monkeypatch.setattr(auth_service.user_crud, "consume_auth_challenge", consume)
  monkeypatch.setattr(auth_service.user_crud, "read_user_by_username", read_user)
  monkeypatch.setattr(auth_service, "_publish_user_best_effort", publish)
  monkeypatch.setattr(auth_service, "create_token", tokens)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)

  result = await auth_service.login_by_code("zhangle", "valid-code-value-123456")

  assert user.hashed_password == "new-hash"
  assert user.auth_version == 3
  assert published == [user]
  assert result["refresh_token"] == "3"


@pytest.mark.asyncio
async def test_access_token_auth_version_invalidates_old_session(monkeypatch):
  token = auth.create_access_token({"sub": "alice", "typ": "access", "ver": 1})

  async def valid(_token):
    return True

  async def read_user(_username):
    return SimpleNamespace(username="alice", auth_version=2)

  monkeypatch.setattr(auth.user_crud, "check_token_valid", valid)
  monkeypatch.setattr(auth.user_crud, "read_user_by_username", read_user)

  with pytest.raises(CustomException) as exc_info:
    await auth.get_current_user(token)
  assert exc_info.value.code == ErrorDesc.CREDENTIALS_NOT_VALID.code
