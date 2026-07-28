from types import SimpleNamespace

import pytest

from src.core import auth


class SyncPlaceholder:
  def __init__(self):
    self.username = "zhangle"
    self.name = "张乐"
    self.hashed_password = "unusable"
    self.roles = []
    self.permissions = []
    self.is_sync = True
    self.updated_at = None
    self.saved = False

  async def save(self):
    self.saved = True


@pytest.mark.asyncio
async def test_valid_directory_login_activates_preset_admin_placeholder(monkeypatch):
  placeholder = SyncPlaceholder()
  admin_role = SimpleNamespace(name="管理员")

  async def read_user(_username):
    return placeholder

  async def import_user(_username):
    return {"user_info": {"l": "张乐"}}

  async def get_admin_role():
    return admin_role

  async def get_permissions(roles):
    assert roles == [admin_role]
    return ["system_manage", "region_manage"]

  monkeypatch.setattr(auth.user_crud, "read_user_by_username", read_user)
  monkeypatch.setattr(auth, "import_user_from_springboard", import_user)
  monkeypatch.setattr(auth, "password_check", lambda _password: True)
  monkeypatch.setattr(auth, "get_password_hash", lambda _password: "activated-hash")
  monkeypatch.setattr(auth, "preset_admin_user", lambda _username: True)
  monkeypatch.setattr(auth.user_crud, "get_admin_role", get_admin_role)
  monkeypatch.setattr(auth.user_crud, "get_all_permissions", get_permissions)
  monkeypatch.setattr(
    auth,
    "verify_password",
    lambda _password, hashed_password: hashed_password == "activated-hash",
  )

  user = await auth.authenticate_user("zhangle", "strong-password-1")

  assert user is placeholder
  assert user.saved is True
  assert user.is_sync is False
  assert user.roles == [admin_role]
  assert user.permissions == ["system_manage", "region_manage"]
  assert user.hashed_password == "activated-hash"
  assert user.updated_at is not None
