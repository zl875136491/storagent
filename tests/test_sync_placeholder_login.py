import pytest

from src.core import auth
from src.core.exception import CustomException, ErrorDesc


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
async def test_password_login_rejects_unregistered_sync_placeholder(monkeypatch):
  placeholder = SyncPlaceholder()

  async def read_user(_username):
    return placeholder

  monkeypatch.setattr(auth.user_crud, "read_user_by_username", read_user)

  with pytest.raises(CustomException) as exc_info:
    await auth.authenticate_user("zhangle", "strong-password-1")

  assert exc_info.value.code == ErrorDesc.PASSWORD_UNSET.code
  assert placeholder.saved is False
  assert placeholder.is_sync is True
