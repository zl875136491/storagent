"""引导账号迁入申请。使用独立的临时账号，不碰真实 apps。"""
import uuid
from pathlib import Path

from tests.virtual_storage.test_virtual_storage import _name
from src.virtual_storage.models.documents import VirtualAccount, VirtualAccountApplication, VirtualAccountGrant
from src.virtual_storage.services.legacy_migration import MigrationStopped, migrate_legacy_account


def _headers(user_id: str, username: str, *, reviewer: bool = False) -> dict:
  return {
    "X-Console-Token": "console-test-token",
    "X-Console-User-Id": user_id,
    "X-Console-Username": username,
    "X-Console-Admin": "0",
    "X-Console-Reviewer": "1" if reviewer else "0",
  }


def test_startup_does_not_bootstrap_an_account():
  app_source = Path("src/virtual_storage/app.py").read_text()
  settings_source = Path("src/virtual_storage/settings.py").read_text()
  assert "create_account" not in app_source
  assert "VS_BOOTSTRAP" not in settings_source


def _call(client, func, *args, **kwargs):
  async def runner():
    return await func(*args, **kwargs)
  return client.portal.call(runner)


def test_migrate_requires_real_owner_and_preserves_account(client, monkeypatch):
  monkeypatch.setenv("VS_CONSOLE_TOKEN", "console-test-token")
  from src.virtual_storage.settings import reset_settings_cache
  reset_settings_cache()
  public_id = "mig" + uuid.uuid4().hex[:8]
  secret = "fixture-secret-" + uuid.uuid4().hex
  created = client.post("/internal/drive", json={
    "op": "account",
    "app_name": public_id,
    "display_name": "迁移夹具",
    "credential": secret,
    "quota_bytes": 1000,
    "quota_objects": 4,
  })
  assert created.status_code == 200, created.text
  account_id = created.json()["account_id"]
  account = _call(client, VirtualAccount.find_one, VirtualAccount.account_id == account_id)
  assert account is not None
  original_hash = account.management_credential_hash
  original_revision = account.revision

  try:
    _call(client, migrate_legacy_account, public_id=public_id)
  except MigrationStopped as exc:
    assert "归属" in str(exc)
  else:
    raise AssertionError("缺少归属时必须停止")

  try:
    _call(client, migrate_legacy_account,
      public_id=public_id,
      owner_user_id="u-test",
      owner_username="vsreg-admin",
      dry_run=False,
    )
  except MigrationStopped as exc:
    assert "测试账号" in str(exc)
  else:
    raise AssertionError("测试账号不能成为归属")

  owner = "owner-" + uuid.uuid4().hex[:8]
  preview = _call(client, migrate_legacy_account,
    public_id=public_id,
    owner_user_id=owner,
    owner_username=owner,
    dry_run=True,
  )
  assert preview["would_write"] is True
  assert preview["account_id"] == account_id
  assert "management_credential_hash" not in preview
  still = _call(client, VirtualAccountApplication.find_one, VirtualAccountApplication.public_account_id == public_id)
  assert still is None

  written = _call(client, migrate_legacy_account,
    public_id=public_id,
    owner_user_id=owner,
    owner_username=owner,
    dry_run=False,
  )
  assert written["dry_run"] is False
  again = _call(client, migrate_legacy_account,
    public_id=public_id,
    owner_user_id=owner,
    owner_username=owner,
    dry_run=False,
  )
  assert again["would_write"] is False
  saved = _call(client, VirtualAccount.find_one, VirtualAccount.account_id == account_id)
  assert saved.account_id == account_id
  assert saved.management_credential_hash == original_hash
  assert saved.revision == original_revision
  assert saved.public_account_id == public_id
  grants = _call(client, VirtualAccountGrant.find(VirtualAccountGrant.account_id == account_id).count)
  assert grants == 1
  _call(client, VirtualAccountGrant.find(VirtualAccountGrant.account_id == account_id).delete)
  stranger = "other-" + uuid.uuid4().hex[:8]
  denied = client.post(
    "/internal/console/applications",
    headers={**_headers(stranger, stranger), "Idempotency-Key": _name("idem")},
    json={"public_account_id": public_id, "purpose": "重复", "requested_quota_bytes": 1, "requested_quota_objects": 1},
  )
  assert denied.status_code == 409
  mine = client.get("/internal/console/applications?mine=1", headers=_headers(owner, owner))
  assert mine.status_code == 200
  assert any(item["public_account_id"] == public_id for item in mine.json()["items"])
  hidden = client.get("/internal/console/applications?mine=1", headers=_headers(stranger, stranger))
  assert all(item["public_account_id"] != public_id for item in hidden.json()["items"])
  queue = client.get("/internal/console/applications?mine=0", headers=_headers(owner, owner, reviewer=True))
  assert all(item["public_account_id"] != public_id for item in queue.json()["items"])
  from src.virtual_storage.models.documents import AuditRecord
  _call(client, VirtualAccountApplication.find(VirtualAccountApplication.public_account_id == public_id).delete)
  _call(client, AuditRecord.find(AuditRecord.account_id == account_id).delete)
  _call(client, VirtualAccount.find(VirtualAccount.account_id == account_id).delete)
