"""管理密钥交付状态与重签修订号。只使用临时账号，不读取或改动 apps。"""
import uuid
from datetime import timedelta

from src.modules.virtual_console.bridge import _revision_match
from src.utils.helpers import utc_now
from src.virtual_storage.models.documents import KeyDelivery, VirtualAccount, VirtualAccountApplication, VirtualAccountGrant
from src.virtual_storage.services.legacy_migration import migrate_legacy_account
from src.virtual_storage.services.support import new_id

SECRET_FIELDS = {
  "encrypted_key",
  "management_credential_hash",
  "management_credential_hint",
  "client_secret",
  "subject_hash",
  "access_token",
  "refresh_token",
}


def _headers(user_id: str, username: str, *, reviewer: bool = False, admin: bool = False) -> dict:
  return {
    "X-Console-Token": "console-test-token",
    "X-Console-User-Id": user_id,
    "X-Console-Username": username,
    "X-Console-Admin": "1" if admin else "0",
    "X-Console-Reviewer": "1" if reviewer or admin else "0",
  }


def _call(client, func, *args, **kwargs):
  async def runner():
    return await func(*args, **kwargs)
  return client.portal.call(runner)


def _no_secrets(body: dict) -> None:
  assert SECRET_FIELDS.isdisjoint(body)
  delivery = body.get("client_secret_delivery")
  assert delivery in {None, "open", "missing", "expired"}


def _cleanup(client, public_id: str, account_id: str) -> None:
  async def runner():
    await VirtualAccountApplication.find(VirtualAccountApplication.public_account_id == public_id).delete()
    await VirtualAccountGrant.find(VirtualAccountGrant.account_id == account_id).delete()
    await KeyDelivery.find(KeyDelivery.account_id == account_id).delete()
    account = await VirtualAccount.find_one(VirtualAccount.account_id == account_id)
    if account is not None:
      await account.delete()
  _call(client, runner)


def test_revision_header_keeps_zero_and_omits_missing_value():
  assert _revision_match({"revision": 0}) == {"If-Match": "0"}
  assert _revision_match({"revision": 186}) == {"If-Match": "186"}
  assert _revision_match({}) == {}
  assert _revision_match({"revision": None}) == {}
  assert _revision_match(None) == {}


def test_migrated_account_has_no_management_delivery(client, monkeypatch):
  monkeypatch.setenv("VS_CONSOLE_TOKEN", "console-test-token")
  from src.virtual_storage.settings import reset_settings_cache
  reset_settings_cache()
  public_id = "fix" + uuid.uuid4().hex[:8]
  owner = "owner-" + uuid.uuid4().hex[:8]
  stranger = "other-" + uuid.uuid4().hex[:8]
  created = client.post("/internal/drive", json={
    "op": "account",
    "app_name": public_id,
    "display_name": "交付夹具",
    "credential": "fixture-secret-" + uuid.uuid4().hex,
    "quota_bytes": 1000,
    "quota_objects": 4,
  })
  assert created.status_code == 200, created.text
  account_id = created.json()["account_id"]
  account = _call(client, VirtualAccount.find_one, VirtualAccount.account_id == account_id)
  original_hash = account.management_credential_hash
  try:
    _call(client, KeyDelivery(
      delivery_id=new_id("dlv"),
      account_id=account_id,
      tenant_id="tn_fixture_" + uuid.uuid4().hex[:8],
      key_id="key_" + uuid.uuid4().hex[:8],
      subject_hash="tenant-marker",
      idempotency_key="tenant-only-" + uuid.uuid4().hex[:8],
      request_fingerprint="fixture",
      encrypted_key="tenant-key-ciphertext",
      window_starts_at=utc_now(),
      window_ends_at=utc_now() + timedelta(minutes=10),
      created_at=utc_now(),
    ).insert)
    written = _call(
      client,
      migrate_legacy_account,
      public_id=public_id,
      owner_user_id=owner,
      owner_username=owner,
      dry_run=False,
    )
    assert written["dry_run"] is False
    listed = client.get("/internal/console/applications?mine=1", headers=_headers(owner, owner))
    assert listed.status_code == 200, listed.text
    item = next(row for row in listed.json()["items"] if row["public_account_id"] == public_id)
    assert item["init_status"] == "active"
    assert item["client_secret_delivery"] == "missing"
    _no_secrets(item)
    session = client.get("/internal/console/session", headers=_headers(owner, owner))
    assert session.status_code == 200, session.text
    matched = [
      row for row in session.json()["accounts"]
      if row.get("public_account_id") == public_id or row.get("account_id") == account_id
    ]
    assert len(matched) == 1
    assert matched[0]["revision"] == account.revision
    assert "management_credential_hash" not in matched[0]
    assert "client_secret" not in matched[0]
    hidden = client.get("/internal/console/applications?mine=1", headers=_headers(stranger, stranger))
    assert all(row["public_account_id"] != public_id for row in hidden.json()["items"])
    denied = client.post(
      f"/internal/console/accounts/{public_id}/secret-delivery",
      headers=_headers(stranger, stranger),
    )
    assert denied.status_code == 403
    denied_rotate = client.post(
      f"/internal/console/accounts/{public_id}/rotate-secret",
      headers={**_headers(stranger, stranger), "If-Match": "0"},
    )
    assert denied_rotate.status_code == 403
    claimed = client.post(
      f"/internal/console/accounts/{public_id}/secret-delivery",
      headers=_headers(owner, owner),
    )
    assert claimed.status_code == 409
    assert "重新生成密钥" in claimed.json()["message"]
    assert "client_secret" not in claimed.json()
    saved = _call(client, VirtualAccount.find_one, VirtualAccount.account_id == account_id)
    assert saved.management_credential_hash == original_hash
    assert saved.credential_version == 1
  finally:
    _cleanup(client, public_id, account_id)


def test_open_window_expires_and_rotate_checks_revision(client, monkeypatch):
  monkeypatch.setenv("VS_CONSOLE_TOKEN", "console-test-token")
  from src.virtual_storage.settings import reset_settings_cache
  reset_settings_cache()
  owner = "owner-" + uuid.uuid4().hex[:8]
  stranger = "other-" + uuid.uuid4().hex[:8]
  public_id = "win" + uuid.uuid4().hex[:8]
  created = client.post(
    "/internal/console/applications",
    headers={**_headers(owner, owner), "Idempotency-Key": "idem-" + uuid.uuid4().hex},
    json={
      "public_account_id": public_id,
      "purpose": "窗口夹具",
      "requested_quota_bytes": 1000,
      "requested_quota_objects": 4,
    },
  )
  assert created.status_code == 201, created.text
  application_id = created.json()["application_id"]
  account_id = ""
  try:
    approved = client.post(
      f"/internal/console/applications/{application_id}/approve",
      headers={**_headers(owner, owner, reviewer=True), "If-Match": str(created.json()["revision"])},
      json={"quota_bytes": 1000, "quota_objects": 4},
    )
    assert approved.status_code == 200, approved.text
    advanced = client.post(
      f"/internal/console/applications/{application_id}/advance",
      headers=_headers(owner, owner),
    )
    assert advanced.status_code == 200, advanced.text
    assert advanced.json()["client_secret_delivery"] == "open"
    _no_secrets(advanced.json())
    account_id = advanced.json()["account_id"]
    delivered = client.post(
      f"/internal/console/accounts/{public_id}/secret-delivery",
      headers=_headers(owner, owner),
    )
    assert delivered.status_code == 200, delivered.text
    assert delivered.json()["client_secret"]
    assert delivered.json()["credential_version"] == 1
    again = client.post(
      f"/internal/console/accounts/{public_id}/secret-delivery",
      headers=_headers(owner, owner),
    )
    assert again.status_code == 200
    assert again.json()["client_secret"] == delivered.json()["client_secret"]
    foreign = client.post(
      f"/internal/console/accounts/{public_id}/secret-delivery",
      headers=_headers(stranger, stranger),
    )
    assert foreign.status_code == 403

    async def expire():
      row = await KeyDelivery.find_one({
        "account_id": account_id,
        "key_id": "client-secret",
        "idempotency_key": "v1",
      })
      row.window_ends_at = utc_now() - timedelta(seconds=5)
      await row.save()
    _call(client, expire)
    listed = client.get(f"/internal/console/applications/{application_id}", headers=_headers(owner, owner))
    assert listed.json()["client_secret_delivery"] == "expired"
    _no_secrets(listed.json())
    ended = client.post(
      f"/internal/console/accounts/{public_id}/secret-delivery",
      headers=_headers(owner, owner),
    )
    assert ended.status_code == 409
    assert "已结束" in ended.json()["message"]
    before = _call(client, VirtualAccount.find_one, VirtualAccount.account_id == account_id)
    conflict = client.post(
      f"/internal/console/accounts/{public_id}/rotate-secret",
      headers={**_headers(owner, owner), "If-Match": "186"},
    )
    assert conflict.status_code == 412
    untouched = _call(client, VirtualAccount.find_one, VirtualAccount.account_id == account_id)
    assert untouched.management_credential_hash == before.management_credential_hash
    assert untouched.credential_version == before.credential_version
    assert untouched.revision == before.revision
    missing = client.post(
      f"/internal/console/accounts/{public_id}/rotate-secret",
      headers=_headers(owner, owner),
    )
    assert missing.status_code == 412
    rotated = client.post(
      f"/internal/console/accounts/{public_id}/rotate-secret",
      headers={**_headers(owner, owner), "If-Match": str(before.revision)},
    )
    assert rotated.status_code == 200, rotated.text
    assert rotated.json()["credential_version"] == before.credential_version + 1
    assert rotated.json()["revision"] == before.revision + 1
    assert rotated.json()["client_secret"]
    assert rotated.json()["client_secret"] != delivered.json()["client_secret"]
    refreshed = client.get(f"/internal/console/applications/{application_id}", headers=_headers(owner, owner))
    assert refreshed.json()["client_secret_delivery"] == "open"
    _no_secrets(refreshed.json())
    session = client.get("/internal/console/session", headers=_headers(owner, owner))
    matched = next(row for row in session.json()["accounts"] if row.get("account_id") == account_id)
    assert matched["revision"] == rotated.json()["revision"]
    assert "client_secret" not in matched
  finally:
    if account_id:
      _cleanup(client, public_id, account_id)
    else:
      _call(client, VirtualAccountApplication.find(VirtualAccountApplication.public_account_id == public_id).delete)
