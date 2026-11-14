"""申请、审核和机器令牌。依赖虚拟存储集成测试的真实 Mongo。"""
import base64
import uuid

from tests.virtual_storage.test_virtual_storage import _name


def _basic(client_id: str, secret: str) -> dict:
  encoded = base64.b64encode(f"{client_id}:{secret}".encode()).decode()
  return {"Authorization": f"Basic {encoded}"}


def _headers(user_id: str, username: str, *, reviewer: bool = False, admin: bool = False) -> dict:
  return {
    "X-Console-Token": "console-test-token",
    "X-Console-User-Id": user_id,
    "X-Console-Username": username,
    "X-Console-Admin": "1" if admin else "0",
    "X-Console-Reviewer": "1" if reviewer or admin else "0",
  }


def test_application_conflict_approval_and_machine_token(client, monkeypatch):
  monkeypatch.setenv("VS_CONSOLE_TOKEN", "console-test-token")
  from src.virtual_storage.settings import reset_settings_cache
  reset_settings_cache()
  owner = "owner-" + uuid.uuid4().hex[:8]
  reviewer = "review-" + uuid.uuid4().hex[:8]
  public_id = "acct" + uuid.uuid4().hex[:8]
  created = client.post(
    "/internal/console/applications",
    headers={**_headers(owner, owner), "Idempotency-Key": _name("idem")},
    json={
      "public_account_id": public_id,
      "purpose": "账单归档",
      "requested_quota_bytes": 1000,
      "requested_quota_objects": 5,
    },
  )
  assert created.status_code == 201, created.text
  body = created.json()
  assert body["status"] == "submitted"
  again = client.post(
    "/internal/console/applications",
    headers=_headers(reviewer, reviewer),
    json={"public_account_id": public_id, "purpose": "别人", "requested_quota_bytes": 1, "requested_quota_objects": 1},
  )
  assert again.status_code == 409, again.text
  approved = client.post(
    f"/internal/console/applications/{body['application_id']}/approve",
    headers={**_headers(owner, owner, reviewer=True), "If-Match": str(body["revision"])},
    json={"quota_bytes": 1000, "quota_objects": 5},
  )
  assert approved.status_code == 200, approved.text
  assert approved.json()["status"] == "approved"
  assert approved.json()["init_status"] == "pending"
  advanced = client.post(
    f"/internal/console/applications/{body['application_id']}/advance",
    headers=_headers(owner, owner),
  )
  assert advanced.status_code == 200, advanced.text
  assert advanced.json()["init_status"] == "active"
  delivered = client.post(
    f"/internal/console/accounts/{public_id}/secret-delivery",
    headers=_headers(owner, owner),
  )
  assert delivered.status_code == 200, delivered.text
  secret = delivered.json()["client_secret"]
  denied = client.post(
    f"/internal/console/accounts/{public_id}/secret-delivery",
    headers=_headers(reviewer, reviewer, reviewer=True),
  )
  assert denied.status_code == 403
  form = client.post(
    "/oauth2/token",
    headers=_basic(public_id, secret),
    data={"grant_type": "client_credentials", "client_secret": secret},
  )
  assert form.status_code == 400
  assert form.json()["error"] == "invalid_request"
  token = client.post(
    "/oauth2/token",
    headers=_basic(public_id, secret),
    data={"grant_type": "client_credentials"},
  )
  assert token.status_code == 200, token.text
  payload = token.json()
  assert payload["expires_in"] == 3600
  assert payload["refresh_token"]
  raw = client.get(
    f"/api/virtual-storage/v1/accounts/{public_id}",
    headers={"Authorization": f"Bearer {secret}"},
  )
  assert raw.status_code == 401
  ok = client.get(
    f"/api/virtual-storage/v1/accounts/{public_id}",
    headers={"Authorization": f"Bearer {payload['access_token']}"},
  )
  assert ok.status_code == 200, ok.text
  foreign = client.get(
    "/api/virtual-storage/v1/accounts/other-account",
    headers={"Authorization": f"Bearer {payload['access_token']}"},
  )
  assert foreign.status_code == 403
  refreshed = client.post(
    "/oauth2/refresh",
    headers=_basic(public_id, secret),
    data={"grant_type": "refresh_token", "refresh_token": payload["refresh_token"]},
  )
  assert refreshed.status_code == 200, refreshed.text
  reused = client.post(
    "/oauth2/refresh",
    headers=_basic(public_id, secret),
    data={"grant_type": "refresh_token", "refresh_token": payload["refresh_token"]},
  )
  assert reused.status_code == 400
  withdrawn = client.post(
    "/internal/console/applications",
    headers=_headers(owner, owner),
    json={"public_account_id": "free" + uuid.uuid4().hex[:6], "purpose": "撤回", "requested_quota_bytes": 1, "requested_quota_objects": 1},
  )
  assert withdrawn.status_code == 201, withdrawn.text
  first = withdrawn.json()
  closed = client.post(
    f"/internal/console/applications/{first['application_id']}/withdraw",
    headers={**_headers(owner, owner), "If-Match": str(first["revision"])},
  )
  assert closed.status_code == 200, closed.text
  assert closed.json()["status"] == "withdrawn"
  reopened = client.post(
    f"/internal/console/applications/{first['application_id']}/withdraw",
    headers={**_headers(owner, owner), "If-Match": str(closed.json()["revision"])},
  )
  assert reopened.status_code == 409
  taken_again = client.post(
    "/internal/console/applications",
    headers=_headers(reviewer, reviewer),
    json={
      "public_account_id": first["public_account_id"],
      "purpose": "重新申请",
      "requested_quota_bytes": 1,
      "requested_quota_objects": 1,
    },
  )
  assert taken_again.status_code == 201, taken_again.text
