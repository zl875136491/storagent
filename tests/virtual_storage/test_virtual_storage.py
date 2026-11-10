import uuid

from src.virtual_storage.core.exception import UnknownMinioResult
from src.virtual_storage.core.minio_gateway import MinioGateway


def _name(prefix: str) -> str:
  return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _account(client, quota_bytes=10_000, quota_objects=20):
  app_name = _name("app")
  credential = "mgmt-" + uuid.uuid4().hex
  response = client.post("/internal/drive", json={
    "op": "account",
    "app_name": app_name,
    "credential": credential,
    "quota_bytes": quota_bytes,
    "quota_objects": quota_objects,
  })
  assert response.status_code == 200, response.text
  return app_name, credential, response.json()


def _auth(credential: str) -> dict:
  return {"Authorization": f"Bearer {credential}"}


def _tenant(client, app_name, credential, quota_bytes, quota_objects, external=None, logical=None):
  external = external or _name("ext")
  logical = logical or _name("name")
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={**_auth(credential), "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": external,
      "logical_name": logical,
      "quota_bytes": quota_bytes,
      "quota_objects": quota_objects,
    },
  )
  assert response.status_code == 200, response.text
  body = response.json()
  assert body["plaintext_available"] is True
  assert body["api_key"].startswith("vst_")
  return body


def _upload_and_finish(client, tenant_id, api_key, object_key, payload: bytes):
  created = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant_id}/uploads",
    headers={"X-API-Key": api_key},
    json={"object_key": object_key, "size_bytes": len(payload), "content_type": "text/plain"},
  )
  assert created.status_code == 200, created.text
  upload = created.json()
  assert upload["state"] == "uploading", upload
  part = client.put(
    f"/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload['upload_id']}/parts/1",
    headers={"X-API-Key": api_key},
    content=payload,
  )
  assert part.status_code == 200, part.text
  again = client.get(
    f"/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload['upload_id']}",
    headers={"X-API-Key": api_key},
  )
  assert again.json()["confirmed_parts"][0]["part_number"] == 1
  complete = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant_id}/uploads/{upload['upload_id']}/complete",
    headers={"X-API-Key": api_key},
    json={"parts": [{"part_number": 1, "etag": "deadbeef"}]},
  )
  assert complete.status_code == 202, complete.text
  driven = client.post("/internal/drive", json={"op": "session", "upload_id": upload["upload_id"]})
  assert driven.status_code == 200, driven.text
  assert driven.json()["action"] in {"completed", "completed_from_stat"}, driven.text
  return upload


def test_ready_reports_transactions(client):
  response = client.get("/ready")
  assert response.status_code == 200
  assert response.json()["transactions"] is True


def test_management_credential_and_rejection(client):
  app_name, credential, _account_body = _account(client)
  ok = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential))
  assert ok.status_code == 200
  assert "management_credential_hash" not in ok.text
  rejected = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth("wrong-secret"))
  assert rejected.status_code == 401


def test_two_tenants_isolate_same_object_key(client):
  app_name, credential, _ = _account(client, quota_bytes=100_000, quota_objects=10)
  first = _tenant(client, app_name, credential, 40_000, 5)
  second = _tenant(client, app_name, credential, 40_000, 5)
  _upload_and_finish(client, first["tenant_id"], first["api_key"], "same.txt", b"alpha-tenant")
  _upload_and_finish(client, second["tenant_id"], second["api_key"], "same.txt", b"beta-tenant")
  listed = client.get(
    f"/api/virtual-storage/v1/tenants/{first['tenant_id']}/objects",
    headers={"X-API-Key": first["api_key"]},
  )
  assert listed.status_code == 200
  assert [row["object_key"] for row in listed.json()["objects"]] == ["same.txt"]
  foreign = client.get(
    f"/api/virtual-storage/v1/tenants/{second['tenant_id']}/objects/{listed.json()['objects'][0]['object_id']}/content",
    headers={"X-API-Key": second["api_key"]},
  )
  assert foreign.status_code == 404
  own = client.get(
    f"/api/virtual-storage/v1/tenants/{first['tenant_id']}/objects/{listed.json()['objects'][0]['object_id']}/content",
    headers={"X-API-Key": first["api_key"]},
  )
  assert own.status_code == 200
  assert own.content == b"alpha-tenant"


def test_quota_competition(client):
  app_name, credential, _ = _account(client, quota_bytes=100, quota_objects=4)
  first = _tenant(client, app_name, credential, 60, 2)
  conflict = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={**_auth(credential), "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 60,
      "quota_objects": 2,
    },
  )
  assert conflict.status_code == 409
  upload = client.post(
    f"/api/virtual-storage/v1/tenants/{first['tenant_id']}/uploads",
    headers={"X-API-Key": first["api_key"]},
    json={"object_key": "a.bin", "size_bytes": 40},
  )
  other = client.post(
    f"/api/virtual-storage/v1/tenants/{first['tenant_id']}/uploads",
    headers={"X-API-Key": first["api_key"]},
    json={"object_key": "b.bin", "size_bytes": 40},
  )
  assert upload.status_code == 200, upload.text
  assert other.status_code == 409, other.text


def test_key_delivery_window_and_query_have_no_plaintext(client):
  app_name, credential, _ = _account(client)
  idem = _name("idem")
  payload = {
    "external_tenant_id": _name("ext"),
    "logical_name": _name("name"),
    "quota_bytes": 1000,
    "quota_objects": 2,
  }
  first = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={**_auth(credential), "Idempotency-Key": idem},
    json=payload,
  ).json()
  replay = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={**_auth(credential), "Idempotency-Key": idem},
    json=payload,
  )
  assert replay.status_code == 200
  assert replay.json()["api_key"] == first["api_key"]
  mismatched = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={**_auth(credential), "Idempotency-Key": idem},
    json={**payload, "quota_bytes": 1001},
  )
  assert mismatched.status_code == 409
  changed = client.post("/internal/drive", json={"op": "expire_delivery", "key_id": first["key_id"]})
  assert changed.status_code == 200
  expired = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={**_auth(credential), "Idempotency-Key": idem},
    json=payload,
  )
  assert expired.status_code == 200
  assert expired.json()["plaintext_available"] is False
  assert "api_key" not in expired.json()
  assert first["api_key"] not in expired.text
  listed = client.get(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{first['tenant_id']}",
    headers=_auth(credential),
  )
  assert first["api_key"] not in listed.text


def test_management_and_tenant_credentials_are_not_interchangeable(client):
  app_name, credential, _ = _account(client)
  tenant = _tenant(client, app_name, credential, 1000, 2)
  as_tenant = client.get(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects",
    headers={"X-API-Key": credential},
  )
  as_management = client.get(
    f"/api/virtual-storage/v1/accounts/{app_name}",
    headers=_auth(tenant["api_key"]),
  )
  assert as_tenant.status_code == 401
  assert as_management.status_code == 401


def test_complete_uses_server_parts_and_resume(client):
  app_name, credential, _ = _account(client, quota_bytes=100_000, quota_objects=5)
  tenant = _tenant(client, app_name, credential, 50_000, 3)
  payload = b"server-side-etag"
  upload = _upload_and_finish(client, tenant["tenant_id"], tenant["api_key"], "resume.txt", payload)
  listed = client.get(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects",
    headers={"X-API-Key": tenant["api_key"]},
  ).json()["objects"]
  assert listed[0]["etag"] != "deadbeef"
  content = client.get(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects/{upload['object_id']}/content",
    headers={"X-API-Key": tenant["api_key"], "Range": "bytes=1-4"},
  )
  assert content.status_code == 206
  assert content.content == payload[1:5]
  assert content.headers["content-range"].startswith("bytes 1-4/")


def test_soft_delete_keeps_quota_until_single_purge(client):
  app_name, credential, _ = _account(client, quota_bytes=100, quota_objects=5)
  tenant = _tenant(client, app_name, credential, 100, 5)
  upload = _upload_and_finish(client, tenant["tenant_id"], tenant["api_key"], "keep.bin", b"1234567890")
  deleted = client.delete(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects/{upload['object_id']}",
    headers={"X-API-Key": tenant["api_key"]},
  )
  assert deleted.status_code == 200
  assert deleted.json()["state"] == "soft_deleted"
  hidden = client.get(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects",
    headers={"X-API-Key": tenant["api_key"]},
  )
  assert hidden.json()["objects"] == []
  blocked = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
    headers={"X-API-Key": tenant["api_key"]},
    json={"object_key": "next.bin", "size_bytes": 95},
  )
  assert blocked.status_code == 409
  account = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential)).json()
  assert account["quota_bytes_trashed"] == 10
  assert account["quota_bytes_used"] == 0
  purged = client.post("/internal/drive", json={"op": "purge", "object_id": upload["object_id"], "force": True})
  assert purged.json()["action"] == "purged"
  again = client.post("/internal/drive", json={"op": "purge", "object_id": upload["object_id"], "force": True})
  assert again.json()["action"] == "gone"
  account = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential)).json()
  assert account["quota_bytes_trashed"] == 0
  restored_capacity = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
    headers={"X-API-Key": tenant["api_key"]},
    json={"object_key": "next.bin", "size_bytes": 95},
  )
  assert restored_capacity.status_code == 200


def test_freeze_cancel_retire_and_name_reuse(client):
  app_name, credential, _ = _account(client, quota_bytes=10_000, quota_objects=10)
  external = _name("ext")
  logical = _name("name")
  tenant = _tenant(client, app_name, credential, 1000, 2, external, logical)
  frozen = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant['tenant_id']}/freeze",
    headers=_auth(credential),
  )
  assert frozen.json()["state"] == "frozen"
  denied = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
    headers={"X-API-Key": tenant["api_key"]},
    json={"object_key": "x", "size_bytes": 10},
  )
  assert denied.status_code == 403
  retired = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant['tenant_id']}/retire",
    headers=_auth(credential),
  )
  assert retired.json()["state"] == "retiring"
  cancelled = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant['tenant_id']}/cancel-retire",
    headers=_auth(credential),
  )
  assert cancelled.status_code == 200, cancelled.text
  assert cancelled.json()["state"] == "frozen"
  client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant['tenant_id']}/retire",
    headers=_auth(credential),
  )
  first_step = client.post("/internal/drive", json={"op": "retire", "tenant_id": tenant["tenant_id"]})
  assert first_step.json()["action"] == "purge_started"
  too_late = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant['tenant_id']}/cancel-retire",
    headers=_auth(credential),
  )
  assert too_late.status_code == 409
  second_step = client.post("/internal/drive", json={"op": "retire", "tenant_id": tenant["tenant_id"]})
  assert second_step.json()["action"] == "retired"
  reused = _tenant(client, app_name, credential, 1000, 2, external, logical)
  assert reused["tenant_id"] != tenant["tenant_id"]
  assert reused["generation"] == 2


def test_unknown_initiate_keeps_one_session_and_reservation(client):
  class TimeoutAfterCreate(MinioGateway):
    def create_multipart(self, bucket, storage_key, upload_id, content_type):
      super().create_multipart(bucket, storage_key, upload_id, content_type)
      raise UnknownMinioResult("timeout after send")

  original = client.app.state.gateway
  client.app.state.gateway = TimeoutAfterCreate()
  try:
    app_name, credential, _ = _account(client, quota_bytes=5000, quota_objects=5)
    tenant = _tenant(client, app_name, credential, 2000, 3)
    created = client.post(
      f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
      headers={"X-API-Key": tenant["api_key"]},
      json={"object_key": "uncertain.bin", "size_bytes": 32},
    )
    assert created.status_code == 200, created.text
    body = created.json()
    assert body["state"] == "initializing"
    assert body["reservation_held"] is True
    assert body["result_uncertain"] is True
    again = client.post(
      f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
      headers={"X-API-Key": tenant["api_key"]},
      json={"object_key": "uncertain.bin", "size_bytes": 32},
    )
    assert again.json()["upload_id"] == body["upload_id"]
    account = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential)).json()
    assert account["quota_bytes_reserved"] == 32
  finally:
    client.app.state.gateway = original


def test_open_inflight_blocks_release(client):
  app_name, credential, _ = _account(client, quota_bytes=5000, quota_objects=5)
  tenant = _tenant(client, app_name, credential, 2000, 3)
  created = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
    headers={"X-API-Key": tenant["api_key"]},
    json={"object_key": "held.bin", "size_bytes": 16},
  )
  upload_id = created.json()["upload_id"]
  client.put(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_id}/parts/1",
    headers={"X-API-Key": tenant["api_key"]},
    content=b"0123456789abcdef",
  )
  client.delete(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_id}",
    headers={"X-API-Key": tenant["api_key"]},
  )
  planted = client.post("/internal/drive", json={"op": "plant_inflight", "resource_id": upload_id})
  assert planted.status_code == 200
  held = client.post("/internal/drive", json={"op": "session", "upload_id": upload_id})
  assert held.json()["action"] == "hold"
  account = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential)).json()
  assert account["quota_bytes_reserved"] == 16


def test_reconcile_covers_intermediate_states_without_releasing(client):
  app_name, credential, _ = _account(client, quota_bytes=100_000, quota_objects=20)
  tenant = _tenant(client, app_name, credential, 80_000, 10)
  created = client.post(
    f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
    headers={"X-API-Key": tenant["api_key"]},
    json={"object_key": "recon.bin", "size_bytes": 8},
  )
  assert created.status_code == 200, created.text
  before = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential)).json()
  assert before["quota_bytes_reserved"] == 8
  report = client.post("/internal/drive", json={"op": "reconcile", "tenant_id": tenant["tenant_id"]})
  assert report.status_code == 200, report.text
  covered = set(report.json()["expected"]["states_covered"])
  for state in ("initializing", "uploading", "completing", "aborting", "reconciling", "purge_pending"):
    assert state in covered
  after = client.get(f"/api/virtual-storage/v1/accounts/{app_name}", headers=_auth(credential)).json()
  assert after["quota_bytes_reserved"] >= 8
  inflated = client.post("/internal/drive", json={
    "op": "inflate_reserved", "tenant_id": tenant["tenant_id"], "reserved": 999,
  })
  assert inflated.status_code == 200
  kept = client.post("/internal/drive", json={"op": "reconcile", "tenant_id": tenant["tenant_id"]})
  assert kept.json()["repaired"] == {}
  viewed = client.get(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant['tenant_id']}",
    headers=_auth(credential),
  )
  assert viewed.json()["quota_bytes_reserved"] == 999


def test_audit_does_not_store_plaintext_key(client):
  app_name, credential, account = _account(client)
  tenant = _tenant(client, app_name, credential, 1000, 2)
  found = client.post("/internal/drive", json={
    "op": "audit_contains",
    "account_id": account["account_id"],
    "needle": tenant["api_key"],
  })
  assert found.status_code == 200
  assert found.json()["contains"] is False


def test_console_login(client):
  app_name, credential, _ = _account(client)
  page = client.get("/console")
  assert page.status_code == 200
  assert "OIDC" in page.text
  login = client.post("/console/session", json={"app_name": app_name, "credential": credential})
  assert login.status_code == 200
  assert credential not in login.text
