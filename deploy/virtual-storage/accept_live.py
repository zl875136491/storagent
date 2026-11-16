"""对已部署的 HTTPS 入口做业务验收。输出不含凭据。"""
import json
import os
import ssl
import time
import urllib.error
import urllib.request
import uuid

CTX = ssl._create_unverified_context()


def request(method, url, payload=None, headers=None, timeout=30):
  data = None if payload is None else json.dumps(payload).encode()
  req = urllib.request.Request(url, data=data, method=method)
  for key, value in (headers or {}).items():
    req.add_header(key, value)
  if payload is not None:
    req.add_header("Content-Type", "application/json")
  try:
    with urllib.request.urlopen(req, context=CTX, timeout=timeout) as response:
      body = response.read()
      return response.status, body, dict(response.headers)
  except urllib.error.HTTPError as exc:
    return exc.code, exc.read(), dict(exc.headers)


def main():
  home = os.environ["VS_ACCEPT_HOME"]
  peer = os.environ["VS_ACCEPT_PEER"]
  app_name = os.environ["VS_BOOTSTRAP_APP_NAME"]
  credential = os.environ["VS_BOOTSTRAP_CREDENTIAL"]
  auth = {"Authorization": f"Bearer {credential}"}
  result = {"checks": []}

  def record(name, ok, detail):
    result["checks"].append({"name": name, "ok": bool(ok), "detail": detail})

  status, body, _headers = request("GET", home + "/ready")
  record("home_ready", status == 200 and b"virtual-storage" in body, f"status={status}")
  status, body, _headers = request("GET", peer + "/ready")
  record("peer_ready", status == 200 and b"nuc-docker-b" in body, f"status={status}")

  status, created, _headers = request(
    "POST",
    peer + f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    {
      "external_tenant_id": "live-" + uuid.uuid4().hex[:8],
      "logical_name": "live-" + uuid.uuid4().hex[:8],
      "quota_bytes": 2_000_000,
      "quota_objects": 10,
    },
    {**auth, "Idempotency-Key": uuid.uuid4().hex},
  )
  created_json = json.loads(created or b"{}")
  record("forward_create_tenant", status == 200 and created_json.get("plaintext_available") is True, f"status={status}")
  if status != 200:
    print(json.dumps(result, ensure_ascii=False))
    raise SystemExit(1)
  tenant_a = created_json
  status_b, other, _headers = request(
    "POST",
    home + f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    {
      "external_tenant_id": "live-" + uuid.uuid4().hex[:8],
      "logical_name": "live-" + uuid.uuid4().hex[:8],
      "quota_bytes": 2_000_000,
      "quota_objects": 10,
    },
    {**auth, "Idempotency-Key": uuid.uuid4().hex},
  )
  other_json = json.loads(other or b"{}")
  record("second_tenant", status_b == 200, f"status={status_b}")

  def upload(tenant, content):
    status, raw, _headers = request(
      "POST",
      home + f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads",
      {"object_key": "shared-name.txt", "size_bytes": len(content), "content_type": "text/plain"},
      {"X-API-Key": tenant["api_key"]},
    )
    upload_json = json.loads(raw or b"{}")
    if status != 200:
      return status, upload_json, b""
    put = urllib.request.Request(
      home + f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_json['upload_id']}/parts/1",
      data=content,
      method="PUT",
      headers={"X-API-Key": tenant["api_key"]},
    )
    with urllib.request.urlopen(put, context=CTX, timeout=30) as response:
      assert response.status == 200
    done_status, done_body, _headers = request(
      "POST",
      home + f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_json['upload_id']}/complete",
      {"parts": [{"part_number": 1, "etag": "deadbeef"}]},
      {"X-API-Key": tenant["api_key"]},
    )
    object_id = upload_json["object_id"]
    content_status = 0
    content_body = b""
    for _ in range(20):
      content_status, content_body, _headers = request(
        "GET",
        home + f"/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects/{object_id}/content",
        headers={"X-API-Key": tenant["api_key"]},
      )
      if content_status == 200:
        break
      time.sleep(1)
    return done_status, upload_json, content_body

  if status_b == 200:
    done_a, _upload_a, content_a = upload(tenant_a, b"tenant-a-bytes")
    done_b, _upload_b, content_b = upload(other_json, b"tenant-b-bytes")
    record("same_name_isolated", done_a == 202 and done_b == 202 and content_a == b"tenant-a-bytes" and content_b == b"tenant-b-bytes", f"{done_a}/{done_b}")
    freeze_status, freeze_body, _headers = request(
      "POST",
      home + f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{other_json['tenant_id']}/freeze",
      headers=auth,
    )
    blocked_status, _blocked, _headers = request(
      "POST",
      home + f"/api/virtual-storage/v1/tenants/{other_json['tenant_id']}/uploads",
      {"object_key": "after-freeze.txt", "size_bytes": 4},
      {"X-API-Key": other_json["api_key"]},
    )
    record("freeze_blocks_upload", freeze_status == 200 and blocked_status == 403, f"{freeze_status}/{blocked_status}")
    retire_status, retire_body, _headers = request(
      "POST",
      home + f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{other_json['tenant_id']}/retire",
      headers=auth,
    )
    final = ""
    for _ in range(30):
      view_status, view_body, _headers = request(
        "GET",
        home + f"/api/virtual-storage/v1/accounts/{app_name}/tenants/{other_json['tenant_id']}",
        headers=auth,
      )
      if view_status == 200:
        final = json.loads(view_body).get("state", "")
        if final == "retired":
          break
      time.sleep(1)
    record("retire_reaches_retired", retire_status == 200 and final == "retired", final or str(retire_status))
  print(json.dumps({key: value for key, value in result.items()}, ensure_ascii=False))
  if not all(item["ok"] for item in result["checks"]):
    raise SystemExit(1)


if __name__ == "__main__":
  main()
