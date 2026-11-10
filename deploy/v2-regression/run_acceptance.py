"""真实测试入口上的 v2 回归和模拟 APPS 客户端。不打印凭据。"""
from __future__ import annotations

import hashlib
import json
import os
import ssl
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

RESULTS: list[dict] = []
RUN_ID = os.environ.get("RUN_ID") or uuid.uuid4().hex[:10]
V2_A = os.environ.get("V2_BASE_A", "http://10.32.12.110:18088/server/local")
V2_B = os.environ.get("V2_BASE_B", "http://10.32.12.111:18088/server/local")
VS_A = os.environ.get("VS_BASE_A", "https://10.32.12.110:8443")
VS_B = os.environ.get("VS_BASE_B", "https://10.32.12.111:8443")
CTX = ssl._create_unverified_context()


def record(suite: str, name: str, ok: bool, detail: str, level: str = "info") -> None:
  RESULTS.append({"suite": suite, "name": name, "ok": bool(ok), "detail": detail[:500], "level": level})
  print(f"{'PASS' if ok else 'FAIL'} {suite} {name} {detail[:180]}")


def call(method: str, url: str, payload=None, headers=None, form=None, timeout=60):
  data = form if form is not None else (None if payload is None else json.dumps(payload).encode())
  req = urllib.request.Request(url, data=data, method=method)
  for key, value in (headers or {}).items():
    if value is not None:
      req.add_header(key, value)
  if payload is not None and form is None:
    req.add_header("Content-Type", "application/json")
  try:
    with urllib.request.urlopen(req, context=CTX, timeout=timeout) as response:
      return response.status, response.read(), dict(response.headers)
  except urllib.error.HTTPError as exc:
    return exc.code, exc.read(), dict(exc.headers)


def json_body(raw: bytes):
  try:
    return json.loads(raw.decode() or "{}")
  except json.JSONDecodeError:
    return {"_raw": raw[:200].decode("utf-8", "replace")}


def ensure_v2_admin() -> tuple[str, str]:
  secret_path = Path("/opt/storagent-v2reg/admin.pass")
  if not secret_path.exists():
    subprocess.run([
      "docker", "exec", "-i", "storagent-nuc-backend-a", "python", "-",
    ], input=b"""
import os
from pathlib import Path
import bcrypt
password = os.urandom(18).hex()
hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
Path('/tmp/v2admin.pass').write_text(password)
Path('/tmp/v2admin.hash').write_text(hashed)
print('hashed')
""", check=True)
    subprocess.check_call(["docker", "cp", "storagent-nuc-backend-a:/tmp/v2admin.pass", str(secret_path)])
    subprocess.check_call(["docker", "cp", "storagent-nuc-backend-a:/tmp/v2admin.hash", "/opt/storagent-v2reg/admin.hash"])
    os.chmod(secret_path, 0o600)
  password = secret_path.read_text().strip()
  hashed = Path("/opt/storagent-v2reg/admin.hash").read_text().strip()
  env = {}
  for line in Path("/opt/storagent-v2reg/env/backend-a.env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
      key, value = line.split("=", 1)
      env[key] = value
  from pymongo import MongoClient
  client = MongoClient(
    host=env["MONGO_DB_HOST"], port=int(env["MONGO_DB_PORT"]),
    username=env["MONGO_DB_USER"], password=env["MONGO_DB_PASSWD"],
    authSource=env["MONGO_DB_AUTH_SOURCE"],
  )
  db = client[env["MONGO_DB_NAME"]]
  db.user.update_one(
    {"username": "vsreg-admin"},
    {"$set": {
      "username": "vsreg-admin",
      "name": "vsreg-admin",
      "hashed_password": hashed,
      "permissions": ["application_manage", "system_manage"],
      "is_sync": False,
      "auth_version": 0,
      "roles_version": 0,
    }, "$setOnInsert": {"created_at": datetime.now(timezone.utc)}},
    upsert=True,
  )
  return "vsreg-admin", password


def login(base: str, username: str, password: str) -> str:
  status, raw, _headers = call("POST", f"{base}/api/v1/auth/login", {"username": username, "password": password})
  body = json_body(raw)
  token = body.get("access_token") or body.get("data", {}).get("access_token")
  if status != 200 or not token:
    raise RuntimeError(f"login failed {status}")
  return token


def v2_suite(token: str) -> str:
  auth = {"Authorization": f"Bearer {token}"}
  app_name = f"vsreg{RUN_ID}"
  status, raw, _headers = call("GET", f"{V2_A}/api/v1/public/endpoints", headers=auth)
  body = json_body(raw)
  record("v2", "endpoints", status == 200 and bool(body), f"status={status}")
  status, raw, _headers = call(
    "POST", f"{V2_A}/api/v1/public/application",
    {"name": app_name, "shown_name": f"reg {RUN_ID}", "description": "v2 regression", "domains": []},
    headers=auth,
  )
  created = json_body(raw)
  app_id = str(created.get("id") or "")
  record("v2", "create_application", status == 200 and bool(app_id), f"status={status}")
  if not app_id:
    return ""
  status, raw, _headers = call("POST", f"{V2_A}/api/v1/public/application/{app_id}/approval", headers=auth, timeout=180)
  text = raw.decode("utf-8", "replace")
  ready = "授权成功" in text or '"status": "success"' in text or "ready" in text
  record("v2", "approve_application", status == 200 and ready, f"status={status} tail={text[-180:]}")
  status, raw, _headers = call(
    "POST", f"{V2_A}/api/v1/public/api-key",
    {"application_id": app_id, "expired_at": None},
    headers=auth,
  )
  key_body = json_body(raw)
  api_key = key_body.get("key") or ""
  record("v2", "create_api_key", status == 200 and api_key.startswith("sk_") or api_key.startswith("ak_") or bool(api_key), f"status={status} prefix={api_key[:3]}")
  if not api_key:
    return ""
  key_header = {"x-api-key": api_key}
  bad, bad_raw, _headers = call("POST", f"{V2_A}/api/v2/files/object/stat", {"object_key": "missing"}, headers={"x-api-key": "invalid-key"})
  bad_text = bad_raw.decode("utf-8", "replace")
  record(
    "v2", "invalid_credential",
    bad == 400 and ("400029" in bad_text or "api_key" in bad_text or "API-KEY" in bad_text),
    f"status={bad} code_present={'400029' in bad_text}",
    "spec",
  )
  payload = b"v2-regression-" + RUN_ID.encode()
  digest = hashlib.sha256(payload).hexdigest()
  status, raw, _headers = call(
    "POST", f"{V2_A}/api/v2/files/multipart/init",
    {"content_type": "text/plain", "size_bytes": len(payload)},
    headers=key_header,
  )
  init_body = json_body(raw).get("data") or json_body(raw)
  upload_id = init_body.get("upload_id")
  object_key = init_body.get("object_key")
  record("v2", "multipart_init", status == 200 and bool(upload_id and object_key), f"status={status}")
  if not upload_id:
    return api_key
  boundary = "----v2reg"
  def part_form(number: int, data: bytes) -> bytes:
    return (
      f"--{boundary}\r\nContent-Disposition: form-data; name=\"upload_id\"\r\n\r\n{upload_id}\r\n"
      f"--{boundary}\r\nContent-Disposition: form-data; name=\"object_key\"\r\n\r\n{object_key}\r\n"
      f"--{boundary}\r\nContent-Disposition: form-data; name=\"part_number\"\r\n\r\n{number}\r\n"
      f"--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"p.bin\"\r\n"
      f"Content-Type: application/octet-stream\r\n\r\n"
    ).encode() + data + f"\r\n--{boundary}--\r\n".encode()
  status, raw, _headers = call(
    "POST", f"{V2_A}/api/v2/files/multipart/part",
    headers={**key_header, "Content-Type": f"multipart/form-data; boundary={boundary}"},
    form=part_form(1, payload),
  )
  part = json_body(raw).get("data") or json_body(raw)
  etag = str(part.get("etag") or "").strip('"')
  record("v2", "upload_part", status == 200 and bool(etag), f"status={status}")
  status, raw, _headers = call(
    "POST", f"{V2_A}/api/v2/files/multipart/part",
    headers={**key_header, "Content-Type": f"multipart/form-data; boundary={boundary}"},
    form=part_form(1, payload),
  )
  retried = json_body(raw).get("data") or json_body(raw)
  record("v2", "retry_same_part", status == 200 and str(retried.get("etag") or "").strip('"') == etag, f"status={status}")
  status, raw, _headers = call(
    "GET", f"{V2_A}/api/v2/files/multipart/parts?upload_id={upload_id}&object_key={object_key}",
    headers=key_header,
  )
  listed = json_body(raw).get("data") or json_body(raw)
  parts = listed.get("parts") or []
  record("v2", "list_parts", status == 200 and len(parts) == 1 and int(parts[0].get("size") or 0) == len(payload), f"status={status} n={len(parts)}")
  status, raw, _headers = call(
    "POST", f"{V2_A}/api/v2/files/multipart/complete",
    {"upload_id": upload_id, "object_key": object_key, "parts": [{"part_number": 1, "etag": etag}]},
    headers=key_header,
  )
  record("v2", "complete", status == 200, f"status={status}")
  status, raw, _headers = call("POST", f"{V2_A}/api/v2/files/object/stat", {"object_key": object_key}, headers=key_header)
  stat = json_body(raw).get("data") or json_body(raw)
  record("v2", "stat_size", status == 200 and int(stat.get("size") or -1) == len(payload), f"status={status} size={stat.get('size')}")
  status, raw, _headers = call("GET", f"{V2_A}/api/v2/files/object/download?object_key={object_key}", headers=key_header)
  record("v2", "download_sha256", status == 200 and hashlib.sha256(raw).hexdigest() == digest and len(raw) == len(payload), f"status={status} len={len(raw)}")
  status = 0
  raw = b""
  for _ in range(8):
    status, raw, _headers = call("GET", f"{V2_B}/api/v2/files/object/locate?object_key={object_key}", headers=key_header)
    if status == 200 and object_key in raw.decode("utf-8", "replace"):
      break
    time.sleep(1)
  record("v2", "locate_from_b", status == 200 and object_key in raw.decode("utf-8", "replace"), f"status={status}")
  status, raw, _headers = call("GET", f"{V2_A}/api/v2/files/objects?prefix=", headers=key_header)
  items = ((json_body(raw).get("data") or {}).get("items")) or []
  matched = [item for item in items if item.get("object_key") == object_key]
  record("v2", "list_contains_object", status == 200 and len(matched) == 1 and matched[0].get("state") == "active", f"status={status}")
  object_id = matched[0]["object_id"] if matched else ""
  if object_id:
    status, raw, _headers = call("POST", f"{V2_A}/api/v2/files/objects/{object_id}/share", {"expires_in_seconds": 60}, headers=key_header)
    share = (json_body(raw).get("data") or {})
    url = share.get("download_url") or ""
    token = ""
    if "#token=" in url:
      token = url.split("#token=", 1)[1]
    first, first_body, _headers = call("POST", f"{V2_A}/api/v2/storage/objects/one-time-download", form=f"token={token}".encode(), headers={"Content-Type": "application/x-www-form-urlencoded"})
    second, _second_body, _headers = call("POST", f"{V2_A}/api/v2/storage/objects/one-time-download", form=f"token={token}".encode(), headers={"Content-Type": "application/x-www-form-urlencoded"})
    record("v2", "share_once_then_410", first == 200 and hashlib.sha256(first_body).hexdigest() == digest and second == 410, f"first={first} second={second}")
    status, raw, _headers = call("DELETE", f"{V2_A}/api/v2/files/objects/{object_id}", headers=key_header)
    deleted = (json_body(raw).get("data") or {})
    record("v2", "soft_delete", status == 200 and deleted.get("state") == "soft_deleted", f"status={status} state={deleted.get('state')}")
    status, raw, _headers = call("GET", f"{V2_A}/api/v2/files/objects?state=trash", headers=key_header)
    trash = ((json_body(raw).get("data") or {}).get("items")) or []
    record("v2", "trash_lists_deleted", status == 200 and any(item.get("object_id") == object_id for item in trash), f"status={status}")
    status, raw, _headers = call("POST", f"{V2_A}/api/v2/files/objects/{object_id}/restore", headers=key_header)
    restored = (json_body(raw).get("data") or {})
    record("v2", "restore", status == 200 and restored.get("state") == "active", f"status={status} state={restored.get('state')}")
  abort_payload = b"abort-me"
  status, raw, _headers = call("POST", f"{V2_A}/api/v2/files/multipart/init", {"content_type": "text/plain", "size_bytes": len(abort_payload)}, headers=key_header)
  abort_init = json_body(raw).get("data") or json_body(raw)
  if abort_init.get("upload_id"):
    status, raw, _headers = call(
      "POST", f"{V2_A}/api/v2/files/multipart/abort",
      {"object_key": abort_init["object_key"], "upload_id": abort_init["upload_id"]},
      headers=key_header,
    )
    record("v2", "abort", status == 200, f"status={status}")
  return api_key


def vs_call(base: str, method: str, path: str, credential: str, payload=None, headers=None, body: bytes | None = None):
  hdrs = {"Authorization": f"Bearer {credential}"}
  hdrs.update(headers or {})
  if body is not None:
    return call(method, base + path, headers=hdrs, form=body)
  return call(method, base + path, payload, hdrs)


def apps_suite() -> None:
  env = {}
  for line in Path("/opt/storagent-vs/env/api.env").read_text().splitlines():
    if "=" in line and not line.startswith("#"):
      key, value = line.split("=", 1)
      env[key] = value
  app = env["VS_BOOTSTRAP_APP_NAME"]
  credential = env["VS_BOOTSTRAP_CREDENTIAL"]
  status, raw, _headers = vs_call(VS_A, "GET", f"/api/virtual-storage/v1/accounts/{app}", credential)
  account = json_body(raw)
  record("apps", "account_query", status == 200 and account.get("app_name") == app, f"status={status}")
  external = f"apps-{RUN_ID}"
  logical = f"tenant-{RUN_ID}"
  idem = f"idem-{RUN_ID}"
  payload = {"external_tenant_id": external, "logical_name": logical, "quota_bytes": 200000, "quota_objects": 4}
  status, raw, _headers = vs_call(VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants", credential, payload, {"Idempotency-Key": idem})
  first = json_body(raw)
  record("apps", "create_tenant", status == 200 and first.get("plaintext_available") is True and str(first.get("api_key", "")).startswith("vst_"), f"status={status}")
  status, raw, _headers = vs_call(VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants", credential, payload, {"Idempotency-Key": idem})
  replay = json_body(raw)
  record("apps", "idempotent_replay", status == 200 and replay.get("api_key") == first.get("api_key"), f"status={status}")
  conflict_payload = dict(payload, quota_bytes=200001)
  status, _raw, _headers = vs_call(VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants", credential, conflict_payload, {"Idempotency-Key": idem})
  record("apps", "idempotency_conflict", status == 409, f"status={status}")
  other_payload = {"external_tenant_id": external + "-b", "logical_name": logical + "-b", "quota_bytes": 200000, "quota_objects": 4}
  status, raw, _headers = vs_call(VS_B, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants", credential, other_payload, {"Idempotency-Key": idem + "-b"})
  second = json_body(raw)
  record("apps", "forward_create_second_tenant", status == 200 and second.get("tenant_id") != first.get("tenant_id"), f"status={status}")
  if not first.get("api_key") or not second.get("api_key"):
    return
  content_a = f"alpha-{RUN_ID}".encode()
  content_b = f"beta-{RUN_ID}".encode()
  object_a = upload_vs(VS_A, first, "same-name.txt", content_a)
  object_b = upload_vs(VS_A, second, "same-name.txt", content_b)
  record("apps", "same_name_isolated", object_a.get("sha_ok") is True and object_b.get("sha_ok") is True and object_a.get("body") != object_b.get("body"), f"a={object_a.get('status')} b={object_b.get('status')}")
  foreign, _raw, _headers = call(
    "GET", f"{VS_A}/api/virtual-storage/v1/tenants/{second['tenant_id']}/objects/{object_a.get('object_id')}/content",
    headers={"X-API-Key": second["api_key"]},
  )
  record("apps", "cross_tenant_object_404", foreign == 404, f"status={foreign}")
  status, raw, _headers = vs_call(VS_A, "GET", f"/api/virtual-storage/v1/accounts/{app}/tenants/{first['tenant_id']}", credential)
  tenant = json_body(raw)
  status, raw, _headers = vs_call(
    VS_A, "PATCH", f"/api/virtual-storage/v1/accounts/{app}/tenants/{first['tenant_id']}", credential,
    {"quota_bytes": 1, "quota_objects": 1}, {"If-Match": str(tenant.get("revision"))},
  )
  record("apps", "quota_cannot_drop_below_occupied", status == 409, f"status={status}")
  bad_match, _raw, _headers = vs_call(
    VS_A, "PATCH", f"/api/virtual-storage/v1/accounts/{app}/tenants/{first['tenant_id']}", credential,
    {"quota_bytes": tenant.get("quota_bytes_total"), "quota_objects": tenant.get("quota_objects_total")},
    {"If-Match": "0"},
  )
  record("apps", "if_match_conflict", bad_match == 412, f"status={bad_match}")
  rotate, raw, _headers = vs_call(
    VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants/{first['tenant_id']}/api-keys", credential,
    {"key_ttl_seconds": 86400}, {"Idempotency-Key": f"rotate-{RUN_ID}"},
  )
  rotated = json_body(raw)
  record("apps", "rotate_key", rotate == 200 and rotated.get("api_key") and rotated.get("api_key") != first.get("api_key"), f"status={rotate}")
  if rotated.get("key_id"):
    revoked, _raw, _headers = vs_call(VS_A, "DELETE", f"/api/virtual-storage/v1/accounts/{app}/tenants/{first['tenant_id']}/api-keys/{rotated['key_id']}", credential)
    denied, _raw, _headers = call("GET", f"{VS_A}/api/virtual-storage/v1/tenants/{first['tenant_id']}/objects", headers={"X-API-Key": rotated.get("api_key")})
    record("apps", "revoke_key", revoked == 200 and denied in {401, 403}, f"revoke={revoked} use={denied}")
  status, _raw, _headers = vs_call(VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants/{second['tenant_id']}/freeze", credential)
  blocked, _raw, _headers = call(
    "POST", f"{VS_A}/api/virtual-storage/v1/tenants/{second['tenant_id']}/uploads",
    {"object_key": "frozen.txt", "size_bytes": 4}, headers={"X-API-Key": second["api_key"]},
  )
  record("apps", "freeze_blocks_upload", status == 200 and blocked == 403, f"freeze={status} upload={blocked}")
  empty = upload_vs(VS_A, first, f"empty-{RUN_ID}.txt", b"")
  record("apps", "zero_byte_object", empty.get("status") == 200 and empty.get("sha_ok") is True and empty.get("body") == b"", f"status={empty.get('status')}")
  status, _raw, _headers = vs_call(VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants/{second['tenant_id']}/retire", credential)
  final = ""
  for _ in range(20):
    view, raw, _headers = vs_call(VS_A, "GET", f"/api/virtual-storage/v1/accounts/{app}/tenants/{second['tenant_id']}", credential)
    final = json_body(raw).get("state", "")
    if final == "retired":
      break
    time.sleep(1)
  record("apps", "retire_releases", status == 200 and final == "retired", f"state={final}")
  race_payload = {"external_tenant_id": f"race-{RUN_ID}", "logical_name": f"race-{RUN_ID}", "quota_bytes": 30, "quota_objects": 2}
  status, raw, _headers = vs_call(VS_A, "POST", f"/api/virtual-storage/v1/accounts/{app}/tenants", credential, race_payload, {"Idempotency-Key": f"race-{RUN_ID}"})
  race_tenant = json_body(raw)
  if status != 200 or not race_tenant.get("api_key"):
    record("apps", "concurrent_quota", False, f"setup={status}")
    return
  import concurrent.futures
  def one(name: str):
    code, _body, _headers = call(
      "POST", f"{VS_A}/api/virtual-storage/v1/tenants/{race_tenant['tenant_id']}/uploads",
      {"object_key": name, "size_bytes": 20}, headers={"X-API-Key": race_tenant["api_key"]},
    )
    return code
  with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
    codes = list(pool.map(one, ("race-a.bin", "race-b.bin")))
  record("apps", "concurrent_quota", sorted(codes) == [200, 409], f"codes={codes}")


def upload_vs(base: str, tenant: dict, object_key: str, content: bytes) -> dict:
  key = {"X-API-Key": tenant["api_key"]}
  status, raw, _headers = call("POST", f"{base}/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads", {"object_key": object_key, "size_bytes": len(content)}, headers=key)
  created = json_body(raw)
  if status != 200:
    return {"status": status, "sha_ok": False}
  if len(content) == 0:
    code, body, _headers = call("GET", f"{base}/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects/{created['object_id']}/content", headers=key)
    return {"status": code, "sha_ok": body == b"", "body": body, "object_id": created.get("object_id"), "complete": status}
  upload_id = created["upload_id"]
  digest = hashlib.sha256(content).hexdigest()
  wrong, _wrong_raw, _headers = call(
    "PUT", f"{base}/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_id}/parts/1",
    headers={**key, "X-Content-SHA256": "0" * 64}, form=content,
  )
  record("apps", "reject_bad_sha256", wrong == 409, f"status={wrong}")
  status, raw, _headers = call(
    "PUT", f"{base}/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_id}/parts/1",
    headers={**key, "X-Content-SHA256": digest}, form=content,
  )
  if status != 200:
    return {"status": status, "sha_ok": False}
  status, _raw, _headers = call("POST", f"{base}/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/uploads/{upload_id}/complete", headers=key)
  body = b""
  code = 0
  for _ in range(20):
    code, body, _headers = call("GET", f"{base}/api/virtual-storage/v1/tenants/{tenant['tenant_id']}/objects/{created['object_id']}/content", headers=key)
    if code == 200:
      break
    time.sleep(1)
  return {"status": code, "sha_ok": hashlib.sha256(body).hexdigest() == digest, "body": body, "object_id": created.get("object_id"), "complete": status}


def main() -> None:
  username, password = ensure_v2_admin()
  token = login(V2_A, username, password)
  v2_suite(token)
  apps_suite()
  passed = sum(1 for item in RESULTS if item["ok"])
  failed = len(RESULTS) - passed
  summary = {"run_id": RUN_ID, "total": len(RESULTS), "pass": passed, "fail": failed, "results": RESULTS}
  Path("/opt/storagent-v2reg/acceptance-result.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
  print(f"TOTAL {len(RESULTS)} PASS {passed} FAIL {failed}")
  if failed:
    raise SystemExit(1)


if __name__ == "__main__":
  main()
