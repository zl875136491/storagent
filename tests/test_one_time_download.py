import asyncio
import json
import re
import threading
from datetime import timedelta
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from src.core.auth import require_admin
from src.core.exception import CustomException, ErrorDesc
from src.modules.storage import download, route as storage_route
from src.utils.helpers import utc_now


class _Compare:
  def __init__(self, kind, key):
    self.kind = kind
    self.key = key
    self.expected = None

  def __eq__(self, expected):
    self.expected = expected
    return self


class _Operation:
  def __init__(self, kind, key, value=None, lease=None):
    self.kind = kind
    self.key = key
    self.value = value
    self.lease = lease


class _Transactions:
  def create(self, key):
    return _Compare("create", key)

  def mod(self, key):
    return _Compare("mod", key)

  def put(self, key, value, lease=None):
    return _Operation("put", key, value, lease)

  def delete(self, key):
    return _Operation("delete", key)


class _Lease:
  def __init__(self, ttl):
    self.ttl = ttl
    self.revoked = False

  async def revoke(self):
    self.revoked = True


class _EtcdState:
  def __init__(self):
    self.items = {}
    self.revision = 0
    self.lock = asyncio.Lock()
    self.lease_ttls = []


class _FakeEtcd:
  def __init__(self, state):
    self.state = state
    self.transactions = _Transactions()

  async def lease(self, ttl):
    self.state.lease_ttls.append(ttl)
    return _Lease(ttl)

  async def get(self, key):
    item = self.state.items.get(key)
    if not item:
      return None
    return SimpleNamespace(value=item["value"], mod_revision=item["mod_revision"])

  async def delete(self, key):
    self.state.items.pop(key, None)

  async def transaction(self, compare, success=None, failure=None):
    async with self.state.lock:
      valid = True
      for condition in compare:
        item = self.state.items.get(condition.key)
        actual = 0 if condition.kind == "create" and item is None else (
          item["create_revision"] if condition.kind == "create" else
          item["mod_revision"] if item else 0
        )
        valid = valid and actual == condition.expected
      operations = success if valid else failure
      for operation in operations or []:
        if operation.kind == "put":
          self.state.revision += 1
          self.state.items[operation.key] = {
            "value": operation.value,
            "create_revision": self.state.revision,
            "mod_revision": self.state.revision,
            "lease_ttl": operation.lease.ttl if operation.lease else None,
          }
        elif operation.kind == "delete":
          self.state.items.pop(operation.key, None)
      return valid, []

  async def close(self):
    return None


class _ObjectResponse:
  def __init__(self, body=b"file-data", content_type="application/octet-stream"):
    self.body = body
    self.offset = 0
    self.closed = False
    self.released = False
    self.headers = {
      "Content-Type": content_type,
      "Content-Length": str(len(body)),
    }

  def read(self, size):
    chunk = self.body[self.offset:self.offset + size]
    self.offset += len(chunk)
    return chunk

  def close(self):
    self.closed = True

  def release_conn(self):
    self.released = True


class _FakeMinio:
  def __init__(self):
    self.stat_error = None
    self.get_error = None
    self.opened = []

  def stat_object(self, bucket, object_key):
    if self.stat_error:
      raise self.stat_error
    return SimpleNamespace(size=9, content_type="text/plain")

  def get_object(self, bucket, object_key):
    if self.get_error:
      raise self.get_error
    response = _ObjectResponse(b"file-data", "text/plain")
    self.opened.append(response)
    return response


@pytest.fixture
def one_time_env(monkeypatch):
  state = _EtcdState()
  minio = _FakeMinio()
  server = SimpleNamespace(
    id="server-local-id",
    name="non-unique-minio-alias",
    host="10.32.129.241",
    minio_port=9000,
    region=SimpleNamespace(name="beijing"),
  )
  inventory = [{
    "name": "Bucket: system-test",
    "files": [{
      "name": "folder",
      "size": 9,
      "last_modified": "2026-08-04T00:00:00Z",
      "children": [{
        "name": "文件.txt",
        "size": 9,
        "last_modified": "2026-08-04T00:00:00Z",
      }],
    }],
  }]

  async def get_etcd():
    return _FakeEtcd(state)

  async def read_server_by_id(_server_id):
    return server

  async def read_server_by_region_name(name):
    return server if name == server.region.name else None

  async def read_cache(_server_id):
    return SimpleNamespace(
      data=inventory,
      expires_at=utc_now() + timedelta(minutes=5),
    )

  monkeypatch.setattr(download, "get_etcd_client", get_etcd)
  monkeypatch.setattr(download.storage_crud, "read_minio_server_by_id", read_server_by_id)
  monkeypatch.setattr(
    download.storage_crud,
    "read_minio_server_by_region_name",
    read_server_by_region_name,
  )
  monkeypatch.setattr(download.storage_crud, "read_server_file_details_cache", read_cache)
  monkeypatch.setattr(
    download.storage_crud,
    "plain_minio_credentials",
    lambda _server: ("access", "secret"),
  )
  monkeypatch.setattr(download, "get_minio_client", lambda *_args, **_kwargs: minio)
  monkeypatch.setattr(download.audit, "audit", lambda *_args, **_kwargs: None)
  monkeypatch.setattr(download.settings, "ONE_TIME_DOWNLOAD_TTL_SECONDS", 300)
  return SimpleNamespace(
    state=state,
    minio=minio,
    server=server,
    inventory=inventory,
  )


async def _issue():
  return await download.issue_one_time_download(
    "server-local-id",
    "system-test",
    "folder/文件.txt",
    "admin",
  )


async def _read_stream(response):
  chunks = []
  async for chunk in response.body_iterator:
    chunks.append(chunk)
  return b"".join(chunks)


@pytest.mark.asyncio
async def test_download_admin_dependency_accepts_admin_and_rejects_non_admin(monkeypatch):
  admin_role = SimpleNamespace(id="admin-role")

  async def get_admin_role():
    return admin_role

  monkeypatch.setattr("src.core.auth.user_crud.get_admin_role", get_admin_role)
  admin_link = SimpleNamespace(to_ref=lambda: SimpleNamespace(id="admin-role"))
  admin = SimpleNamespace(username="admin", roles=[admin_link])
  regular = SimpleNamespace(username="user", roles=[])

  assert await require_admin(admin) is admin
  with pytest.raises(CustomException) as exc_info:
    await require_admin(regular)
  assert exc_info.value.status_code == 403

  api_route = next(
    item for item in storage_route.router.routes
    if item.path == "/{minio_server_id}/objects/presigned-download"
  )
  assert require_admin in {dependency.call for dependency in api_route.dependant.dependencies}


def test_create_contract_keeps_capability_in_fragment_only(monkeypatch):
  token = "A" * 43

  async def issue(*_args, **_kwargs):
    return {
      "token": token,
      "expires_at": utc_now() + timedelta(minutes=5),
      "expires_in_seconds": 300,
      "filename": "file.txt",
    }

  async def admin():
    return SimpleNamespace(username="admin")

  monkeypatch.setattr(storage_route.storage_download, "issue_one_time_download", issue)
  app = FastAPI()
  app.include_router(storage_route.router, prefix="/api/storage")
  app.dependency_overrides[require_admin] = admin
  client = TestClient(app, base_url="http://backend.example:6783")

  response = client.post(
    "/api/storage/507f1f77bcf86cd799439011/objects/presigned-download",
    json={"bucket": "system-test", "object_key": "file.txt"},
  )

  assert response.status_code == 200
  result = response.json()
  parsed = urlsplit(result["download_url"])
  assert parsed.scheme == ""
  assert parsed.netloc == ""
  assert parsed.path == "/api/storage/objects/one-time-download"
  assert parsed.query == ""
  assert parsed.fragment == f"token={token}"
  assert token not in parsed.path
  assert result["url"] == result["download_url"]
  assert result["single_use"] is True
  assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_bootstrap_uses_fixed_path_native_post_and_strict_headers():
  response = await storage_route.download_one_time_object_bootstrap()
  html = response.body.decode("utf-8")
  nonce = re.search(r'<script nonce="([^"]+)">', html).group(1)

  assert "window.location.hash" in html
  assert html.index("window.history.replaceState") < html.index("new URLSearchParams")
  assert 'form.method = "post"' in html
  assert 'form.enctype = "application/x-www-form-urlencoded"' in html
  assert "form.submit()" in html
  assert "fetch(" not in html
  assert response.headers["cache-control"] == "private, no-store, max-age=0"
  assert response.headers["referrer-policy"] == "no-referrer"
  assert response.headers["x-content-type-options"] == "nosniff"
  csp = response.headers["content-security-policy"]
  assert f"script-src 'nonce-{nonce}'" in csp
  assert "default-src 'none'" in csp
  assert "form-action 'self'" in csp
  assert "frame-ancestors 'none'" in csp

  download_routes = [
    item for item in storage_route.router.routes
    if "one-time-download" in item.path
  ]
  assert {item.path for item in download_routes} == {"/objects/one-time-download"}
  assert {method for item in download_routes for method in item.methods} == {"GET", "POST"}


@pytest.mark.asyncio
async def test_anonymous_redeem_route_rate_limits_before_parsing_body(monkeypatch):
  token = "B" * 43
  body = f"token={token}".encode("ascii")
  receive_calls = 0
  calls = []

  async def receive():
    nonlocal receive_calls
    receive_calls += 1
    return {"type": "http.request", "body": body, "more_body": False}

  request = Request({
    "type": "http",
    "method": "POST",
    "path": "/api/storage/objects/one-time-download",
    "headers": [
      (b"content-type", b"application/x-www-form-urlencoded"),
      (b"content-length", str(len(body)).encode("ascii")),
    ],
    "client": ("198.51.100.10", 12345),
  }, receive=receive)

  def rate_limit(received_request):
    assert received_request is request
    assert receive_calls == 0
    calls.append("rate-limit")

  async def redeem(received_token):
    assert received_token == token
    calls.append("redeem")
    return "stream"

  monkeypatch.setattr(storage_route, "rate_limit_one_time_download", rate_limit)
  monkeypatch.setattr(storage_route.storage_download, "redeem_one_time_download", redeem)

  assert await storage_route.download_one_time_object(request) == "stream"
  assert calls == ["rate-limit", "redeem"]


@pytest.mark.asyncio
async def test_redeem_route_rejects_oversized_body_before_reading_it(monkeypatch):
  receive_calls = 0

  async def receive():
    nonlocal receive_calls
    receive_calls += 1
    return {"type": "http.request", "body": b"", "more_body": False}

  request = Request({
    "type": "http",
    "method": "POST",
    "path": "/api/storage/objects/one-time-download",
    "headers": [
      (b"content-type", b"application/x-www-form-urlencoded"),
      (b"content-length", b"257"),
    ],
    "client": ("198.51.100.10", 12345),
  }, receive=receive)
  monkeypatch.setattr(storage_route, "rate_limit_one_time_download", lambda _request: None)

  with pytest.raises(CustomException) as exc_info:
    await storage_route.download_one_time_object(request)

  assert exc_info.value.code == ErrorDesc.ONE_TIME_DOWNLOAD_INVALID.code
  assert receive_calls == 0


@pytest.mark.asyncio
async def test_redeem_route_rejects_chunked_body_over_limit(monkeypatch):
  chunks = iter([
    {"type": "http.request", "body": b"token=" + b"A" * 128, "more_body": True},
    {"type": "http.request", "body": b"B" * 128, "more_body": False},
  ])

  async def receive():
    return next(chunks)

  request = Request({
    "type": "http",
    "method": "POST",
    "path": "/api/storage/objects/one-time-download",
    "headers": [(b"content-type", b"application/x-www-form-urlencoded")],
    "client": ("198.51.100.10", 12345),
  }, receive=receive)
  monkeypatch.setattr(storage_route, "rate_limit_one_time_download", lambda _request: None)

  with pytest.raises(CustomException) as exc_info:
    await storage_route.download_one_time_object(request)

  assert exc_info.value.code == ErrorDesc.ONE_TIME_DOWNLOAD_INVALID.code


@pytest.mark.asyncio
async def test_issue_requires_object_in_fresh_inventory(one_time_env):
  one_time_env.inventory[0]["files"] = []

  with pytest.raises(CustomException) as exc_info:
    await _issue()

  assert exc_info.value.code == ErrorDesc.OBJECT_NOT_FOUND.code
  assert one_time_env.minio.opened == []
  assert one_time_env.state.items == {}


@pytest.mark.asyncio
async def test_issue_and_redeem_streams_filename_without_api_key(one_time_env):
  issued = await _issue()

  assert issued["expires_in_seconds"] == 300
  assert issued["filename"] == "文件.txt"
  assert len(one_time_env.state.items) == 1
  stored_payload = json.loads(next(iter(one_time_env.state.items.values()))["value"])
  assert stored_payload["region_name"] == "beijing"
  assert "server_name" not in stored_payload
  assert "api_key" not in stored_payload
  assert "secret" not in stored_payload

  response = await download.redeem_one_time_download(issued["token"])
  assert "filename*=UTF-8''%E6%96%87%E4%BB%B6.txt" in response.headers["content-disposition"]
  assert response.headers["cache-control"] == "private, no-store, max-age=0"
  assert await _read_stream(response) == b"file-data"
  assert one_time_env.minio.opened[0].closed is True
  assert one_time_env.minio.opened[0].released is True


@pytest.mark.asyncio
async def test_expired_grant_is_rejected_before_opening_minio(one_time_env):
  grant = download._DownloadGrant(
    region_name="beijing",
    bucket="system-test",
    object_key="folder/文件.txt",
    filename="文件.txt",
    expires_at=utc_now() - timedelta(seconds=1),
  )
  token = await download._store_grant(grant, 60)

  with pytest.raises(CustomException) as exc_info:
    await download.redeem_one_time_download(token)

  assert exc_info.value.code == ErrorDesc.ONE_TIME_DOWNLOAD_INVALID.code
  assert one_time_env.minio.opened == []
  assert one_time_env.state.items == {}


@pytest.mark.asyncio
async def test_concurrent_and_repeated_redemption_has_exactly_one_winner(one_time_env):
  token = (await _issue())["token"]

  results = await asyncio.gather(
    download.redeem_one_time_download(token),
    download.redeem_one_time_download(token),
    return_exceptions=True,
  )
  winners = [result for result in results if not isinstance(result, Exception)]
  losers = [result for result in results if isinstance(result, CustomException)]

  assert len(winners) == 1
  assert len(losers) == 1
  assert losers[0].code == ErrorDesc.ONE_TIME_DOWNLOAD_INVALID.code
  assert len(one_time_env.minio.opened) == 1
  assert await _read_stream(winners[0]) == b"file-data"
  with pytest.raises(CustomException) as exc_info:
    await download.redeem_one_time_download(token)
  assert exc_info.value.code == ErrorDesc.ONE_TIME_DOWNLOAD_INVALID.code


@pytest.mark.asyncio
async def test_source_failure_does_not_consume_token_and_can_be_retried(one_time_env):
  token = (await _issue())["token"]
  original_payload = next(iter(one_time_env.state.items.values()))["value"]
  one_time_env.minio.get_error = ConnectionError("node unavailable")

  with pytest.raises(CustomException) as exc_info:
    await download.redeem_one_time_download(token)

  assert exc_info.value.code == ErrorDesc.DOWNLOAD_SOURCE_UNAVAILABLE.code
  assert len(one_time_env.state.items) == 1
  restored = next(iter(one_time_env.state.items.values()))
  assert restored["value"] == original_payload
  assert restored["lease_ttl"] <= 299
  assert json.loads(restored["value"])["expires_at"] == json.loads(original_payload)["expires_at"]

  one_time_env.minio.get_error = None
  response = await download.redeem_one_time_download(token)
  assert await _read_stream(response) == b"file-data"
  assert one_time_env.state.items == {}


@pytest.mark.asyncio
async def test_restore_never_overwrites_an_existing_grant(one_time_env):
  token = (await _issue())["token"]
  key = download._token_key(token)
  current = one_time_env.state.items[key]["value"]
  stale = current.replace(b'"actor":"admin"', b'"actor":"stale"')
  expires_at = download._DownloadGrant.model_validate_json(current).expires_at

  restored = await download._restore_claimed_grant(key, stale, expires_at)

  assert restored is False
  assert one_time_env.state.items[key]["value"] == current


@pytest.mark.asyncio
async def test_missing_object_and_stat_node_failure_do_not_issue_token(one_time_env):
  missing = RuntimeError("missing")
  missing.code = "NoSuchKey"
  one_time_env.minio.stat_error = missing
  with pytest.raises(CustomException) as missing_exc:
    await _issue()
  assert missing_exc.value.code == ErrorDesc.OBJECT_NOT_FOUND.code
  assert one_time_env.state.items == {}

  one_time_env.minio.stat_error = ConnectionError("node unavailable")
  with pytest.raises(CustomException) as node_exc:
    await _issue()
  assert node_exc.value.code == ErrorDesc.DOWNLOAD_SOURCE_UNAVAILABLE.code
  assert node_exc.value.status_code == 503
  assert one_time_env.state.items == {}


@pytest.mark.asyncio
async def test_cancel_before_stream_restores_grant_and_closes_late_response(one_time_env):
  token = (await _issue())["token"]
  original_payload = next(iter(one_time_env.state.items.values()))["value"]
  started = threading.Event()
  allow_return = threading.Event()

  def blocking_get_object(_bucket, _object_key):
    response = _ObjectResponse(b"file-data", "text/plain")
    one_time_env.minio.opened.append(response)
    started.set()
    allow_return.wait(timeout=5)
    return response

  one_time_env.minio.get_object = blocking_get_object
  task = asyncio.create_task(download.redeem_one_time_download(token))
  assert await asyncio.to_thread(started.wait, 2)
  task.cancel()
  try:
    with pytest.raises(asyncio.CancelledError):
      await task
    assert len(one_time_env.state.items) == 1
    assert next(iter(one_time_env.state.items.values()))["value"] == original_payload
  finally:
    allow_return.set()

  for _ in range(100):
    if one_time_env.minio.opened[0].closed:
      break
    await asyncio.sleep(0.01)
  assert one_time_env.minio.opened[0].closed is True
  assert one_time_env.minio.opened[0].released is True


@pytest.mark.asyncio
async def test_closing_started_stream_closes_response_without_restoring_token(one_time_env):
  token = (await _issue())["token"]
  response = await download.redeem_one_time_download(token)

  first_chunk = await anext(response.body_iterator)
  assert first_chunk == b"file-data"
  await response.body_iterator.aclose()

  assert one_time_env.minio.opened[0].closed is True
  assert one_time_env.minio.opened[0].released is True
  assert one_time_env.state.items == {}
