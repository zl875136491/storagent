import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.core.exception import CustomException, ErrorDesc
from src.core.middleware import UploadBodyLimitMiddleware
from src.modules.files import service as files_service


def _http_scope(*, content_length: int | None = None) -> dict:
  headers = []
  if content_length is not None:
    headers.append((b"content-length", str(content_length).encode()))
  return {
    "type": "http",
    "asgi": {"version": "3.0"},
    "http_version": "1.1",
    "method": "POST",
    "scheme": "http",
    "path": "/api/v1/files/multipart/part",
    "raw_path": b"/api/v1/files/multipart/part",
    "query_string": b"",
    "headers": headers,
    "client": ("127.0.0.1", 1234),
    "server": ("test", 80),
  }


@pytest.mark.asyncio
async def test_upload_body_limit_rejects_content_length_before_app(monkeypatch):
  monkeypatch.setattr(
    files_service.settings,
    "APPLICATION_UPLOAD_MAX_PART_BYTES",
    8,
  )
  monkeypatch.setattr(UploadBodyLimitMiddleware, "_MULTIPART_OVERHEAD_BYTES", 2)
  called = False

  async def app(_scope, _receive, _send):
    nonlocal called
    called = True

  sent = []

  async def receive():
    raise AssertionError("oversized declared body must not be read")

  async def send(message):
    sent.append(message)

  middleware = UploadBodyLimitMiddleware(app)
  await middleware(_http_scope(content_length=11), receive, send)

  assert called is False
  assert sent[0]["status"] == 413
  payload = json.loads(sent[1]["body"])
  assert payload["code"] == ErrorDesc.UPLOAD_PART_TOO_LARGE.code


@pytest.mark.asyncio
async def test_upload_body_limit_rejects_chunked_body_and_suppresses_inner_error(
  monkeypatch,
):
  monkeypatch.setattr(
    files_service.settings,
    "APPLICATION_UPLOAD_MAX_PART_BYTES",
    8,
  )
  monkeypatch.setattr(UploadBodyLimitMiddleware, "_MULTIPART_OVERHEAD_BYTES", 2)
  messages = iter([
    {"type": "http.request", "body": b"123456", "more_body": True},
    {"type": "http.request", "body": b"78901", "more_body": False},
  ])

  async def receive():
    return next(messages)

  async def inner_app(scope, inner_receive, send):
    del scope
    try:
      while True:
        message = await inner_receive()
        if not message.get("more_body"):
          break
    except RuntimeError:
      await send({"type": "http.response.start", "status": 503, "headers": []})
      await send({"type": "http.response.body", "body": b"wrong"})

  sent = []

  async def send(message):
    sent.append(message)

  middleware = UploadBodyLimitMiddleware(inner_app)
  await middleware(_http_scope(), receive, send)

  assert [item.get("status") for item in sent if "status" in item] == [413]
  payload = json.loads(sent[-1]["body"])
  assert payload["code"] == ErrorDesc.UPLOAD_PART_TOO_LARGE.code


@pytest.mark.asyncio
async def test_upload_part_uses_bounded_read_before_quota_mutation(monkeypatch):
  monkeypatch.setattr(
    files_service.settings,
    "APPLICATION_UPLOAD_MAX_PART_BYTES",
    8,
  )
  monkeypatch.setattr(
    files_service.settings,
    "APPLICATION_UPLOAD_MAX_IN_MEMORY_PARTS",
    1,
  )

  @asynccontextmanager
  async def part_lock(*_args, **_kwargs):
    yield object()

  async def get_session(*_args, **_kwargs):
    return SimpleNamespace(declared_size_bytes=100)

  async def prepare(*_args, **_kwargs):
    pytest.fail("oversized part must be rejected before quota state mutation")

  class OversizedUpload:
    requested = None

    async def read(self, size):
      self.requested = size
      return b"x" * size

  upload = OversizedUpload()
  monkeypatch.setattr(files_service.files_quota, "upload_part_lock", part_lock)
  monkeypatch.setattr(files_service.files_quota, "get_upload_session", get_session)
  monkeypatch.setattr(files_service.files_quota, "prepare_part", prepare)

  with pytest.raises(CustomException) as exc_info:
    await files_service.multipart_upload_part(
      {"app_name": "app", "api_key_id": "key"},
      "object",
      "upload",
      1,
      upload,
    )

  assert upload.requested == 9
  assert exc_info.value.code == ErrorDesc.UPLOAD_PART_TOO_LARGE.code
