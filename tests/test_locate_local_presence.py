"""对象定位只报告节点本地持有的副本。"""
from types import SimpleNamespace

import pytest

from src.modules.files import locate


class FakeMinioClient:
  def __init__(self, object_names):
    self.object_names = object_names
    self.list_calls = []
    self.stat_calls = []
    self.stat = object()

  def list_objects(self, bucket, prefix, recursive):
    self.list_calls.append((bucket, prefix, recursive))
    return [SimpleNamespace(object_name=name) for name in self.object_names]

  def stat_object(self, bucket, object_key):
    self.stat_calls.append((bucket, object_key))
    return self.stat


def _patch_client(monkeypatch, client):
  monkeypatch.setattr(locate.storage_crud, "plain_minio_credentials", lambda _server: ("access", "secret"))
  monkeypatch.setattr(locate, "get_minio_client", lambda *_args: client)


@pytest.mark.asyncio
async def test_stat_ignores_proxy_read_when_object_is_not_listed_locally(monkeypatch):
  client = FakeMinioClient([])
  _patch_client(monkeypatch, client)

  result = await locate._stat_on_server(
    SimpleNamespace(host="minio.example", minio_port=9000),
    "system-test",
    "object-key",
  )

  assert result is None
  assert client.list_calls == [("system-test", "object-key", True)]
  assert client.stat_calls == []


@pytest.mark.asyncio
async def test_stat_does_not_match_another_object_with_same_prefix(monkeypatch):
  client = FakeMinioClient(["object-key-suffix"])
  _patch_client(monkeypatch, client)

  result = await locate._stat_on_server(
    SimpleNamespace(host="minio.example", minio_port=9000),
    "system-test",
    "object-key",
  )

  assert result is None
  assert client.stat_calls == []


@pytest.mark.asyncio
async def test_stat_returns_metadata_for_exact_local_object(monkeypatch):
  client = FakeMinioClient(["object-key-suffix", "object-key"])
  _patch_client(monkeypatch, client)

  result = await locate._stat_on_server(
    SimpleNamespace(host="minio.example", minio_port=9000),
    "system-test",
    "object-key",
  )

  assert result is client.stat
  assert client.stat_calls == [("system-test", "object-key")]
