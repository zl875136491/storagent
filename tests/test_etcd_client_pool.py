"""Request-path Etcd clients reuse one authenticated channel."""
import inspect

import pytest

from src.core import etcd_op


class _FakeEtcd:
  created = 0
  closed = 0

  def __init__(self, **kwargs):
    type(self).created += 1
    self.kwargs = kwargs
    self.close_calls = 0

  async def close(self):
    self.close_calls += 1
    type(self).closed += 1


@pytest.fixture
async def reset_etcd_pool(monkeypatch):
  _FakeEtcd.created = 0
  _FakeEtcd.closed = 0
  monkeypatch.setattr(etcd_op, "aetcd", type("A", (), {"Client": _FakeEtcd}))
  monkeypatch.setattr(etcd_op.settings, "ETCD_HOST", "127.0.0.1")
  monkeypatch.setattr(etcd_op.settings, "ETCD_PORT", 2379)
  monkeypatch.setattr(etcd_op.settings, "ETCD_USERNAME", "root")
  monkeypatch.setattr(etcd_op.settings, "ETCD_PASSWORD", "secret")
  await etcd_op.close_shared_etcd_client()
  yield
  await etcd_op.close_shared_etcd_client()


@pytest.mark.asyncio
async def test_request_path_reuses_one_authenticated_client(reset_etcd_pool):
  first = await etcd_op.get_etcd_client()
  second = await etcd_op.get_etcd_client()
  await first.close()
  third = await etcd_op.get_etcd_client()

  assert first is second is third
  assert _FakeEtcd.created == 1
  assert first._inner.close_calls == 0
  assert first.kwargs["username"] == "root"


@pytest.mark.asyncio
async def test_dedicated_watch_client_can_close_without_dropping_pool(reset_etcd_pool):
  pooled = await etcd_op.get_etcd_client()
  watch = await etcd_op.get_etcd_client(dedicated=True)
  await watch.close()
  again = await etcd_op.get_etcd_client()

  assert watch is not pooled
  assert watch is not pooled._inner
  assert watch.close_calls == 1
  assert again is pooled
  assert _FakeEtcd.created == 2
  assert pooled._inner.close_calls == 0


@pytest.mark.asyncio
async def test_close_shared_etcd_client_drops_the_pool(reset_etcd_pool):
  first = await etcd_op.get_etcd_client()
  await etcd_op.close_shared_etcd_client()
  second = await etcd_op.get_etcd_client()

  assert first._inner.close_calls == 1
  assert second is not first
  assert _FakeEtcd.created == 2


def test_watch_reconnect_uses_a_dedicated_client():
  source = inspect.getsource(etcd_op.watch_etcd_task)
  assert "get_etcd_client(dedicated=True)" in source


def test_lifespan_keeps_watch_off_the_request_pool():
  from pathlib import Path
  text = (Path(__file__).resolve().parents[1] / "main.py").read_text()
  assert "get_etcd_client(dedicated=True)" in text
  assert "close_shared_etcd_client" in text
