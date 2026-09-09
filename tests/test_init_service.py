"""Bounded init_service must not hang the process on a stuck Etcd client."""
import asyncio
import time

import pytest

from src.configs.configs import settings
from src.core.initialization import init_service


@pytest.mark.asyncio
async def test_init_service_timeout_still_tries_mongo_aliases(monkeypatch):
  hang = asyncio.Event()

  async def never():
    await hang.wait()
    raise AssertionError("init_service should have timed out")

  mongo = []

  async def aliases():
    mongo.append("ok")
    return 1

  monkeypatch.setattr(settings, "INIT_SERVICE_TIMEOUT_SECONDS", 0.05)
  monkeypatch.setattr(settings, "INIT_SERVICE_STEP_TIMEOUT_SECONDS", 0.05)
  monkeypatch.setattr("src.core.etcd_op.get_etcd_client", never)
  monkeypatch.setattr("src.core.sync.ensure_mc_aliases_from_mongo", aliases)

  started = time.monotonic()
  await init_service()
  assert time.monotonic() - started < 1.5
  assert mongo == ["ok"]
