"""Audit delivery must tolerate Celery redelivery without duplicate rows."""
import pytest
from pymongo.errors import DuplicateKeyError

from src.core import audit


@pytest.mark.asyncio
async def test_worker_audit_duplicate_event_is_a_success(monkeypatch):
  class AuditEvent:
    def __init__(self, **kwargs):
      self.kwargs = kwargs

    async def insert(self):
      raise DuplicateKeyError("event_id already exists")

  monkeypatch.setattr("src.modules.public.model.AuditEvent", AuditEvent)

  await audit.persist_audit_event(
    "storage.reconcile",
    "admin",
    "bucket-a",
    True,
    "done",
    event_id="event-1",
  )


@pytest.mark.asyncio
async def test_worker_audit_raises_transient_write_failure_for_retry(monkeypatch):
  class AuditEvent:
    def __init__(self, **_kwargs):
      pass

    async def insert(self):
      raise RuntimeError("mongo temporarily unavailable")

  monkeypatch.setattr("src.modules.public.model.AuditEvent", AuditEvent)

  with pytest.raises(RuntimeError, match="mongo temporarily unavailable"):
    await audit.persist_audit_event(
      "storage.reconcile",
      "admin",
      "bucket-a",
      True,
      "done",
      event_id="event-2",
    )
