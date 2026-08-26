"""Regression tests for the Region-bound Celery task envelope."""
from types import SimpleNamespace

import pytest

from src.core import celery_client
from src.core.celery_routing import (
  HEADER_ORIGIN_REGION,
  HEADER_TASK_PROTOCOL,
  TaskEnvelopeError,
  task_headers,
  task_queue_name,
  validate_task_headers,
)


def test_queue_name_is_region_and_protocol_specific():
  assert task_queue_name("Beijing", queue_prefix="storagent", protocol_version=2) == (
    "storagent.beijing.v2"
  )
  assert task_headers("Beijing", protocol_version=2) == {
    HEADER_ORIGIN_REGION: "beijing",
    HEADER_TASK_PROTOCOL: "2",
  }


@pytest.mark.parametrize(
  ("headers", "message"),
  [
    ({}, "缺少有效"),
    (task_headers("tianjin", protocol_version=2), "来源区域不匹配"),
    (task_headers("beijing", protocol_version=3), "协议不匹配"),
  ],
)
def test_worker_rejects_missing_or_incompatible_envelope(headers, message):
  with pytest.raises(TaskEnvelopeError, match=message):
    validate_task_headers(headers, worker_region="beijing", protocol_version=2)


def test_worker_accepts_matching_envelope():
  assert validate_task_headers(
    task_headers("beijing", protocol_version=2),
    worker_region="Beijing",
    protocol_version="2",
  ) == "beijing"


def test_dispatch_task_routes_to_origin_region_queue(monkeypatch):
  sent: dict = {}

  class FakeApp:
    def send_task(self, name, **kwargs):
      sent["name"] = name
      sent.update(kwargs)
      return SimpleNamespace(id="celery-task-1")

  monkeypatch.setattr(celery_client.settings, "CELERY_ENABLED", True)
  monkeypatch.setattr(celery_client.settings, "REGION", "beijing")
  monkeypatch.setattr(celery_client.settings, "CELERY_TASK_QUEUE_PREFIX", "storagent")
  monkeypatch.setattr(celery_client.settings, "CELERY_TASK_PROTOCOL_VERSION", 2)
  monkeypatch.setattr(celery_client, "celery_app", FakeApp())

  task_id = celery_client.dispatch_task(
    "storagent.storage.execute_operation",
    "operation-1",
    origin_region="tianjin",
  )

  assert task_id == "celery-task-1"
  assert sent == {
    "name": "storagent.storage.execute_operation",
    "args": ("operation-1",),
    "kwargs": {},
    "queue": "storagent.tianjin.v2",
    "routing_key": "storagent.tianjin.v2",
    "headers": {
      HEADER_ORIGIN_REGION: "tianjin",
      HEADER_TASK_PROTOCOL: "2",
    },
  }
