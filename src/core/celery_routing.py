"""Shared Celery task-envelope and regional queue naming helpers.

Both the API producer and the standalone worker import this module.  Keeping
the queue name and envelope contract in one place prevents an API in one
Region from silently handing a local Mongo task to a worker in another Region.
"""
from __future__ import annotations

import re
from collections.abc import Mapping


_COMPONENT_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_VERSION_RE = re.compile(r"^[1-9][0-9]{0,7}$")

HEADER_ORIGIN_REGION = "storagent-origin-region"
HEADER_TASK_PROTOCOL = "storagent-task-protocol"


class TaskEnvelopeError(RuntimeError):
  """A worker received a task that does not belong to its task protocol."""


def _component(value: str, *, field: str) -> str:
  normalized = str(value or "").strip().lower()
  if normalized in {"", "undefined", "unknown"} or not _COMPONENT_RE.fullmatch(normalized):
    raise ValueError(f"{field} is invalid: {value!r}")
  return normalized


def normalize_region(value: str) -> str:
  """Return the stable Region component used in routing keys."""
  return _component(value, field="region")


def normalize_queue_prefix(value: str) -> str:
  """Return a safe queue namespace shared by producer and worker."""
  return _component(value, field="queue prefix")


def normalize_protocol_version(value: str | int) -> str:
  normalized = str(value or "").strip()
  if not _VERSION_RE.fullmatch(normalized):
    raise ValueError(f"task protocol version is invalid: {value!r}")
  return normalized


def task_queue_name(
  region: str,
  *,
  queue_prefix: str = "storagent",
  protocol_version: str | int = "2",
) -> str:
  """Build a queue unique to one Region and one incompatible protocol."""
  return ".".join((
    normalize_queue_prefix(queue_prefix),
    normalize_region(region),
    "v" + normalize_protocol_version(protocol_version),
  ))


def task_headers(
  region: str,
  *,
  protocol_version: str | int = "2",
) -> dict[str, str]:
  """Create the minimal non-sensitive task envelope sent through Celery."""
  return {
    HEADER_ORIGIN_REGION: normalize_region(region),
    HEADER_TASK_PROTOCOL: normalize_protocol_version(protocol_version),
  }


def validate_task_headers(
  headers: Mapping[str, object] | None,
  *,
  worker_region: str,
  protocol_version: str | int,
) -> str:
  """Validate a task header against the receiving worker configuration.

  The return value is the normalized origin region.  Missing headers are
  rejected deliberately: accepting legacy default-queue work would reopen the
  cross-region consumption path during a rolling upgrade.
  """
  values = headers or {}
  try:
    origin = normalize_region(str(values.get(HEADER_ORIGIN_REGION) or ""))
    expected_region = normalize_region(worker_region)
    received_protocol = normalize_protocol_version(
      str(values.get(HEADER_TASK_PROTOCOL) or ""),
    )
    expected_protocol = normalize_protocol_version(protocol_version)
  except ValueError as error:
    raise TaskEnvelopeError("Celery 任务缺少有效的区域或协议头") from error
  if origin != expected_region:
    raise TaskEnvelopeError(
      f"Celery 任务来源区域不匹配: origin={origin} worker={expected_region}",
    )
  if received_protocol != expected_protocol:
    raise TaskEnvelopeError(
      "Celery 任务协议不匹配: "
      f"received=v{received_protocol} worker=v{expected_protocol}",
    )
  return origin
