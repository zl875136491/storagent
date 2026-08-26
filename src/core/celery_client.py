"""Celery producer used by the Storagent API process."""
from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from celery import Celery

from src.configs.configs import settings
from src.core.celery_routing import task_headers, task_queue_name
from src.utils.logger import logger


def _default_mongodb_url(database: str) -> str:
  user = quote_plus(settings.MONGO_DB_USER)
  password = quote_plus(settings.MONGO_DB_PASSWD)
  auth_source = quote_plus(settings.MONGO_DB_AUTH_SOURCE)
  return (
    f"mongodb://{user}:{password}@{settings.MONGO_DB_HOST}:{settings.MONGO_DB_PORT}/"
    f"{database}?authSource={auth_source}"
  )


def broker_url() -> str:
  return settings.CELERY_BROKER_URL.strip() or _default_mongodb_url(settings.CELERY_MONGODB_DATABASE)


def result_backend() -> str:
  return settings.CELERY_RESULT_BACKEND.strip() or broker_url()


def _configured_default_queue() -> str:
  """Keep import-time test setup tolerant; startup validation is authoritative."""
  try:
    return task_queue_name(
      settings.REGION,
      queue_prefix=settings.CELERY_TASK_QUEUE_PREFIX,
      protocol_version=settings.CELERY_TASK_PROTOCOL_VERSION,
    )
  except ValueError:
    return "celery"


celery_app = Celery("storagent-api", broker=broker_url(), backend=result_backend())
celery_app.conf.update(
  task_default_queue=_configured_default_queue(),
  broker_transport_options={
    "ttl": True,
    "messages_collection": settings.CELERY_MONGODB_MESSAGES_COLLECTION,
    "routing_collection": settings.CELERY_MONGODB_ROUTING_COLLECTION,
    "queues_collection": settings.CELERY_MONGODB_QUEUES_COLLECTION,
  },
  mongodb_backend_settings={
    "database": settings.CELERY_MONGODB_DATABASE,
    "taskmeta_collection": settings.CELERY_MONGODB_RESULT_COLLECTION,
  },
)


def dispatch_task(
  name: str,
  *args: Any,
  origin_region: str | None = None,
  **kwargs: Any,
) -> str | None:
  """Publish a task only to the queue owned by its source Region.

  The envelope is intentionally carried in Celery headers rather than task
  arguments, so old positional task contracts do not get silently reshaped.
  A new worker rejects tasks without this envelope instead of consuming the
  legacy shared ``celery`` queue during a rolling upgrade.
  """
  if not settings.CELERY_ENABLED:
    return None
  region = origin_region or settings.REGION
  queue = task_queue_name(
    region,
    queue_prefix=settings.CELERY_TASK_QUEUE_PREFIX,
    protocol_version=settings.CELERY_TASK_PROTOCOL_VERSION,
  )
  logger.debug("派发 Celery 任务 {} queue={} origin={}", name, queue, region)
  result = celery_app.send_task(
    name,
    args=args,
    kwargs=kwargs,
    queue=queue,
    routing_key=queue,
    headers=task_headers(
      region,
      protocol_version=settings.CELERY_TASK_PROTOCOL_VERSION,
    ),
  )
  return str(result.id)
