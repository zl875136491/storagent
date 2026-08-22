"""Celery producer used by the Storagent API process."""
from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from celery import Celery

from src.configs.configs import settings
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


celery_app = Celery("storagent-api", broker=broker_url(), backend=result_backend())
celery_app.conf.update(
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


def dispatch_task(name: str, *args: Any, **kwargs: Any) -> str | None:
  if not settings.CELERY_ENABLED:
    return None
  logger.debug("派发 Celery 任务 {}", name)
  result = celery_app.send_task(name, args=args, kwargs=kwargs)
  return str(result.id)
