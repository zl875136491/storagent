"""入队到独立前缀队列。broker 不可用时保留数据库状态，等待扫描补偿。"""
from __future__ import annotations

from src.utils.logger import logger
from src.virtual_storage.settings import get_settings


def enqueue_scan() -> None:
  settings = get_settings()
  if not settings.celery_broker_url:
    return
  try:
    from src.core.celery_routing import task_headers
    from src.virtual_storage.core.security import queue_name
    from src.virtual_storage.workers.celery_app import celery_app

    celery_app.send_task(
      "virtual_storage.scan",
      queue=queue_name(),
      headers=task_headers(settings.region, protocol_version=settings.protocol_version),
    )
  except Exception as exc:
    logger.warning(f"虚拟存储扫描任务入队失败，会话状态已保留: {type(exc).__name__}")
