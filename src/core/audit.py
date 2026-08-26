"""
关键操作审计：结构化日志 + Mongo 落库，便于跨区追溯。
"""
from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from pymongo.errors import DuplicateKeyError

from src.utils.logger import logger
from src.core import metrics as metrics_mod


def _detail_str(detail: Any) -> str:
  if detail is None:
    return ""
  if isinstance(detail, str):
    return detail[:2000]
  try:
    return json.dumps(detail, ensure_ascii=False, default=str)[:2000]
  except Exception:
    return str(detail)[:2000]


async def _persist(
  action: str,
  actor: str,
  resource: str,
  success: bool,
  detail: str,
  *,
  event_id: str = "",
  raise_on_failure: bool = False,
) -> None:
  try:
    from src.configs.configs import settings
    from src.modules.public.model import AuditEvent

    await AuditEvent(
      action=action,
      actor=actor,
      resource=resource,
      success=success,
      detail=detail,
      region=settings.REGION,
      event_id=event_id or None,
    ).insert()
  except DuplicateKeyError:
    # A broker acknowledgement can be lost after MongoDB committed the audit
    # row. The deterministic event id turns a redelivery into a no-op.
    return
  except Exception as e:
    # 落库失败不影响主流程
    logger.warning(f"审计落库失败: {e}")
    if raise_on_failure:
      raise


async def persist_audit_event(
  action: str,
  actor: str,
  resource: str,
  success: bool,
  detail: str,
  event_id: str = "",
) -> None:
  """Persist a worker-delivered audit event with retryable failure semantics.

  The event UUID is unique, so retrying after a broker acknowledgement or
  worker-loss window is safe. Local API fallbacks continue to use the best
  effort helper above and never make the foreground request fail.
  """
  await _persist(
    action,
    actor,
    resource,
    success,
    detail,
    event_id=event_id,
    raise_on_failure=True,
  )


def audit(
  action: str,
  *,
  actor: str | None = None,
  resource: str | None = None,
  detail: Any = None,
  success: bool = True,
) -> None:
  actor_s = actor or "-"
  resource_s = resource or "-"
  detail_s = _detail_str(detail)
  event_id = uuid.uuid4().hex
  payload = {
    "audit": True,
    "action": action,
    "actor": actor_s,
    "resource": resource_s,
    "success": success,
    "detail": detail_s or detail,
  }
  metrics_mod.incr("audit_events_total")
  if success:
    logger.info(f"[AUDIT] {payload}")
  else:
    logger.warning(f"[AUDIT] {payload}")
    metrics_mod.incr("audit_failures_total")

  try:
    from src.core.celery_client import dispatch_task
    task_id = dispatch_task(
      "storagent.audit.persist",
      action,
      actor_s,
      resource_s,
      success,
      detail_s,
      event_id,
    )
    if task_id is None:
      loop = asyncio.get_running_loop()
      loop.create_task(_persist(action, actor_s, resource_s, success, detail_s, event_id=event_id))
  except RuntimeError:
    # 无事件循环（如纯同步单测）时仅写日志
    pass
  except Exception as error:
    # Celery 暂时不可用时审计仍保留原有本地异步落库能力。
    logger.warning("Celery 审计任务派发失败，回退到本地执行: {}", error)
    try:
      loop = asyncio.get_running_loop()
      loop.create_task(_persist(action, actor_s, resource_s, success, detail_s, event_id=event_id))
    except RuntimeError:
      pass
