"""
关键操作审计：结构化日志，便于跨区追溯（谁在何时改了什么）。
"""
from __future__ import annotations

from typing import Any

from src.utils.logger import logger
from src.core import metrics as metrics_mod


def audit(
  action: str,
  *,
  actor: str | None = None,
  resource: str | None = None,
  detail: Any = None,
  success: bool = True,
) -> None:
  payload = {
    "audit": True,
    "action": action,
    "actor": actor or "-",
    "resource": resource or "-",
    "success": success,
    "detail": detail,
  }
  metrics_mod.incr("audit_events_total")
  if success:
    logger.info(f"[AUDIT] {payload}")
  else:
    logger.warning(f"[AUDIT] {payload}")
    metrics_mod.incr("audit_failures_total")
