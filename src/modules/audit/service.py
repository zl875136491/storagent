"""Database queries for the administrator-only audit timeline."""
from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone

from src.modules.public.model import AuditEvent
from src.utils.helpers import utc_now


MAX_PAGE_SIZE = 100
MAX_RANGE_DAYS = 365


def _as_utc(value: datetime | None) -> datetime | None:
  if value is None:
    return None
  if value.tzinfo is None:
    return value.replace(tzinfo=timezone.utc)
  return value.astimezone(timezone.utc)


def _item(event: AuditEvent) -> dict:
  return {
    "id": str(event.id),
    "action": event.action,
    "actor": event.actor,
    "resource": event.resource,
    "success": event.success,
    # Detail is already limited while writing. Keep an explicit display limit
    # so a legacy record cannot expand an administrative response indefinitely.
    "detail": event.detail[:4000],
    "region": event.region,
    "created_at": event.created_at,
  }


async def list_events(
  *,
  start_at: datetime | None = None,
  end_at: datetime | None = None,
  action: str | None = None,
  actor: str | None = None,
  region: str | None = None,
  resource: str | None = None,
  success: bool | None = None,
  page: int = 1,
  page_size: int = 50,
) -> dict:
  """Query audit history with bounded dates, filters and server-side paging."""
  page = max(page, 1)
  page_size = min(max(page_size, 1), MAX_PAGE_SIZE)
  end = _as_utc(end_at) or utc_now()
  start = _as_utc(start_at) or end - timedelta(days=30)
  if start >= end:
    raise ValueError("开始时间必须早于结束时间")
  if end - start > timedelta(days=MAX_RANGE_DAYS):
    raise ValueError(f"单次最多查询 {MAX_RANGE_DAYS} 天")

  filters: dict = {"created_at": {"$gte": start, "$lt": end}}
  if action:
    filters["action"] = action
  if actor:
    filters["actor"] = actor
  if region:
    filters["region"] = region
  if resource:
    # Resource is displayed as a label or object ID. A literal, case-insensitive
    # contains query is useful for triage while escaping prevents regex input.
    filters["resource"] = {"$regex": re.escape(resource), "$options": "i"}
  if success is not None:
    filters["success"] = success

  query = AuditEvent.find(filters)
  total = await query.count()
  events = await query.sort("-created_at").skip((page - 1) * page_size).limit(page_size).to_list()
  return {
    "data": [_item(event) for event in events],
    "total": total,
    "page": page,
    "page_size": page_size,
    "has_more": page * page_size < total,
  }


async def get_options() -> dict:
  """Read distinct filter values without exposing individual event details."""
  collection = AuditEvent.get_motor_collection()
  actions, actors, regions = await asyncio.gather(
    collection.distinct("action"),
    collection.distinct("actor"),
    collection.distinct("region"),
  )
  return {
    "actions": sorted(str(value) for value in actions if value),
    "actors": sorted(str(value) for value in actors if value),
    "regions": sorted(str(value) for value in regions if value),
  }
