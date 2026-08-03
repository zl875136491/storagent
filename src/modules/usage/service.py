from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

from src.configs.configs import settings
from src.core import metrics as metrics_mod
from src.core.exception import CustomException, ErrorDesc
from src.modules.public.model import APIKey, APIUsageEvent, Application
from src.utils.helpers import utc_now
from src.utils.logger import logger


UsageOperation = Literal["upload", "download"]
UsageInterval = Literal["hour", "day"]
MAX_QUERY_DAYS = 90
MAX_EVENT_ROWS = 2_000


def _as_utc(value: datetime) -> datetime:
  if value.tzinfo is None:
    return value.replace(tzinfo=timezone.utc)
  return value.astimezone(timezone.utc)


def normalize_range(
  start_at: datetime | None,
  end_at: datetime | None,
) -> tuple[datetime, datetime]:
  end = _as_utc(end_at) if end_at else utc_now()
  start = _as_utc(start_at) if start_at else end - timedelta(days=7)
  if start >= end:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "开始时间必须早于结束时间")
  if end - start > timedelta(days=MAX_QUERY_DAYS):
    raise CustomException(ErrorDesc.INVALID_PARAMS, f"单次最多查询 {MAX_QUERY_DAYS} 天")
  return start, end


def _period_start(value: datetime, interval: UsageInterval) -> datetime:
  value = _as_utc(value)
  if interval == "day":
    return value.replace(hour=0, minute=0, second=0, microsecond=0)
  return value.replace(minute=0, second=0, microsecond=0)


async def record_transfer(
  app_context: dict,
  operation: UsageOperation,
  bytes_transferred: int,
) -> None:
  """记录成功的数据传输；统计失败不反向影响已经完成的文件请求。"""
  try:
    await APIUsageEvent(
      api_key_id=str(app_context.get("api_key_id") or ""),
      api_key_hint=str(app_context.get("api_key_hint") or ""),
      app_name=str(app_context["app_name"]),
      app_shown_name=str(
        app_context.get("app_shown_name")
        or app_context["app_name"]
      ),
      operation=operation,
      bytes_transferred=max(int(bytes_transferred), 0),
      region=settings.REGION,
    ).insert()
    metrics_mod.incr(f"api_usage_{operation}_requests_total")
    metrics_mod.incr(
      f"api_usage_{operation}_bytes_total",
      max(int(bytes_transferred), 0),
    )
  except Exception as exc:
    try:
      metrics_mod.incr("api_usage_record_failures_total")
    except Exception:
      pass
    logger.warning(f"API 用量记录失败: {exc}")


async def get_options() -> dict:
  applications = await Application.find_all().sort("+name").to_list()
  api_keys = await APIKey.find(APIKey.deleted == False, fetch_links=True).to_list()
  key_options = []
  seen: set[str] = set()
  for item in api_keys:
    stable_id = str(item.key or item.id)
    if not stable_id or stable_id in seen:
      continue
    seen.add(stable_id)
    app = item.application
    if not isinstance(app, Application):
      continue
    key_options.append({
      "id": stable_id,
      "hint": item.key_hint or "************",
      "app_name": app.name,
      "app_shown_name": app.shown_name or app.name,
    })
  key_options.sort(key=lambda item: (item["app_name"], item["hint"]))
  return {
    "applications": [
      {"name": item.name, "shown_name": item.shown_name or item.name}
      for item in applications
    ],
    "api_keys": key_options,
  }


async def query_usage(
  *,
  start_at: datetime | None,
  end_at: datetime | None,
  interval: UsageInterval,
  app_name: str | None = None,
  api_key_id: str | None = None,
) -> dict:
  start, end = normalize_range(start_at, end_at)
  match: dict = {
    "occurred_at": {"$gte": start, "$lt": end},
  }
  if app_name:
    match["app_name"] = app_name
  if api_key_id:
    match["api_key_id"] = api_key_id

  def _operation_sum(operation: UsageOperation, value) -> dict:
    return {
      "$sum": {
        "$cond": [
          {"$eq": ["$operation", operation]},
          value,
          0,
        ],
      },
    }

  counters = {
    "upload_requests": _operation_sum("upload", 1),
    "upload_bytes": _operation_sum("upload", "$bytes_transferred"),
    "download_requests": _operation_sum("download", 1),
    "download_bytes": _operation_sum("download", "$bytes_transferred"),
  }
  facet_rows = await APIUsageEvent.aggregate([
    {"$match": match},
    {"$facet": {
      "totals": [
        {"$group": {"_id": None, **counters}},
        {"$project": {"_id": 0}},
      ],
      "points": [
        {"$group": {
          "_id": {
            "period_start": {
              "$dateTrunc": {
                "date": "$occurred_at",
                "unit": interval,
                "timezone": settings.TIMEZONE,
              },
            },
            "app_name": "$app_name",
            "api_key_id": "$api_key_id",
            "region": "$region",
          },
          "app_shown_name": {"$last": "$app_shown_name"},
          "api_key_hint": {"$last": "$api_key_hint"},
          "first_at": {"$min": "$occurred_at"},
          "last_at": {"$max": "$occurred_at"},
          **counters,
        }},
        {"$project": {
          "_id": 0,
          "period_start": "$_id.period_start",
          "app_name": "$_id.app_name",
          "app_shown_name": {
            "$cond": [
              {"$eq": ["$app_shown_name", ""]},
              "$_id.app_name",
              "$app_shown_name",
            ],
          },
          "api_key_id": "$_id.api_key_id",
          "api_key_hint": 1,
          "region": "$_id.region",
          "first_at": 1,
          "last_at": 1,
          "upload_requests": 1,
          "upload_bytes": 1,
          "download_requests": 1,
          "download_bytes": 1,
        }},
        {"$sort": {
          "period_start": 1,
          "app_name": 1,
          "api_key_id": 1,
          "region": 1,
        }},
      ],
      "events": [
        {"$sort": {"occurred_at": -1}},
        {"$limit": MAX_EVENT_ROWS},
        {"$project": {
          "_id": 0,
          "id": {"$toString": "$_id"},
          "occurred_at": 1,
          "app_name": 1,
          "app_shown_name": {
            "$cond": [
              {"$eq": ["$app_shown_name", ""]},
              "$app_name",
              "$app_shown_name",
            ],
          },
          "api_key_id": 1,
          "api_key_hint": 1,
          "operation": 1,
          "bytes_transferred": 1,
          "region": 1,
        }},
      ],
      "event_count": [{"$count": "value"}],
    }},
  ]).to_list()

  facet = facet_rows[0] if facet_rows else {}
  totals = (facet.get("totals") or [{
    "upload_requests": 0,
    "upload_bytes": 0,
    "download_requests": 0,
    "download_bytes": 0,
  }])[0]
  points = facet.get("points") or []
  latest_events = facet.get("events") or []
  event_count = (facet.get("event_count") or [{"value": 0}])[0]["value"]
  return {
    "region": settings.REGION,
    "start_at": start,
    "end_at": end,
    "interval": interval,
    "totals": totals,
    "points": points,
    "events": latest_events,
    "truncated": event_count > MAX_EVENT_ROWS,
  }
