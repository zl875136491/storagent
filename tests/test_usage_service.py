from datetime import datetime, timedelta, timezone

import pytest

from src.core.exception import CustomException
from src.modules.usage import route as usage_route
from src.modules.usage import service as usage_service


def test_normalize_range_defaults_to_seven_days(monkeypatch):
  now = datetime(2026, 8, 3, 4, 5, 28, tzinfo=timezone.utc)
  monkeypatch.setattr(usage_service, "utc_now", lambda: now)

  start, end = usage_service.normalize_range(None, None)

  assert end == now
  assert start == now - timedelta(days=7)


def test_normalize_range_rejects_invalid_or_oversized_ranges():
  end = datetime(2026, 8, 3, tzinfo=timezone.utc)

  with pytest.raises(CustomException):
    usage_service.normalize_range(end, end)
  with pytest.raises(CustomException):
    usage_service.normalize_range(end - timedelta(days=91), end)


def test_period_start_normalizes_naive_utc_values():
  value = datetime(2026, 8, 3, 4, 35, 28)

  assert usage_service._period_start(value, "hour") == datetime(
    2026, 8, 3, 4, tzinfo=timezone.utc,
  )
  assert usage_service._period_start(value, "day") == datetime(
    2026, 8, 3, tzinfo=timezone.utc,
  )


@pytest.mark.asyncio
async def test_query_usage_aggregates_counts_bytes_and_latest_events(monkeypatch):
  aggregation = [{
    "totals": [{
      "upload_requests": 2,
      "upload_bytes": 3072,
      "download_requests": 1,
      "download_bytes": 4096,
    }],
    "points": [
      {
        "period_start": datetime(2026, 8, 3, 1, tzinfo=timezone.utc),
        "app_name": "app-a",
        "app_shown_name": "应用 A",
        "api_key_id": "hash-a",
        "api_key_hint": "sk_a************0001",
        "region": "beijing",
        "first_at": datetime(2026, 8, 3, 1, 5, tzinfo=timezone.utc),
        "last_at": datetime(2026, 8, 3, 1, 25, tzinfo=timezone.utc),
        "upload_requests": 1,
        "upload_bytes": 1024,
        "download_requests": 1,
        "download_bytes": 4096,
      },
      {
        "period_start": datetime(2026, 8, 3, 2, tzinfo=timezone.utc),
        "app_name": "app-b",
        "app_shown_name": "应用 B",
        "api_key_id": "hash-b",
        "api_key_hint": "sk_b************0002",
        "region": "beijing",
        "first_at": datetime(2026, 8, 3, 2, 10, tzinfo=timezone.utc),
        "last_at": datetime(2026, 8, 3, 2, 10, tzinfo=timezone.utc),
        "upload_requests": 1,
        "upload_bytes": 2048,
        "download_requests": 0,
        "download_bytes": 0,
      },
    ],
    "events": [
      {"id": "event-3"},
      {"id": "event-2"},
      {"id": "event-1"},
    ],
    "event_count": [{"value": 3}],
  }]
  captured_pipeline = []

  class FakeAggregation:
    async def to_list(self):
      return aggregation

  class FakeUsageEvent:
    @staticmethod
    def aggregate(pipeline):
      captured_pipeline.extend(pipeline)
      return FakeAggregation()

  monkeypatch.setattr(usage_service, "APIUsageEvent", FakeUsageEvent)
  monkeypatch.setattr(usage_service.settings, "REGION", "beijing")

  result = await usage_service.query_usage(
    start_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
    end_at=datetime(2026, 8, 4, tzinfo=timezone.utc),
    interval="hour",
  )

  assert result["totals"] == {
    "upload_requests": 2,
    "upload_bytes": 3072,
    "download_requests": 1,
    "download_bytes": 4096,
  }
  assert len(result["points"]) == 2
  assert result["points"][0]["upload_requests"] == 1
  assert result["points"][0]["download_requests"] == 1
  assert [item["id"] for item in result["events"]] == [
    "event-3", "event-2", "event-1",
  ]
  assert captured_pipeline[0]["$match"]["occurred_at"]["$gte"] == datetime(
    2026, 8, 3, tzinfo=timezone.utc,
  )
  point_group = captured_pipeline[1]["$facet"]["points"][0]["$group"]
  assert point_group["_id"]["period_start"]["$dateTrunc"]["unit"] == "hour"


@pytest.mark.asyncio
async def test_record_transfer_failure_never_breaks_file_request(monkeypatch):
  class BrokenUsageEvent:
    def __init__(self, **_kwargs):
      pass

    async def insert(self):
      raise RuntimeError("mongo unavailable")

  def broken_metric(*_args, **_kwargs):
    raise RuntimeError("metrics unavailable")

  monkeypatch.setattr(usage_service, "APIUsageEvent", BrokenUsageEvent)
  monkeypatch.setattr(usage_service.metrics_mod, "incr", broken_metric)

  await usage_service.record_transfer(
    {
      "api_key_id": "hash-a",
      "api_key_hint": "sk_a************0001",
      "app_name": "app-a",
      "app_shown_name": "应用 A",
    },
    "upload",
    1024,
  )


def test_usage_routes_require_admin_dependency():
  for path in {"", "/options"}:
    route = next(item for item in usage_route.router.routes if item.path == path)
    dependency_calls = {item.call for item in route.dependant.dependencies}
    assert usage_route.require_admin in dependency_calls
