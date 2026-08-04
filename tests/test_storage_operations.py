from datetime import timedelta
from types import SimpleNamespace

import pytest
from bson import ObjectId

from src.core import minio_op
from src.modules.storage import operations, schema, service
from src.utils.helpers import utc_now


def test_parse_cluster_admin_info_reports_capacity_and_degraded_disk():
  server = SimpleNamespace(
    id=ObjectId(),
    name="beijing",
    host="10.32.129.241",
    minio_port=9000,
    region=SimpleNamespace(name="beijing", shown_name="北京"),
  )
  payload = {
    "info": {
      "mode": "online",
      "buckets": {"count": 4},
      "objects": {"count": 120},
      "versions": {"count": 130},
      "deletemarkers": {"count": 2},
      "usage": {"size": 4096},
      "backend": {"onlineDisks": 2, "offlineDisks": 1},
      "servers": [{
        "uptime": 60,
        "version": "2025-02-28T09:55:16Z",
        "drives": [{
          "endpoint": "/data/minio1",
          "path": "/data/minio1",
          "state": "ok",
          "totalspace": 1000,
          "usedspace": 250,
          "availspace": 750,
          "metrics": {"totalWaiting": 2},
        }],
      }],
      "pools": {"0": {"0": {"rawCapacity": 3000, "rawUsage": 750, "healDisks": 1}}},
    },
  }

  result = operations.parse_cluster_admin_info(
    server,
    payload,
    elapsed_ms=12.5,
    checked_at=utc_now(),
  )

  assert result["status"] == "degraded"
  assert result["raw_capacity_bytes"] == 3000
  assert result["raw_used_bytes"] == 750
  assert result["healing_disks"] == 1
  assert result["offline_disks"] == 1
  assert result["drives"][0]["waiting_operations"] == 2
  schema.ClusterHealthItem.model_validate(result)


def test_parse_replication_source_exposes_queue_failures_and_latency():
  arn = "arn:minio:replication::target-1:system-test"
  payload = {
    "replicationstats": {
      "currStats": {
        "Stats": {
          arn: {
            "replicationCount": 9,
            "completedReplicationSize": 8192,
            "failed": {"totals": {"count": 1, "bytes": 512}},
          },
        },
        "queued": {"curr": {"count": 2, "bytes": 2048}},
        "failed": {"totals": {"count": 1, "bytes": 512}},
      },
      "queueStats": {
        "nodes": [{
          "tgtTransferStats": {arn: {"Total": {"currRate": 1024.5}}},
          "mrfStats": {"failedCount_last5min": 3},
          "retries": {"total": 4},
        }],
      },
    },
    "remoteTargets": [{
      "endpoint": "10.31.133.207:9000",
      "arn": arn,
      "isOnline": True,
      "latency": {"curr": 5_000_000, "avg": 3_000_000, "max": 9_000_000},
      "totalDowntime": 2_000_000_000,
      "lastOnline": "2026-08-04T01:44:58Z",
    }],
  }

  result = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "hangzhou"],
    endpoints={"10.31.133.207:9000": "hangzhou"},
    elapsed_ms=8,
  )

  assert result["status"] == "degraded"
  assert result["queued_count"] == 2
  assert result["mrf_failed_last_5m"] == 3
  assert result["retries_total"] == 4
  target = result["targets"][0]
  assert target["target"] == "hangzhou"
  assert target["latency_current_ms"] == 5
  assert target["total_downtime_seconds"] == 2
  assert target["current_rate_bps"] == 1024.5
  schema.ReplicationSourceMetric.model_validate(result)


@pytest.mark.asyncio
async def test_server_details_returns_valid_cache_without_minio(monkeypatch):
  now = utc_now()
  server = SimpleNamespace(id=ObjectId())
  cache = SimpleNamespace(
    data=[{"name": "Bucket: system-test", "total_size": 1, "created_at": now, "files": []}],
    fetched_at=now,
    expires_at=now + timedelta(minutes=5),
  )

  async def no_fetch(_client):
    pytest.fail("valid cache must not call MinIO")

  async def read_server(_id):
    return server

  async def cleanup(_now):
    return 0

  async def read_cache(_id):
    return cache

  monkeypatch.setattr(service.storage_crud, "read_minio_server_by_id", read_server)
  monkeypatch.setattr(service.storage_crud, "delete_expired_server_file_details", cleanup)
  monkeypatch.setattr(service.storage_crud, "read_server_file_details_cache", read_cache)
  monkeypatch.setattr(service, "get_buckets_info", no_fetch)

  result = await service.get_server_details(server.id)

  assert result["cache_hit"] is True
  assert result["data"] == cache.data


@pytest.mark.asyncio
async def test_server_details_deletes_expired_then_refetches(monkeypatch):
  now = utc_now()
  server = SimpleNamespace(
    id=ObjectId(),
    host="10.32.129.241",
    minio_port=9000,
  )
  calls = {"cleanup": 0, "fetch": 0, "write": 0}
  stored = SimpleNamespace(
    data=[{"name": "Bucket: system-test", "total_size": 10, "created_at": now, "files": []}],
    fetched_at=now,
    expires_at=now + timedelta(minutes=10),
  )

  async def cleanup(_now):
    calls["cleanup"] += 1
    return 1

  async def no_cache(_id):
    return None

  async def fetch(_client):
    calls["fetch"] += 1
    return stored.data

  async def write(_id, data, fetched_at, expires_at):
    calls["write"] += 1
    stored.data = data
    stored.fetched_at = fetched_at
    stored.expires_at = expires_at
    return stored

  async def read_server(_id):
    return server

  monkeypatch.setattr(service.storage_crud, "read_minio_server_by_id", read_server)
  monkeypatch.setattr(service.storage_crud, "delete_expired_server_file_details", cleanup)
  monkeypatch.setattr(service.storage_crud, "read_server_file_details_cache", no_cache)
  monkeypatch.setattr(service.storage_crud, "plain_minio_credentials", lambda _server: ("key", "secret"))
  monkeypatch.setattr(service.storage_crud, "write_server_file_details_cache", write)
  monkeypatch.setattr(service, "get_minio_client", lambda **_kwargs: object())
  monkeypatch.setattr(service, "get_buckets_info", fetch)

  result = await service.get_server_details(server.id)

  assert calls["cleanup"] >= 1
  assert calls["fetch"] == 1
  assert calls["write"] == 1
  assert result["cache_hit"] is False


@pytest.mark.asyncio
async def test_resync_command_uses_argv_and_remote_arn(monkeypatch):
  captured = {}

  async def run(args, *, timeout):
    captured["args"] = args
    captured["timeout"] = timeout
    return True, [{"status": "success", "op": "resync-start"}], "", 3.0

  monkeypatch.setattr(minio_op, "run_mc_json", run)
  success, payload, error, _ = await minio_op.start_bucket_replication_resync(
    "beijing",
    "system-test",
    "arn:minio:replication::abc:system-test",
    older_than="7d12h",
    timeout=15,
  )

  assert success is True
  assert error == ""
  assert payload["op"] == "resync-start"
  assert captured["args"] == [
    "replicate", "resync", "start", "beijing/system-test",
    "--remote-bucket", "arn:minio:replication::abc:system-test",
    "--older-than", "7d12h",
  ]
