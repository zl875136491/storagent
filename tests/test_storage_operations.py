from contextlib import asynccontextmanager
from datetime import timedelta
from types import SimpleNamespace

import pytest
from bson import ObjectId

from src.core import minio_op
from src.modules.storage import crud, operations, route, schema, service
from src.utils.helpers import utc_now


def test_server_details_cache_codec_round_trip_unicode_tree():
  data = [{
    "name": "Bucket: system-test",
    "total_size": 3,
    "created_at": "2026-08-04T10:00:00+08:00",
    "files": [{"name": "目录/文件.txt", "size": 3, "last_modified": "now"}],
  }]

  encoded = crud._encode_server_file_details(data)

  assert isinstance(encoded, bytes)
  assert crud._decode_server_file_details(encoded) == data


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


def test_parse_cluster_admin_info_marks_online_but_full_drive_critical():
  server = SimpleNamespace(
    id=ObjectId(),
    name="kunshan",
    host="10.8.136.107",
    minio_port=9000,
    region=SimpleNamespace(name="kunshan", shown_name="昆山"),
  )
  payload = {
    "info": {
      "mode": "online",
      "backend": {"onlineDisks": 3, "offlineDisks": 0},
      "servers": [{
        "drives": [
          {"endpoint": "/data/minio1", "path": "/data/minio1", "state": "ok", "totalspace": 15_995_754_250_240, "usedspace": 358_878_490_624, "availspace": 15_636_875_759_616, "used_inodes": 28_747, "free_inodes": 1_562_265_269},
          {"endpoint": "/data/minio2", "path": "/data/minio2", "state": "ok", "totalspace": 15_995_754_250_240, "usedspace": 358_878_490_624, "availspace": 15_636_875_759_616, "used_inodes": 28_747, "free_inodes": 1_562_265_269},
          {"endpoint": "/data/minio3", "path": "/data/minio3", "state": "ok", "totalspace": 15_553_527_808, "usedspace": 15_553_331_200, "availspace": 196_608, "used_inodes": 14_536, "free_inodes": 440},
        ],
      }],
    },
  }

  result = operations.parse_cluster_admin_info(server, payload, elapsed_ms=3, checked_at=utc_now())

  small_drive = next(item for item in result["drives"] if item["path"] == "/data/minio3")
  assert result["status"] == "critical"
  assert result["critical_disks"] == 1
  assert small_drive["health"] == "critical"
  assert small_drive["usage_percent"] == 100.0
  assert small_drive["capacity_skew"] is True
  assert any("严重阈值" in reason and "剩余 196608B" in reason for reason in small_drive["health_reasons"])
  schema.ClusterHealthItem.model_validate(result)


def test_parse_cluster_admin_info_marks_drive_threshold_warning():
  server = SimpleNamespace(
    id=ObjectId(), name="test", host="127.0.0.1", minio_port=9000,
    region=SimpleNamespace(name="test", shown_name="测试"),
  )
  payload = {"info": {"mode": "online", "backend": {"onlineDisks": 1}, "servers": [{"drives": [{"endpoint": "/data", "state": "ok", "totalspace": 1000, "usedspace": 900, "availspace": 100, "used_inodes": 90, "free_inodes": 10}]}]}}

  result = operations.parse_cluster_admin_info(server, payload, elapsed_ms=1, checked_at=utc_now())

  assert result["status"] == "degraded"
  assert result["warning_disks"] == 1
  assert result["drives"][0]["health"] == "warning"


def test_parse_replication_source_exposes_queue_failures_and_latency():
  arn = "arn:minio:replication::target-1:system-test"
  payload = {
    "replicationstats": {
      "currStats": {
        "Stats": {
          arn: {
            "replicationCount": 9,
            "completedReplicationSize": 8192,
            "failed": {
              "lastHour": {"count": 1, "bytes": 512},
              "totals": {"count": 1, "bytes": 512},
            },
          },
        },
        "queued": {"curr": {"count": 2, "bytes": 2048}},
        "failed": {
          "lastHour": {"count": 1, "bytes": 512},
          "totals": {"count": 1, "bytes": 512},
        },
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
    resync_by_arn=operations.parse_replication_resync_status({
      "resyncInfo": {"target": [{
        "arn": arn,
        "resetid": "reset-1",
        "startTime": "2026-08-04T03:40:26Z",
        "endTime": "2026-08-04T03:41:26Z",
        "resyncStatus": "Ongoing",
        "completedReplicationSize": 4096,
        "failedReplicationCount": 2,
        "failedReplicationSize": 256,
        "replicationCount": 7,
        "object": "object-key",
      }]},
    }),
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
  assert target["status"] == "syncing"
  assert target["resync_status"] == "running"
  assert target["resync_reset_id"] == "reset-1"
  assert target["resync_completed_bytes"] == 4096
  assert target["resync_object_count"] == 7
  assert target["resync_failed_count"] == 2
  assert target["resync_failed_bytes"] == 256
  assert target["recent_failed_count"] == 1
  schema.ReplicationSourceMetric.model_validate(result)


def test_mrf_only_is_a_diagnostic_and_does_not_degrade_source():
  arn = "arn:minio:replication::tianjin:mesh-e2e-20260731"
  payload = {
    "replicationstats": {
      "currStats": {
        "Stats": {arn: {
          "failed": {
            "lastHour": {"count": 0, "bytes": 0},
            "totals": {"count": 0, "bytes": 0},
          },
        }},
        "queued": {"curr": {"count": 0, "bytes": 0}},
        "failed": {
          "lastHour": {"count": 0, "bytes": 0},
          "totals": {"count": 0, "bytes": 0},
        },
      },
      "queueStats": {"nodes": [{
        "mrfStats": {"failedCount_last5min": 1},
      }]},
    },
    "remoteTargets": [{
      "endpoint": "10.17.158.115:9000",
      "arn": arn,
      "isOnline": True,
    }],
  }

  result = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "tianjin"],
    endpoints={"10.17.158.115:9000": "tianjin"},
    elapsed_ms=8,
  )

  assert result["status"] == "healthy"
  assert result["targets"][0]["status"] == "healthy"
  assert result["mrf_failed_last_5m"] == 1
  assert result["status_reasons"] == [{
    "code": "mrf_recent_backlog_observed",
    "message": "MinIO 报告过 MRF 补偿队列记录；该计数可能粘滞，仅供诊断",
    "value": 1,
    "severity": "info",
  }]
  schema.ReplicationSourceMetric.model_validate(result)


def test_mrf_with_recent_failures_remains_degraded_for_the_real_failure():
  arn = "arn:minio:replication::tianjin:mesh-e2e-20260731"
  payload = {
    "replicationstats": {
      "currStats": {
        "Stats": {arn: {
          "failed": {
            "lastHour": {"count": 0, "bytes": 0},
            "totals": {"count": 0, "bytes": 0},
          },
        }},
        "queued": {"curr": {"count": 0, "bytes": 0}},
        "failed": {
          "lastHour": {"count": 2, "bytes": 512},
          "totals": {"count": 2, "bytes": 512},
        },
      },
      "queueStats": {"nodes": [{
        "mrfStats": {"failedCount_last5min": 1},
      }]},
    },
    "remoteTargets": [{
      "endpoint": "10.17.158.115:9000",
      "arn": arn,
      "isOnline": True,
    }],
  }

  result = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "tianjin"],
    endpoints={"10.17.158.115:9000": "tianjin"},
    elapsed_ms=8,
  )

  assert result["status"] == "degraded"
  assert result["targets"][0]["status"] == "healthy"
  reasons = {item["code"]: item for item in result["status_reasons"]}
  assert reasons["recent_replication_failures"]["value"] == 2
  assert reasons["recent_replication_failures"]["severity"] == "degraded"
  assert reasons["mrf_recent_backlog_observed"]["severity"] == "info"


@pytest.mark.asyncio
async def test_replication_overview_aggregates_status_reasons(monkeypatch):
  servers = [
    SimpleNamespace(name="beijing", host="10.32.129.241", minio_port=9000),
    SimpleNamespace(name="tianjin", host="10.17.158.115", minio_port=9000),
  ]

  async def read_servers():
    return servers

  async def read_applications():
    return []

  async def read_metrics(source, _bucket, *, timeout):
    assert timeout > 0
    target = "tianjin" if source == "beijing" else "beijing"
    endpoint = (
      "10.17.158.115:9000" if target == "tianjin" else "10.32.129.241:9000"
    )
    arn = f"arn:minio:replication::{target}:mesh-e2e-20260731"
    recent_failed = 2 if source == "tianjin" else 0
    mrf_failed = 1 if source == "beijing" else 0
    return True, {
      "replicationstats": {
        "currStats": {
          "Stats": {arn: {
            "failed": {
              "lastHour": {"count": 0, "bytes": 0},
              "totals": {"count": 0, "bytes": 0},
            },
          }},
          "queued": {"curr": {"count": 0, "bytes": 0}},
          "failed": {
            "lastHour": {"count": recent_failed, "bytes": 512},
            "totals": {"count": recent_failed, "bytes": 512},
          },
        },
        "queueStats": {"nodes": [{
          "mrfStats": {"failedCount_last5min": mrf_failed},
        }]},
      },
      "remoteTargets": [{
        "endpoint": endpoint,
        "arn": arn,
        "isOnline": True,
      }],
    }, "", 1.0

  async def read_resync_status(*_args, **_kwargs):
    return True, {"resyncInfo": {"target": []}}, "", 1.0

  monkeypatch.setattr(operations.storage_crud, "read_minio_server_list", read_servers)
  monkeypatch.setattr(operations.public_crud, "read_application_list", read_applications)
  monkeypatch.setattr(operations.minio_op, "get_bucket_replication_metrics", read_metrics)
  monkeypatch.setattr(
    operations.minio_op,
    "get_bucket_replication_resync_status",
    read_resync_status,
  )

  result = await operations.get_replication_overview("mesh-e2e-20260731")

  assert result["buckets"][0]["status"] == "degraded"
  bucket_reasons = {
    item["code"]: item for item in result["buckets"][0]["status_reasons"]
  }
  assert bucket_reasons["degraded_sources"]["value"] == 1
  assert bucket_reasons["mrf_recent_backlog_observed"]["value"] == 1
  assert bucket_reasons["mrf_recent_backlog_observed"]["severity"] == "info"

  assert result["summary"]["status"] == "degraded"
  summary_reasons = {
    item["code"]: item for item in result["summary"]["status_reasons"]
  }
  assert summary_reasons["degraded_sources"]["value"] == 1
  assert summary_reasons["mrf_recent_backlog_observed"]["value"] == 1
  schema.ReplicationOperationsResponse.model_validate(result)


def test_completed_resync_with_failed_objects_is_partial():
  arn = "arn:minio:replication::target-1:one-v2"

  result = operations.parse_replication_resync_status({
    "resyncInfo": {"target": [{
      "arn": arn,
      "resyncStatus": "Completed",
      "startTime": "2026-08-04T03:42:08Z",
      "endTime": "2026-08-04T04:21:56Z",
      "completedReplicationSize": 99_796_971_825,
      "failedReplicationCount": 9,
      "replicationCount": 2241,
    }]},
  })[arn]

  assert result["resync_status"] == "partial"
  assert result["resync_failed_count"] == 9
  assert result["resync_completed_bytes"] == 99_796_971_825
  schema.ReplicationTargetMetric.model_validate({
    "source": "beijing",
    "target": "tianjin",
    "arn": arn,
    "endpoint": "10.17.158.115:9000",
    "status": "degraded",
    "online": True,
    **result,
  })


def test_resync_parser_accepts_single_target_and_string_counters():
  arn = "arn:minio:replication::target-1:one-v2"

  result = operations.parse_replication_resync_status({
    "resyncInfo": {"target": {
      "arn": arn,
      "resyncStatus": "Completed",
      "completedReplicationSize": "99796971825",
      "failedReplicationCount": "9",
      "failedReplicationSize": "256",
      "replicationCount": "2241",
    }},
  })[arn]

  assert result["resync_status"] == "partial"
  assert result["resync_object_count"] == 2241
  assert result["resync_completed_bytes"] == 99_796_971_825
  assert result["resync_failed_count"] == 9
  assert result["resync_failed_bytes"] == 256


def test_resync_parser_tolerates_missing_optional_fields():
  arn = "arn:minio:replication::target-1:one-v2"

  result = operations.parse_replication_resync_status({
    "resyncInfo": {"target": [{"arn": arn}]},
  })[arn]

  assert result == {
    "resync_status": "unknown",
    "resync_reset_id": "",
    "resync_started_at": None,
    "resync_updated_at": None,
    "resync_completed_bytes": 0,
    "resync_object_count": 0,
    "resync_failed_count": 0,
    "resync_failed_bytes": 0,
    "resync_current_object": "",
    "resync_error": "",
  }


def test_string_false_online_and_missing_arn_are_not_healthy():
  arn = "arn:minio:replication::target-1:one-v2"
  payload = {
    "replicationstats": {"currStats": {
      "Stats": {arn: {
        "replicationCount": "1",
        "completedReplicationSize": "1024",
        "failed": {"totals": {"count": "0", "bytes": "0"}},
      }},
    }},
    "remoteTargets": {
      "endpoint": "10.17.158.115:9000",
      "arn": arn,
      "isOnline": "false",
    },
  }

  result = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "tianjin"],
    endpoints={"10.17.158.115:9000": "tianjin"},
    elapsed_ms=8,
  )

  assert result["targets"][0]["online"] is False
  assert result["targets"][0]["status"] == "critical"
  assert result["actual_target_count"] == 1

  payload["remoteTargets"]["arn"] = ""
  missing_arn = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "tianjin"],
    endpoints={"10.17.158.115:9000": "tianjin"},
    elapsed_ms=8,
  )
  assert missing_arn["targets"][0]["status"] == "critical"
  assert missing_arn["actual_target_count"] == 0


def test_historical_replication_failures_do_not_keep_link_degraded():
  arn = "arn:minio:replication::target-1:one-v2"
  payload = {
    "replicationstats": {"currStats": {
      "Stats": {arn: {
        "replicationCount": 2248,
        "completedReplicationSize": 102_648_528_207,
        "failed": {
          "lastHour": {"count": 0, "bytes": 0},
          "totals": {"count": 383, "bytes": 26_355_568_041},
        },
      }},
      "failed": {
        "lastHour": {"count": 0, "bytes": 0},
        "totals": {"count": 383, "bytes": 26_355_568_041},
      },
      "queued": {"curr": {"count": 0, "bytes": 0}},
    }},
    "remoteTargets": [{
      "endpoint": "10.17.158.115:9000",
      "arn": arn,
      "isOnline": True,
    }],
  }

  result = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "tianjin"],
    endpoints={"10.17.158.115:9000": "tianjin"},
    elapsed_ms=8,
    resync_by_arn=operations.parse_replication_resync_status({
      "resyncInfo": {"target": [{
        "arn": arn,
        "resyncStatus": "Completed",
        "replicationCount": 2241,
      }]},
    }),
  )

  assert result["status"] == "healthy"
  assert result["failed_count"] == 383
  assert result["recent_failed_count"] == 0
  assert result["targets"][0]["status"] == "healthy"


def test_historical_failures_without_resync_record_remain_degraded():
  arn = "arn:minio:replication::kunshan:one-v2"
  payload = {
    "replicationstats": {"currStats": {
      "Stats": {arn: {
        "replicationCount": 2245,
        "completedReplicationSize": 102_449_168_471,
        "failed": {
          "lastHour": {"count": 0, "bytes": 0},
          "totals": {"count": 80, "bytes": 11_137_630_473},
        },
      }},
      "failed": {
        "lastHour": {"count": 0, "bytes": 0},
        "totals": {"count": 80, "bytes": 11_137_630_473},
      },
      "queued": {"curr": {"count": 0, "bytes": 0}},
    }},
    "remoteTargets": [{
      "endpoint": "10.8.136.107:9000",
      "arn": arn,
      "isOnline": True,
    }],
  }

  result = operations.parse_replication_source(
    "beijing",
    payload,
    server_names=["beijing", "kunshan"],
    endpoints={"10.8.136.107:9000": "kunshan"},
    elapsed_ms=8,
    resync_by_arn={},
  )

  assert result["targets"][0]["resync_status"] == "idle"
  assert result["targets"][0]["failed_count"] == 80
  assert result["targets"][0]["recent_failed_count"] == 0
  assert result["targets"][0]["status"] == "degraded"
  assert result["status"] == "degraded"


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


@pytest.mark.asyncio
async def test_start_resync_returns_existing_running_task(monkeypatch):
  arn = "arn:minio:replication::shenzhen:one-v2"
  servers = [
    SimpleNamespace(name="beijing", host="10.32.129.241", minio_port=9000),
    SimpleNamespace(name="shenzhen", host="10.41.102.223", minio_port=9000),
  ]

  async def read_servers():
    return servers

  async def read_metrics(_source, _bucket, *, timeout):
    assert timeout > 0
    return True, {
      "remoteTargets": [{"arn": arn, "endpoint": "10.41.102.223:9000"}],
    }, "", 1.0

  async def read_status(_source, _bucket, _arn=None, *, timeout):
    assert _arn == arn
    assert timeout > 0
    return True, {
      "resyncInfo": {"target": [{
        "arn": arn,
        "resyncStatus": "Ongoing",
        "replicationCount": 807,
        "completedReplicationSize": 34_643_839_580,
      }]},
    }, "", 1.0

  async def should_not_start(*_args, **_kwargs):
    pytest.fail("an existing resync must not be started again")

  monkeypatch.setattr(operations.storage_crud, "read_minio_server_list", read_servers)
  monkeypatch.setattr(operations.minio_op, "get_bucket_replication_metrics", read_metrics)
  monkeypatch.setattr(operations.minio_op, "get_bucket_replication_resync_status", read_status)
  monkeypatch.setattr(operations.minio_op, "start_bucket_replication_resync", should_not_start)
  monkeypatch.setattr(operations.audit, "audit", lambda *_args, **_kwargs: None)

  result = await operations.start_replication_resync(
    "one-v2",
    "beijing",
    "shenzhen",
    None,
    "admin",
  )

  assert result["message"] == "对象补传任务正在运行"
  assert result["detail"]["already_running"] is True
  assert result["detail"]["resync_status"] == "running"
  assert result["detail"]["resync_object_count"] == 807


@pytest.mark.asyncio
async def test_start_resync_queues_persistent_task(monkeypatch):
  arn = "arn:minio:replication::shenzhen:one-v2"
  servers = [
    SimpleNamespace(name="beijing", host="10.32.129.241", minio_port=9000),
    SimpleNamespace(name="shenzhen", host="10.41.102.223", minio_port=9000),
  ]

  class Operation:
    id = "resync-task"
    kind = "replication_resync"
    status = "queued"
    server = "beijing"
    bucket = "one-v2"
    target = "shenzhen"
    actor = "admin"
    message = ""
    result = {}

    async def save(self):
      return None

  operation = Operation()
  spawned = []

  async def read_servers():
    return servers

  async def read_metrics(*_args, **_kwargs):
    return True, {
      "remoteTargets": [{"arn": arn, "endpoint": "10.41.102.223:9000"}],
    }, "", 1.0

  async def read_status(*_args, **_kwargs):
    return True, {"resyncInfo": {"target": []}}, "", 1.0

  async def read_active(*_args, **_kwargs):
    return None

  async def create(**kwargs):
    assert kwargs["kind"] == "replication_resync"
    assert kwargs["server"] == "beijing"
    assert kwargs["bucket"] == "one-v2"
    assert kwargs["target"] == "shenzhen"
    return operation

  def spawn(coro):
    spawned.append(coro)
    coro.close()

  monkeypatch.setattr(operations.storage_crud, "read_minio_server_list", read_servers)
  monkeypatch.setattr(operations.storage_crud, "read_active_storage_operation", read_active)
  monkeypatch.setattr(operations.storage_crud, "create_storage_operation", create)
  monkeypatch.setattr(operations.minio_op, "get_bucket_replication_metrics", read_metrics)
  monkeypatch.setattr(operations.minio_op, "get_bucket_replication_resync_status", read_status)
  monkeypatch.setattr(operations, "_spawn", spawn)

  result = await operations.start_replication_resync(
    "one-v2",
    "beijing",
    "shenzhen",
    None,
    "admin",
  )

  assert len(spawned) == 1
  assert result["message"] == "对象补传启动任务已进入队列"
  assert result["detail"]["operation_id"] == "resync-task"
  assert result["detail"]["operation_status"] == "queued"


@pytest.mark.asyncio
async def test_resync_runner_handles_concurrent_start_as_idempotent(monkeypatch):
  arn = "arn:minio:replication::shenzhen:one-v2"
  status_calls = 0

  class Operation:
    id = "resync-task"
    kind = "replication_resync"
    status = "queued"
    server = "beijing"
    bucket = "one-v2"
    target = "shenzhen"
    actor = "admin"
    message = ""
    result = {
      "source_server": "beijing",
      "target_server": "shenzhen",
      "remote_arn": arn,
      "older_than": "",
    }

    async def save(self):
      return None

  @asynccontextmanager
  async def unlocked(_name):
    yield

  async def read_status(*_args, **_kwargs):
    nonlocal status_calls
    status_calls += 1
    targets = [] if status_calls == 1 else [{"arn": arn, "resyncStatus": "Ongoing"}]
    return True, {"resyncInfo": {"target": targets}}, "", 1.0

  async def start(*_args, **_kwargs):
    return False, {}, "Resync is already in progress", 1.0

  monkeypatch.setattr(operations, "_distributed_operation_lock", unlocked)
  monkeypatch.setattr(operations.minio_op, "get_bucket_replication_resync_status", read_status)
  monkeypatch.setattr(operations.minio_op, "start_bucket_replication_resync", start)
  monkeypatch.setattr(operations.metrics_mod, "incr", lambda *_args, **_kwargs: None)
  monkeypatch.setattr(operations.audit, "audit", lambda *_args, **_kwargs: None)

  operation = Operation()
  await operations._run_replication_resync_operation(operation)

  assert status_calls == 2
  assert operation.status == "succeeded"
  assert operation.result["native"]["already_running"] is True


@pytest.mark.asyncio
async def test_unmanaged_bucket_overview_classifies_non_application_buckets(monkeypatch):
  retained_at = utc_now()

  async def server_names():
    return ["beijing", "shenzhen"]

  async def authoritative_apps():
    return {
      "enabled-app": {"enabled": True, "shown_name": "启用应用"},
      "disabled-app": {"enabled": False, "shown_name": "停用应用"},
    }

  async def list_buckets(server, **_kwargs):
    values = {
      "beijing": ["enabled-app", "disabled-app", "orphan-a", "storagent-expired-archive"],
      "shenzhen": ["enabled-app", "orphan-a"],
    }
    return True, values[server], "", 1.0

  async def dispositions():
    return [SimpleNamespace(
      bucket="orphan-a",
      status="retained",
      reason="第三方迁移保留",
      updated_at=retained_at,
    )]

  async def latest(*_args, **_kwargs):
    return SimpleNamespace(
      status="failed",
      message="上一次空桶复核发现对象",
      created_at=retained_at - timedelta(minutes=2),
      finished_at=retained_at - timedelta(minutes=1),
    )

  monkeypatch.setattr(operations.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(operations, "_read_authoritative_application_entries", authoritative_apps)
  monkeypatch.setattr(operations.minio_op, "list_server_buckets", list_buckets)
  monkeypatch.setattr(operations.storage_crud, "list_unmanaged_bucket_dispositions", dispositions)
  monkeypatch.setattr(operations.storage_crud, "read_latest_storage_operation", latest)
  monkeypatch.setattr(operations.settings, "OBJECT_ARCHIVE_BUCKET", "storagent-expired-archive")

  result = await operations.get_unmanaged_bucket_overview()

  rows = {item["name"]: item for item in result["buckets"]}
  assert set(rows) == {"disabled-app", "orphan-a", "storagent-expired-archive"}
  assert result["summary"]["unmanaged_count"] == 1
  assert rows["orphan-a"]["kind"] == "unmanaged"
  assert rows["orphan-a"]["disposition"] == "retained"
  assert rows["disabled-app"]["kind"] == "disabled_application"
  assert rows["disabled-app"]["missing_servers"] == ["shenzhen"]
  assert rows["storagent-expired-archive"]["kind"] == "system"
  schema.UnmanagedBucketOperationsResponse.model_validate(result)


@pytest.mark.asyncio
async def test_unmanaged_bucket_overview_keeps_authoritative_app_bucket_managed_when_projection_is_missing(monkeypatch):
  async def server_names():
    return ["beijing"]

  async def authoritative_apps():
    return {"enabled-app": {"enabled": True, "shown_name": "权威应用"}}

  async def list_buckets(*_args, **_kwargs):
    return True, ["enabled-app", "manual-bucket"], "", 1.0

  async def dispositions():
    return []

  monkeypatch.setattr(operations.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(operations, "_read_authoritative_application_entries", authoritative_apps)
  monkeypatch.setattr(operations.minio_op, "list_server_buckets", list_buckets)
  monkeypatch.setattr(operations.storage_crud, "list_unmanaged_bucket_dispositions", dispositions)
  async def no_latest(*_args, **_kwargs):
    return None

  monkeypatch.setattr(operations.storage_crud, "read_latest_storage_operation", no_latest)

  result = await operations.get_unmanaged_bucket_overview()

  assert [item["name"] for item in result["buckets"]] == ["manual-bucket"]
  assert result["buckets"][0]["kind"] == "unmanaged"


@pytest.mark.asyncio
async def test_unmanaged_bucket_delete_requires_confirmation_and_queues_task(monkeypatch):
  async def overview():
    return {
      "buckets": [{
        "name": "manual-bucket",
        "kind": "unmanaged",
        "servers": ["beijing", "shenzhen"],
        "coverage_status": "complete",
        "missing_servers": [],
        "unreachable_servers": [],
      }],
    }

  async def no_active(*_args, **_kwargs):
    return None

  async def no_previous(*_args, **_kwargs):
    return None

  class Operation:
    id = "unmanaged-delete-task"
    kind = "unmanaged_bucket_delete"
    status = "queued"
    server = "all"
    bucket = "manual-bucket"
    target = ""
    actor = "admin"
    message = ""
    result = {}
    created_at = utc_now()
    started_at = None
    finished_at = None

    async def save(self):
      return None

  operation = Operation()
  queued = []

  async def create(**kwargs):
    assert kwargs == {
      "kind": "unmanaged_bucket_delete",
      "server": "all",
      "bucket": "manual-bucket",
      "target": "",
      "actor": "admin",
    }
    return operation

  def enqueue(operation_id, factory):
    queued.append(operation_id)
    coro = factory()
    coro.close()

  monkeypatch.setattr(operations, "get_unmanaged_bucket_overview", overview)
  monkeypatch.setattr(operations.storage_crud, "read_active_storage_operation", no_active)
  monkeypatch.setattr(operations.storage_crud, "read_latest_storage_operation", no_previous)
  monkeypatch.setattr(operations.storage_crud, "create_storage_operation", create)
  monkeypatch.setattr(operations, "_enqueue_storage_operation", enqueue)

  with pytest.raises(Exception, match="完全一致"):
    await operations.delete_unmanaged_bucket("manual-bucket", "other", "admin")

  result = await operations.delete_unmanaged_bucket("manual-bucket", "manual-bucket", "admin")

  assert queued == ["unmanaged-delete-task"]
  assert operation.result == {
    "servers": ["beijing", "shenzhen"],
    "target_servers": ["beijing", "shenzhen"],
    "removed_servers": [],
  }
  assert result["status"] == "queued"
  assert result["bucket"] == "manual-bucket"


@pytest.mark.asyncio
async def test_unmanaged_bucket_delete_rejects_retained_bucket(monkeypatch):
  async def overview():
    return {
      "buckets": [{
        "name": "manual-bucket",
        "kind": "unmanaged",
        "servers": ["beijing", "shenzhen"],
        "coverage_status": "complete",
        "missing_servers": [],
        "unreachable_servers": [],
        "disposition": "retained",
      }],
    }

  monkeypatch.setattr(operations, "get_unmanaged_bucket_overview", overview)

  with pytest.raises(Exception, match="受控保留"):
    await operations.delete_unmanaged_bucket("manual-bucket", "manual-bucket", "admin")


@pytest.mark.asyncio
async def test_unmanaged_bucket_partial_cleanup_retries_only_remaining_servers(monkeypatch):
  @asynccontextmanager
  async def unlocked(_name):
    yield

  class Operation:
    id = "retry-delete-task"
    kind = "unmanaged_bucket_delete"
    status = "queued"
    server = "all"
    bucket = "manual-bucket"
    target = ""
    actor = "admin"
    message = ""
    result = {
      "servers": ["beijing", "shenzhen"],
      "target_servers": ["shenzhen"],
      "removed_servers": ["beijing"],
    }
    created_at = utc_now()
    started_at = None
    finished_at = None

    async def save(self):
      return None

  async def overview():
    return {
      "buckets": [{
        "name": "manual-bucket",
        "kind": "unmanaged",
        "servers": ["shenzhen"],
        "coverage_status": "partial",
        "missing_servers": ["beijing"],
        "unreachable_servers": [],
      }],
    }

  async def summary(server, bucket, **_kwargs):
    assert (server, bucket) == ("shenzhen", "manual-bucket")
    return True, {"object_count": 0, "total_bytes": 0}, "", 1.0

  removed = []

  async def remove(server, bucket, **_kwargs):
    removed.append((server, bucket))
    return True, "", 1.0

  recorded = {}

  async def disposition(*_args, **kwargs):
    recorded.update(kwargs)

  monkeypatch.setattr(operations, "_distributed_operation_lock", unlocked)
  monkeypatch.setattr(operations, "get_unmanaged_bucket_overview", overview)
  monkeypatch.setattr(operations.minio_op, "get_bucket_object_summary", summary)
  monkeypatch.setattr(operations.minio_op, "remove_empty_bucket", remove)
  monkeypatch.setattr(operations.storage_crud, "upsert_unmanaged_bucket_disposition", disposition)
  monkeypatch.setattr(operations.metrics_mod, "incr", lambda *_args, **_kwargs: None)
  monkeypatch.setattr(operations.audit, "audit", lambda *_args, **_kwargs: None)

  operation = Operation()
  await operations._run_unmanaged_bucket_delete_operation(operation)

  assert removed == [("shenzhen", "manual-bucket")]
  assert operation.status == "succeeded"
  assert operation.result["removed_servers"] == ["beijing", "shenzhen"]
  assert recorded["servers"] == ["beijing", "shenzhen"]


def test_replication_task_acceptance_contract_is_explicit():
  operation = SimpleNamespace(
    id="operation-1",
    kind="replication_reconcile",
    status="queued",
    bucket="manual-bucket",
    server="all",
    target="",
    message="复制规则校准任务已进入队列",
    result={},
  )

  response = operations._replication_operation_response(operation)

  reconcile_route = next(
    item for item in route.router.routes
    if item.path == "/operations/replication/{bucket_name}/reconcile"
  )
  resync_route = next(
    item for item in route.router.routes
    if item.path == "/operations/replication/{bucket_name}/resync"
  )
  assert reconcile_route.status_code == 202
  assert resync_route.status_code == 202
  assert response["accepted"] is True
  assert response["operation_id"] == "operation-1"
  assert response["operation_status"] == "queued"
  schema.ReplicationOperationResponse.model_validate(response)
