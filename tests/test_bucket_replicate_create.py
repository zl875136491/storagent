import pytest

from src.core import minio_op
from src.core.exception import CustomException, ErrorDesc
from src.modules.storage import schema as storage_schema
from src.modules.storage import service as storage_service


@pytest.mark.asyncio
async def test_create_bucket_replicate_calls_minio_and_persists_ports(monkeypatch):
  calls = {"read": 0}

  async def server_names():
    return ["beijing", "hangzhou"]

  async def bucket_exists(_server, _bucket):
    return True

  async def replicate_infos(_bucket):
    calls["read"] += 1
    if calls["read"] == 1:
      return {"servers": {}, "replicates": []}
    return {
      "servers": {},
      "replicates": [{
        "from": "hangzhou",
        "to": "beijing",
        "from_position": "right",
        "to_position": "left",
        "status": {"status": "success"},
        "rule_id": "rule-1",
      }],
    }

  async def create_rule(
    from_server,
    to_server,
    bucket,
    *,
    priority,
    enabled,
    replicate_options,
  ):
    assert (from_server, to_server, bucket) == ("hangzhou", "beijing", "system-test")
    assert priority == 7
    assert enabled is False
    assert replicate_options == ["delete", "delete-marker", "metadata-sync"]
    return True, "ok"

  persisted = {}

  async def persist(bucket, from_server, to_server, from_position, to_position):
    persisted.update({
      "bucket": bucket,
      "from": from_server,
      "to": to_server,
      "from_position": from_position,
      "to_position": to_position,
    })

  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(storage_service, "check_server_bucket_existed", bucket_exists)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", replicate_infos)
  monkeypatch.setattr(storage_service, "create_minio_bucket_replicate", create_rule)
  monkeypatch.setattr(storage_service.graph_service, "set_bucket_edge_position", persist)
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  payload = storage_schema.BucketReplicateCreateRequest.model_validate({
    "from": "hangzhou",
    "to": "beijing",
    "from_side": "right",
    "to_side": "left",
    "status": {
      "status": "Disabled",
      "priority": 7,
      "delete_marker_replication": "Enabled",
      "existing_object_replication": "Disabled",
      "source_selection_criteria": "Enabled",
    },
  })
  result = await storage_service.create_bucket_replicate("system-test", payload)
  assert result["rule_id"] == "rule-1"
  assert persisted == {
    "bucket": "system-test",
    "from": "hangzhou",
    "to": "beijing",
    "from_position": "right",
    "to_position": "left",
  }


@pytest.mark.parametrize("bucket", ["../bad", "UPPERCASE", "bad;echo-x", "a..b"])
def test_bucket_name_rejects_shell_metacharacters(bucket):
  with pytest.raises(Exception):
    storage_service._validate_bucket_name(bucket)


@pytest.mark.asyncio
async def test_minio_replicate_command_applies_requested_options(monkeypatch):
  captured = {}

  async def run_cmd(command):
    captured["command"] = command
    return True, "ok"

  monkeypatch.setattr(minio_op, "_run_cmd", run_cmd)
  result = await minio_op.create_bucket_replicate(
    "hangzhou",
    "beijing",
    "system-test",
    priority=7,
    enabled=False,
    replicate_options=["delete", "existing-objects", "metadata-sync"],
  )
  assert result == (True, "创建复制成功")
  assert captured["command"] == (
    "mc replicate add hangzhou/system-test "
    "--remote-bucket beijing/system-test "
    "--replicate delete,existing-objects,metadata-sync --priority 7 --disable"
  )


@pytest.mark.asyncio
async def test_minio_replicate_translates_site_replication_conflict(monkeypatch):
  async def run_cmd(_command):
    return False, (
      "mc: <ERROR> unable to configure remote target. Cannot add remote target "
      "endpoint since this server is in a cluster replication setup."
    )

  monkeypatch.setattr(minio_op, "_run_cmd", run_cmd)

  success, reason = await minio_op.create_bucket_replicate(
    "hangzhou",
    "beijing",
    "system-test",
  )

  assert success is False
  assert reason == (
    "源站点已启用 Site Replication，无法创建 Bucket Replication；"
    "请先将受管 MinIO 节点迁移为桶复制模式"
  )


@pytest.mark.asyncio
async def test_remote_bucket_suffix_is_removed_exactly(monkeypatch):
  async def run_cmd(_command):
    return True, (
      "Rule ID: rule-1\n"
      "Remote Bucket: http://10.32.129.241:9001/bucket1\n"
    )

  monkeypatch.setattr(minio_op, "_run_cmd", run_cmd)
  result = await minio_op.get_bucket_replicate_info("hangzhou", "bucket1")
  assert result == {"10.32.129.241:9001": "rule-1"}


@pytest.mark.asyncio
async def test_create_returns_pending_when_minio_readback_lags(monkeypatch):
  async def server_names():
    return ["beijing", "hangzhou"]

  async def bucket_exists(_server, _bucket):
    return True

  async def replicate_infos(_bucket):
    return {"servers": {}, "replicates": []}

  async def create_rule(*_args, **_kwargs):
    return True, "ok"

  async def persist(*_args):
    return None

  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(storage_service, "check_server_bucket_existed", bucket_exists)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", replicate_infos)
  monkeypatch.setattr(storage_service, "create_minio_bucket_replicate", create_rule)
  monkeypatch.setattr(storage_service.graph_service, "set_bucket_edge_position", persist)
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  payload = storage_schema.BucketReplicateCreateRequest.model_validate({
    "from": "hangzhou",
    "to": "beijing",
  })
  result = await storage_service.create_bucket_replicate("system-test", payload)
  assert result["rule_id"] == ""
  assert result["status"]["status"] == "pending"


@pytest.mark.asyncio
async def test_create_rejects_when_bucket_is_missing_from_either_site(monkeypatch):
  async def server_names():
    return ["beijing", "hangzhou"]

  async def bucket_exists(server, _bucket):
    return server == "hangzhou"

  async def should_not_read_replicates(_bucket):
    pytest.fail("replication state must not be read when the target bucket is missing")

  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(storage_service, "check_server_bucket_existed", bucket_exists)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", should_not_read_replicates)

  payload = storage_schema.BucketReplicateCreateRequest.model_validate({
    "from": "hangzhou",
    "to": "beijing",
  })
  with pytest.raises(CustomException) as exc_info:
    await storage_service.create_bucket_replicate("system-test", payload)
  assert exc_info.value.code == ErrorDesc.RES_NOT_FOUND.code


@pytest.mark.asyncio
async def test_minio_delete_replicate_uses_rule_id(monkeypatch):
  captured = {}

  async def run_cmd(command):
    captured["command"] = command
    return True, "ok"

  monkeypatch.setattr(minio_op, "_run_cmd", run_cmd)
  result = await minio_op.delete_bucket_replicate("hangzhou", "system-test", "rule-abc")
  assert result == (True, "删除复制成功")
  assert captured["command"] == (
    "mc replicate remove --id rule-abc hangzhou/system-test"
  )


@pytest.mark.asyncio
async def test_delete_bucket_replicate_calls_minio_and_clears_edge(monkeypatch):
  deleted = {}

  async def server_names():
    return ["beijing", "hangzhou"]

  async def replicate_infos(_bucket):
    return {
      "servers": {},
      "replicates": [{
        "from": "hangzhou",
        "to": "beijing",
        "rule_id": "rule-1",
      }],
    }

  async def remove_rule(from_server, bucket, rule_id):
    assert (from_server, bucket, rule_id) == ("hangzhou", "system-test", "rule-1")
    return True, "ok"

  async def clear_edge(bucket, from_server, to_server):
    deleted.update({
      "bucket": bucket,
      "from": from_server,
      "to": to_server,
    })
    return True

  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", replicate_infos)
  monkeypatch.setattr(storage_service, "delete_minio_bucket_replicate", remove_rule)
  monkeypatch.setattr(storage_service.graph_service, "delete_bucket_edge_position", clear_edge)
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  result = await storage_service.delete_bucket_replicate(
    "system-test",
    from_server="hangzhou",
    to_server="beijing",
    rule_id="rule-1",
  )
  assert result["rule_id"] == "rule-1"
  assert deleted == {
    "bucket": "system-test",
    "from": "hangzhou",
    "to": "beijing",
  }


@pytest.mark.asyncio
async def test_delete_bucket_replicate_resolves_rule_id_when_omitted(monkeypatch):
  async def server_names():
    return ["beijing", "hangzhou"]

  async def replicate_infos(_bucket):
    return {
      "servers": {},
      "replicates": [{
        "from": "hangzhou",
        "to": "beijing",
        "rule_id": "rule-found",
      }],
    }

  captured = {}

  async def remove_rule(from_server, bucket, rule_id):
    captured["rule_id"] = rule_id
    return True, "ok"

  async def clear_edge(*_args):
    return True

  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", replicate_infos)
  monkeypatch.setattr(storage_service, "delete_minio_bucket_replicate", remove_rule)
  monkeypatch.setattr(storage_service.graph_service, "delete_bucket_edge_position", clear_edge)
  monkeypatch.setattr("src.core.audit.audit", lambda *args, **kwargs: None)

  result = await storage_service.delete_bucket_replicate(
    "system-test",
    from_server="hangzhou",
    to_server="beijing",
  )
  assert result["rule_id"] == "rule-found"
  assert captured["rule_id"] == "rule-found"


@pytest.mark.asyncio
async def test_delete_bucket_replicate_rejects_mismatched_rule_id(monkeypatch):
  async def server_names():
    return ["beijing", "hangzhou"]

  async def replicate_infos(_bucket):
    return {
      "servers": {},
      "replicates": [{
        "from": "hangzhou",
        "to": "beijing",
        "rule_id": "rule-real",
      }],
    }

  async def should_not_remove(*_args):
    pytest.fail("mismatched rule_id must not call mc replicate remove")

  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_infos", replicate_infos)
  monkeypatch.setattr(storage_service, "delete_minio_bucket_replicate", should_not_remove)

  with pytest.raises(CustomException) as exc_info:
    await storage_service.delete_bucket_replicate(
      "system-test",
      from_server="hangzhou",
      to_server="beijing",
      rule_id="rule-other",
    )
  assert exc_info.value.code == ErrorDesc.INVALID_RULE_PARAMS.code


@pytest.mark.asyncio
async def test_get_replicate_infos_migrates_legacy_percent_positions(monkeypatch):
  class Node:
    def __init__(self, server, x, y):
      self.server = server
      self.position_x = x
      self.position_y = y

  persisted = {}

  async def aliases():
    return {}

  async def server_names():
    return ["hangzhou", "beijing"]

  async def node_positions(_bucket):
    return [Node("hangzhou", 50, 25), Node("beijing", 10, 80)]

  async def edge_positions(_bucket):
    return []

  async def replicate_info(_server, _bucket):
    return None

  async def replicate_status(_server, _bucket):
    return {}

  async def update_pos(bucket, server, x, y):
    persisted[server] = {"bucket": bucket, "x": x, "y": y}

  monkeypatch.setattr(storage_service, "get_site_alias", aliases)
  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(
    storage_service.graph_crud, "read_many_bucket_node_positions", node_positions
  )
  monkeypatch.setattr(
    storage_service.graph_crud, "read_many_bucket_edge_positions", edge_positions
  )
  monkeypatch.setattr(storage_service, "get_bucket_replicate_info", replicate_info)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_status", replicate_status)
  monkeypatch.setattr(storage_service.graph_service, "set_bucket_node_position", update_pos)

  result = await storage_service.get_bucket_replicate_infos("system-test")
  assert result["servers"]["hangzhou"] == {"position_x": 450, "position_y": 140}
  assert result["servers"]["beijing"] == {"position_x": 90, "position_y": 448}
  assert persisted["hangzhou"]["x"] == 450
  assert persisted["beijing"]["y"] == 448


@pytest.mark.asyncio
async def test_get_replicate_infos_omits_default_zero_positions(monkeypatch):
  class Node:
    def __init__(self, server, x, y):
      self.server = server
      self.position_x = x
      self.position_y = y

  async def aliases():
    return {}

  async def server_names():
    return ["beijing", "hangzhou", "kunshan"]

  async def node_positions(_bucket):
    return [Node("hangzhou", 220, 340)]

  async def edge_positions(_bucket):
    return []

  async def replicate_info(_server, _bucket):
    return None

  async def replicate_status(_server, _bucket):
    return {}

  monkeypatch.setattr(storage_service, "get_site_alias", aliases)
  monkeypatch.setattr(storage_service.storage_crud, "read_minio_server_names", server_names)
  monkeypatch.setattr(
    storage_service.graph_crud, "read_many_bucket_node_positions", node_positions
  )
  monkeypatch.setattr(
    storage_service.graph_crud, "read_many_bucket_edge_positions", edge_positions
  )
  monkeypatch.setattr(storage_service, "get_bucket_replicate_info", replicate_info)
  monkeypatch.setattr(storage_service, "get_bucket_replicate_status", replicate_status)

  result = await storage_service.get_bucket_replicate_infos("system-test")
  assert result["servers"] == {"hangzhou": {"position_x": 220, "position_y": 340}}
  assert result["server_ids"] == ["beijing", "hangzhou", "kunshan"]
  assert "beijing" not in result["servers"]
  assert "kunshan" not in result["servers"]
