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
  monkeypatch.setattr(storage_service.graph_crud, "update_bucket_edge_position", persist)
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
  monkeypatch.setattr(storage_service.graph_crud, "update_bucket_edge_position", persist)
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
