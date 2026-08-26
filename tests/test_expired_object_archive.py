from types import SimpleNamespace
from datetime import timedelta

import pytest

from src.modules.files import archive
from src.utils.helpers import utc_now


def _catalog_item():
  return SimpleNamespace(
    app_name="system-test",
    object_id="obj-archive-1",
    bucket="system-test",
    storage_key="reports/expired.csv",
    object_key="reports/expired.csv",
    minio_version_id="version-1",
    size_bytes=16,
    etag="source-etag",
    source_region="beijing",
    deletion_generation=2,
    state="soft_deleted",
    restore_until=utc_now() - timedelta(seconds=1),
  )


def _enable_archive(monkeypatch):
  archive.clear_archive_policy_cache()
  monkeypatch.setattr(archive.settings, "OBJECT_ARCHIVE_ENABLED", True)
  monkeypatch.setattr(archive.settings, "OBJECT_ARCHIVE_AUTOCONFIGURE", True)
  monkeypatch.setattr(archive.settings, "REGION", "beijing")


@pytest.mark.asyncio
async def test_archive_expired_object_copies_then_removes_source(monkeypatch):
  _enable_archive(monkeypatch)
  item = _catalog_item()
  # PyMongo returns BSON datetimes without tzinfo by default.
  item.restore_until = item.restore_until.replace(tzinfo=None)
  saved = []

  class Client:
    def __init__(self):
      self.calls = []
      self.archived = False

    def bucket_exists(self, bucket):
      self.calls.append(("bucket_exists", bucket))
      return False

    def make_bucket(self, bucket):
      self.calls.append(("make_bucket", bucket))

    def stat_object(self, bucket, *_args, **_kwargs):
      if bucket == item.bucket:
        return SimpleNamespace(size=16, etag="source-etag")
      if self.archived:
        return SimpleNamespace(size=16, etag="source-etag")
      error = RuntimeError("not archived")
      error.code = "NoSuchKey"
      raise error

    def copy_object(self, bucket, key, source):
      self.calls.append(("copy_object", bucket, key, source.bucket_name, source.object_name, source.version_id))
      self.archived = True
      return SimpleNamespace(etag="source-etag")

    def remove_object(self, bucket, key, version_id=None):
      self.calls.append(("remove_object", bucket, key, version_id))

  client = Client()

  async def claim(app_name, object_id, **_kwargs):
    assert (app_name, object_id) == ("system-test", "obj-archive-1")
    item.state = "archive_pending"
    return item

  async def save(target, **changes):
    saved.append(changes)
    for key, value in changes.items():
      setattr(target, key, value)
    return target

  async def get_client(_item):
    return client

  async def read_current(*_args):
    return item

  class Lock:
    async def __aenter__(self):
      return object()

    async def __aexit__(self, *_args):
      return False

  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", claim)
  monkeypatch.setattr(archive.crud, "save_object", save)
  monkeypatch.setattr(archive.crud, "read_object_by_id", read_current)
  monkeypatch.setattr(archive, "_get_client", get_client)
  monkeypatch.setattr(archive.quota, "application_quota_lock", lambda _app: Lock())
  monkeypatch.setattr(archive.settings, "OBJECT_ARCHIVE_BUCKET", "storagent-expired-archive")

  result = await archive.archive_expired_object(item, now=utc_now())

  assert result == "archived"
  assert item.state == "archived"
  assert item.archive_checksum == "source-etag"
  assert item.archive_id.startswith("storagent-expired-archive/expired/system-test/")
  assert [call[0] for call in client.calls] == [
    "bucket_exists",
    "make_bucket",
    "copy_object",
    "remove_object",
  ]
  assert saved[-1]["archive_after"] is None


@pytest.mark.asyncio
async def test_archive_failure_keeps_source_and_schedules_retry(monkeypatch):
  _enable_archive(monkeypatch)
  item = _catalog_item()
  saved = []

  class Client:
    def bucket_exists(self, _bucket):
      return True

    def stat_object(self, bucket, *_args, **_kwargs):
      if bucket == item.bucket:
        return SimpleNamespace(size=16, etag="source-etag")
      error = RuntimeError("not archived")
      error.code = "NoSuchKey"
      raise error

    def copy_object(self, *_args):
      raise RuntimeError("archive target unavailable")

    def remove_object(self, *_args, **_kwargs):
      pytest.fail("source must not be removed after a failed archive copy")

  async def claim(*_args, **_kwargs):
    item.state = "archive_pending"
    return item

  async def save(target, **changes):
    saved.append(changes)
    for key, value in changes.items():
      setattr(target, key, value)
    return target

  async def get_client(_item):
    return Client()

  async def read_current(*_args):
    return item

  class Lock:
    async def __aenter__(self):
      return object()

    async def __aexit__(self, *_args):
      return False

  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", claim)
  monkeypatch.setattr(archive.crud, "save_object", save)
  monkeypatch.setattr(archive.crud, "read_object_by_id", read_current)
  monkeypatch.setattr(archive, "_get_client", get_client)
  monkeypatch.setattr(archive.quota, "application_quota_lock", lambda _app: Lock())

  result = await archive.archive_expired_object(item, now=utc_now())

  assert result == "failed"
  assert item.state == "archive_failed"
  assert "archive target unavailable" in item.archive_error
  assert saved[-1]["archive_after"] is not None


def test_archive_copy_accepts_minio_rewritten_multipart_etag():
  item = _catalog_item()
  item.etag = "8ac4de77ed13a21e1003acf9b528dca2-1"

  class Client:
    def __init__(self):
      self.archived = False
      self.removed = []

    def bucket_exists(self, _bucket):
      return True

    def stat_object(self, bucket, *_args, **_kwargs):
      if bucket == item.bucket:
        return SimpleNamespace(size=16, etag=item.etag)
      if self.archived:
        return SimpleNamespace(size=16, etag="f8241716cfb99e658e2f1f770faf14d9")
      error = RuntimeError("not archived")
      error.code = "NoSuchKey"
      raise error

    def copy_object(self, *_args, **_kwargs):
      self.archived = True

    def remove_object(self, bucket, key, version_id=None):
      self.removed.append((bucket, key, version_id))

  client = Client()

  checksum = archive._copy_then_remove(
    client,
    item,
    "storagent-expired-archive",
    "expired/system-test/object",
  )

  assert checksum == item.etag
  assert client.removed == [("system-test", "reports/expired.csv", "version-1")]


@pytest.mark.asyncio
async def test_archive_retry_completes_when_source_was_removed_before_catalog_commit(monkeypatch):
  _enable_archive(monkeypatch)
  item = _catalog_item()
  saved = []

  class Client:
    def bucket_exists(self, _bucket):
      return True

    def stat_object(self, bucket, *_args, **_kwargs):
      if bucket == item.bucket:
        error = RuntimeError("source version missing")
        error.code = "NoSuchVersion"
        raise error
      return SimpleNamespace(size=item.size_bytes, etag=item.etag)

    def copy_object(self, *_args, **_kwargs):
      pytest.fail("a verified archive must not be copied again")

    def remove_object(self, *_args, **_kwargs):
      pytest.fail("a missing source must not be removed again")

  async def claim(*_args, **_kwargs):
    item.state = "archive_pending"
    return item

  async def save(target, **changes):
    saved.append(changes)
    for key, value in changes.items():
      setattr(target, key, value)
    return target

  async def read_current(*_args):
    return item

  class Lock:
    async def __aenter__(self):
      return object()

    async def __aexit__(self, *_args):
      return False

  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", claim)
  monkeypatch.setattr(archive.crud, "save_object", save)
  monkeypatch.setattr(archive.crud, "read_object_by_id", read_current)
  async def get_client(_item):
    return Client()

  monkeypatch.setattr(archive, "_get_client", get_client)
  monkeypatch.setattr(archive.quota, "application_quota_lock", lambda _app: Lock())

  result = await archive.archive_expired_object(item, now=utc_now())

  assert result == "archived"
  assert item.state == "archived"
  assert saved[-1]["archive_checksum"] == item.etag


@pytest.mark.asyncio
async def test_archive_pass_counts_each_outcome(monkeypatch):
  _enable_archive(monkeypatch)
  rows = [_catalog_item(), _catalog_item()]
  rows[1].object_id = "obj-archive-2"
  outcomes = iter(["archived", "failed"])

  async def list_rows(*_args, **_kwargs):
    return rows

  async def archive_one(_item, **_kwargs):
    return next(outcomes)

  monkeypatch.setattr(archive.crud, "list_expired_objects_for_archive", list_rows)
  monkeypatch.setattr(archive, "archive_expired_object", archive_one)
  async def consensus():
    return None
  monkeypatch.setattr(archive, "_assert_archive_policy_consensus", consensus)

  result = await archive.archive_expired_objects_once()

  assert result == {"candidates": 2, "archived": 1, "failed": 1, "skipped": 0}


@pytest.mark.asyncio
async def test_archive_disabled_never_claims_candidate(monkeypatch):
  item = _catalog_item()

  async def must_not_claim(*_args, **_kwargs):
    raise AssertionError("disabled archive must not lease a source object")

  monkeypatch.setattr(archive.settings, "OBJECT_ARCHIVE_ENABLED", False)
  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", must_not_claim)

  assert await archive.archive_expired_object(item) == "disabled"


@pytest.mark.asyncio
async def test_archive_skips_foreign_region_before_claim(monkeypatch):
  _enable_archive(monkeypatch)
  item = _catalog_item()
  item.source_region = "shanghai"

  async def must_not_claim(*_args, **_kwargs):
    raise AssertionError("foreign Region must not lease an archive candidate")

  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", must_not_claim)

  assert await archive.archive_expired_object(item) == "skipped"


def test_archive_autoconfigure_handles_minio_missing_lifecycle_response(monkeypatch):
  _enable_archive(monkeypatch)
  configured = []

  class Client:
    def bucket_exists(self, _bucket):
      return True

    def get_bucket_versioning(self, _bucket):
      return SimpleNamespace(status="Enabled")

    def set_bucket_versioning(self, _bucket, _config):
      raise AssertionError("an enabled bucket must not have versioning rewritten")

    def get_bucket_lifecycle(self, _bucket):
      error = RuntimeError("The lifecycle configuration does not exist")
      error.code = "NoSuchLifecycleConfiguration"
      raise error

    def set_bucket_lifecycle(self, bucket, lifecycle):
      configured.append((bucket, lifecycle))

  archive._ensure_archive_bucket_policy(Client(), "storagent-expired-archive")

  assert len(configured) == 1
  rule = configured[0][1].rules[0]
  assert rule.rule_id == archive.ARCHIVE_LIFECYCLE_RULE_ID
  assert rule.status == "Enabled"


@pytest.mark.asyncio
async def test_non_authority_archive_requires_matching_published_policy(monkeypatch):
  _enable_archive(monkeypatch)
  monkeypatch.setattr(archive.settings, "REGION", "tianjin")
  monkeypatch.setattr(archive.settings, "SYNC_AUTHORITY_REGION", "beijing")

  async def pull(_key):
    return {"fingerprint": "different"}

  from src.core import etcd_op
  monkeypatch.setattr(etcd_op, "pull_from_etcd_by_key", pull)

  with pytest.raises(RuntimeError, match="配置不一致"):
    await archive._assert_archive_policy_consensus()
