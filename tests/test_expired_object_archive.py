from types import SimpleNamespace

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
    etag="source-etag",
    source_region="beijing",
    deletion_generation=2,
    state="soft_deleted",
  )


@pytest.mark.asyncio
async def test_archive_expired_object_copies_then_removes_source(monkeypatch):
  item = _catalog_item()
  saved = []

  class Client:
    def __init__(self):
      self.calls = []

    def bucket_exists(self, bucket):
      self.calls.append(("bucket_exists", bucket))
      return False

    def make_bucket(self, bucket):
      self.calls.append(("make_bucket", bucket))

    def stat_object(self, *_args):
      raise RuntimeError("not archived")

    def copy_object(self, bucket, key, source):
      self.calls.append(("copy_object", bucket, key, source.bucket_name, source.object_name, source.version_id))
      return SimpleNamespace(etag="archive-etag")

    def remove_object(self, bucket, key):
      self.calls.append(("remove_object", bucket, key))

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

  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", claim)
  monkeypatch.setattr(archive.crud, "save_object", save)
  monkeypatch.setattr(archive, "_get_client", get_client)
  monkeypatch.setattr(archive.settings, "OBJECT_ARCHIVE_BUCKET", "storagent-expired-archive")

  result = await archive.archive_expired_object(item, now=utc_now())

  assert result == "archived"
  assert item.state == "archived"
  assert item.archive_checksum == "archive-etag"
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
  item = _catalog_item()
  saved = []

  class Client:
    def bucket_exists(self, _bucket):
      return True

    def stat_object(self, *_args):
      raise RuntimeError("not archived")

    def copy_object(self, *_args):
      raise RuntimeError("archive target unavailable")

    def remove_object(self, *_args):
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

  monkeypatch.setattr(archive.crud, "claim_expired_object_for_archive", claim)
  monkeypatch.setattr(archive.crud, "save_object", save)
  monkeypatch.setattr(archive, "_get_client", get_client)

  result = await archive.archive_expired_object(item, now=utc_now())

  assert result == "failed"
  assert item.state == "archive_failed"
  assert "archive target unavailable" in item.archive_error
  assert saved[-1]["archive_after"] is not None


@pytest.mark.asyncio
async def test_archive_pass_counts_each_outcome(monkeypatch):
  rows = [_catalog_item(), _catalog_item()]
  rows[1].object_id = "obj-archive-2"
  outcomes = iter(["archived", "failed"])

  async def list_rows(*_args, **_kwargs):
    return rows

  async def archive_one(_item, **_kwargs):
    return next(outcomes)

  monkeypatch.setattr(archive.crud, "list_expired_objects_for_archive", list_rows)
  monkeypatch.setattr(archive, "archive_expired_object", archive_one)

  result = await archive.archive_expired_objects_once()

  assert result == {"candidates": 2, "archived": 1, "failed": 1, "skipped": 0}
