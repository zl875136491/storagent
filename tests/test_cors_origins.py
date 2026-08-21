"""In-memory CORS origin allowlist and application domain CRUD."""
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from src.core import sync as sync_module
from src.core.cors_origins import (
  MAX_DOMAINS_PER_APP,
  allowlist,
  normalize_origin,
  normalize_origin_list,
  refresh_from_application_entries,
)
from src.core.exception import CustomException, ErrorDesc
from src.modules.public import service as public_service
from src.modules.public.model import DEFAULT_APPLICATION_QUOTA_BYTES


def _restore_dynamic(previous):
  allowlist.replace_dynamic(previous)


def _mongo_application(*, name="demo", domains=None, **overrides):
  values = dict(
    id=f"id-{name}",
    name=name,
    shown_name=name,
    description="",
    enabled=False,
    provisioning_status="pending",
    provisioning_error="",
    provisioning_updated_at=None,
    quota_bytes=DEFAULT_APPLICATION_QUOTA_BYTES,
    author=SimpleNamespace(id="u1", username="alice", name="Alice"),
    approver=None,
    enabled_at=None,
    updated_at=datetime(2026, 8, 1, tzinfo=timezone.utc),
    domains=list(domains or []),
  )
  values.update(overrides)
  return SimpleNamespace(**values)


def _patch_domain_crud(monkeypatch, *, app, applications, actor=None):
  actor = actor or SimpleNamespace(id="u1", username="alice", permissions=[], roles=[])

  async def read_app(_application_id):
    return app

  async def pull(_key, client=None):
    del client
    return applications

  async def merge(_key, mutator, client=None):
    del client
    mutator(applications)
    return applications

  async def project(_name, entry):
    app.domains = list(entry.get("domains") or [])
    return app, True

  async def response(application, **_kwargs):
    return {"name": application.name, "domains": list(application.domains)}

  async def is_superadmin(_user):
    return True

  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_app)
  monkeypatch.setattr("src.core.etcd_op.pull_from_etcd_by_key", pull)
  monkeypatch.setattr("src.core.etcd_op.merge_update_etcd_key", merge)
  monkeypatch.setattr(public_service.sync_module, "upsert_application_from_etcd", project)
  monkeypatch.setattr(public_service, "_application_response", response)
  monkeypatch.setattr("src.core.auth._is_superadmin", is_superadmin)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)
  return actor


def test_normalize_origin_canonicalizes_trailing_slash():
  assert normalize_origin("https://app.example.com/") == "https://app.example.com"
  assert normalize_origin(" http://10.0.0.1:3001 ") == "http://10.0.0.1:3001"


def test_normalize_origin_rejects_path_and_credentials():
  with pytest.raises(ValueError, match="路径"):
    normalize_origin("https://app.example.com/api")
  with pytest.raises(ValueError, match="用户名"):
    normalize_origin("https://user:pass@app.example.com")
  with pytest.raises(ValueError, match="不能为空"):
    normalize_origin("  ")


def test_normalize_origin_list_deduplicates_and_caps():
  assert normalize_origin_list([
    "https://a.example.com/",
    "https://a.example.com",
    "https://b.example.com",
  ]) == ["https://a.example.com", "https://b.example.com"]
  with pytest.raises(ValueError, match="最多"):
    normalize_origin_list([
      f"https://app{index}.example.com" for index in range(MAX_DOMAINS_PER_APP + 1)
    ])


def test_allowlist_lookup_is_in_memory_only():
  previous = set(allowlist._dynamic)
  try:
    allowlist.replace_dynamic(["https://app.example.com/"])
    assert "https://app.example.com" in allowlist
    assert "https://app.example.com/" in allowlist
    assert "https://other.example.com" not in allowlist
  finally:
    _restore_dynamic(previous)


def test_refresh_rebuilds_dynamic_origins_from_full_application_map():
  previous = set(allowlist._dynamic)
  try:
    refresh_from_application_entries({
      "alpha": {"domains": ["https://shared.example.com", "https://alpha.example.com"]},
      "beta": {"domains": ["https://shared.example.com"]},
    })
    assert "https://shared.example.com" in allowlist
    assert "https://alpha.example.com" in allowlist
    refresh_from_application_entries({
      "beta": {"domains": ["https://shared.example.com"]},
    })
    assert "https://shared.example.com" in allowlist
    assert "https://alpha.example.com" not in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_sync_applications_to_mongo_refreshes_allowlist(monkeypatch):
  previous = set(allowlist._dynamic)
  upserts = []

  async def upsert(name, data):
    upserts.append((name, data))
    return SimpleNamespace(name=name), True

  class _Query:
    async def to_list(self):
      return []

  class Application:
    @staticmethod
    def find_all():
      return _Query()

  monkeypatch.setattr(sync_module, "upsert_application_from_etcd", upsert)
  monkeypatch.setattr("src.modules.public.model.Application", Application)
  try:
    await sync_module.sync_applications_to_mongo({
      "demo": {
        "shown_name": "Demo",
        "domains": ["https://app.example.com/", "not-an-origin"],
      },
    })
    assert upserts[0][1]["domains"] == ["https://app.example.com"]
    assert "https://app.example.com" in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_add_and_delete_application_domain_updates_etcd_and_allowlist(monkeypatch):
  previous = set(allowlist._dynamic)
  applications = {
    "demo": {
      "shown_name": "Demo",
      "quota_bytes": 200 * 1024 ** 3,
      "author_username": "bob",
      "domains": ["https://a.example.com"],
    },
  }
  app = _mongo_application(domains=["https://a.example.com"])
  actor = _patch_domain_crud(monkeypatch, app=app, applications=applications)

  try:
    added = await public_service.add_application_domain(
      "id-demo",
      "https://b.example.com/",
      actor,
    )
    assert added["domains"] == ["https://a.example.com", "https://b.example.com"]
    assert applications["demo"]["domains"] == [
      "https://a.example.com",
      "https://b.example.com",
    ]
    assert applications["demo"]["quota_bytes"] == 200 * 1024 ** 3
    assert applications["demo"]["author_username"] == "bob"
    assert "https://b.example.com" in allowlist

    removed = await public_service.delete_application_domain(
      "id-demo",
      "https://a.example.com",
      actor,
    )
    assert removed["domains"] == ["https://b.example.com"]
    assert "https://a.example.com" not in allowlist
    assert "https://b.example.com" in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_add_application_domain_upserts_when_etcd_map_is_empty(monkeypatch):
  previous = set(allowlist._dynamic)
  applications = {}
  app = _mongo_application(domains=[])
  actor = _patch_domain_crud(monkeypatch, app=app, applications=applications)

  try:
    added = await public_service.add_application_domain(
      "id-demo",
      "https://legacy.example.com/",
      actor,
    )
    assert added["domains"] == ["https://legacy.example.com"]
    entry = applications["demo"]
    assert entry["domains"] == ["https://legacy.example.com"]
    assert entry["shown_name"] == "demo"
    assert entry["author_username"] == "alice"
    assert entry["quota_bytes"] == DEFAULT_APPLICATION_QUOTA_BYTES
    assert "https://legacy.example.com" in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_delete_application_domain_upserts_when_app_missing_from_etcd(monkeypatch):
  previous = set(allowlist._dynamic)
  applications = {}
  app = _mongo_application(domains=["https://legacy.example.com"])
  actor = _patch_domain_crud(monkeypatch, app=app, applications=applications)

  try:
    removed = await public_service.delete_application_domain(
      "id-demo",
      "https://legacy.example.com",
      actor,
    )
    assert removed["domains"] == []
    assert applications["demo"]["domains"] == []
    assert applications["demo"]["author_username"] == "alice"
    assert "https://legacy.example.com" not in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_add_application_domain_keeps_shared_origin_in_allowlist(monkeypatch):
  previous = set(allowlist._dynamic)
  applications = {
    "keep": {"domains": ["https://shared.example.com"]},
  }
  app = _mongo_application(domains=[])
  actor = _patch_domain_crud(monkeypatch, app=app, applications=applications)

  try:
    refresh_from_application_entries(applications)
    added = await public_service.add_application_domain(
      "id-demo",
      "https://shared.example.com",
      actor,
    )
    assert added["domains"] == ["https://shared.example.com"]
    assert applications["keep"]["domains"] == ["https://shared.example.com"]
    assert applications["demo"]["domains"] == ["https://shared.example.com"]
    assert "https://shared.example.com" in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_delete_application_unpublishes_and_refreshes_allowlist(monkeypatch):
  previous = set(allowlist._dynamic)
  applications = {
    "keep": {"domains": ["https://keep.example.com"]},
    "demo": {"domains": ["https://demo.example.com"]},
  }
  app = SimpleNamespace(
    id="id-demo",
    name="demo",
    author=SimpleNamespace(id="u1", username="alice"),
  )
  actor = SimpleNamespace(id="u1", username="alice", permissions=[], roles=[])
  deleted = []

  async def read_app(_application_id):
    return app

  async def merge(_key, mutator, client=None):
    del client
    mutator(applications)
    return applications

  async def delete_local(_application_id):
    deleted.append(_application_id)
    return True

  async def is_superadmin(_user):
    return True

  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_app)
  monkeypatch.setattr(public_service.public_crud, "delete_application_by_id", delete_local)
  monkeypatch.setattr("src.core.etcd_op.merge_update_etcd_key", merge)
  monkeypatch.setattr("src.core.auth._is_superadmin", is_superadmin)
  monkeypatch.setattr("src.core.audit.audit", lambda *_args, **_kwargs: None)

  try:
    refresh_from_application_entries(applications)
    assert "https://demo.example.com" in allowlist
    result = await public_service.delete_application("id-demo", actor)
    assert result["message"] == "应用已删除"
    assert "demo" not in applications
    assert deleted == ["id-demo"]
    assert "https://demo.example.com" not in allowlist
    assert "https://keep.example.com" in allowlist
  finally:
    _restore_dynamic(previous)


@pytest.mark.asyncio
async def test_application_domain_rejects_unrelated_user(monkeypatch):
  app = SimpleNamespace(
    id="id-demo",
    name="demo",
    author=SimpleNamespace(id="u1", username="alice"),
  )
  actor = SimpleNamespace(id="u2", username="bob", permissions=[], roles=[])

  async def read_app(_application_id):
    return app

  async def is_superadmin(_user):
    return False

  monkeypatch.setattr(public_service.public_crud, "read_application_by_id", read_app)
  monkeypatch.setattr("src.core.auth._is_superadmin", is_superadmin)

  with pytest.raises(CustomException) as error:
    await public_service.add_application_domain(
      "id-demo",
      "https://app.example.com",
      actor,
    )
  assert error.value.code == ErrorDesc.INSUFFICIENT_PERMISSIONS.code
