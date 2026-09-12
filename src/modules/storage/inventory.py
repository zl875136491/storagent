"""Queryable MinIO object index stored in Mongo, with sliced directory reads."""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import uuid4

from src.configs.configs import settings
from src.core.exception import CustomException, ErrorDesc
from src.core.minio_op import get_minio_client, list_server_object_rows
from src.modules.storage import crud as storage_crud
from src.modules.storage.model import ServerFileInventoryMeta, ServerFileNode
from src.utils.helpers import utc_now

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_INSERT_BATCH = 800
_SEARCH_QUERY_MAX = 128
_DEFAULT_CHILD_LIMIT = 40
_MAX_CHILD_LIMIT = 100
_DEFAULT_SEARCH_PAGE_SIZE = 50
_MAX_SEARCH_PAGE_SIZE = 100
_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

SortKey = Literal["size", "name", "last_modified", "object_key"]
SortOrder = Literal["asc", "desc"]

def _aware_utc(value: datetime) -> datetime:
  return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def parse_inventory_datetime(value: Any) -> datetime:
  if isinstance(value, datetime):
    aware = _aware_utc(value)
    return aware.astimezone(timezone.utc)
  if isinstance(value, str):
    text = value.strip()
    if text:
      normalized = text.replace(" ", "T")
      if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
      try:
        parsed = datetime.fromisoformat(normalized)
      except ValueError:
        parsed = None
      if parsed is not None:
        return _aware_utc(parsed).astimezone(timezone.utc)
  return _EPOCH


def split_object_key(object_key: str) -> list[str]:
  key = str(object_key or "").replace("\\", "/").strip("/")
  if not key:
    return []
  return [part for part in key.split("/") if part]


def _dir_key(bucket: str, parent: str, name: str) -> tuple[str, str, str]:
  return (bucket, parent, name)


@dataclass
class _DirAcc:
  size: int = 0
  object_count: int = 0
  child_files: int = 0
  child_dirs: set[str] = field(default_factory=set)
  last_modified: datetime = field(default_factory=lambda: _EPOCH)


def build_inventory_documents(
  server_id: str,
  generation: str,
  objects: list[dict[str, Any]],
  bucket_created: dict[str, datetime] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """Turn a flat object listing into file/dir rows plus per-bucket totals."""
  dirs: dict[tuple[str, str, str], _DirAcc] = {}
  files: list[dict[str, Any]] = []
  created = bucket_created or {}

  def ensure_dir(bucket: str, parent: str, name: str) -> _DirAcc:
    key = _dir_key(bucket, parent, name)
    acc = dirs.get(key)
    if acc is None:
      acc = _DirAcc()
      dirs[key] = acc
    return acc

  def touch_dir(bucket: str, parent: str, name: str) -> _DirAcc:
    acc = ensure_dir(bucket, parent, name)
    if parent:
      parent_name = parent.rsplit("/", 1)[-1]
      parent_parent = parent.rsplit("/", 1)[0] if "/" in parent else ""
      ensure_dir(bucket, parent_parent, parent_name).child_dirs.add(name)
    return acc

  for raw in objects:
    bucket = str(raw.get("bucket") or "").strip()
    parts = split_object_key(str(raw.get("object_key") or ""))
    if not bucket or not parts:
      continue
    size = max(int(raw.get("size") or 0), 0)
    modified = parse_inventory_datetime(raw.get("last_modified"))
    file_name = parts[-1]
    parent = "/".join(parts[:-1])
    object_key = "/".join(parts)
    files.append({
      "server_id": server_id,
      "generation": generation,
      "bucket": bucket,
      "parent": parent,
      "name": file_name,
      "kind": "file",
      "object_key": object_key,
      "size": size,
      "object_count": 1,
      "child_count": 0,
      "last_modified": modified,
      "name_lower": file_name.lower(),
      "object_key_lower": object_key.lower(),
    })
    if parent:
      parent_name = parent.rsplit("/", 1)[-1]
      parent_parent = parent.rsplit("/", 1)[0] if "/" in parent else ""
      ensure_dir(bucket, parent_parent, parent_name).child_files += 1
    prefix_parts: list[str] = []
    for part in parts[:-1]:
      parent_path = "/".join(prefix_parts)
      acc = touch_dir(bucket, parent_path, part)
      acc.size += size
      acc.object_count += 1
      if modified > acc.last_modified:
        acc.last_modified = modified
      prefix_parts.append(part)

  dir_docs: list[dict[str, Any]] = []
  for (bucket, parent, name), acc in dirs.items():
    object_key = f"{parent}/{name}" if parent else name
    dir_docs.append({
      "server_id": server_id,
      "generation": generation,
      "bucket": bucket,
      "parent": parent,
      "name": name,
      "kind": "dir",
      "object_key": object_key,
      "size": acc.size,
      "object_count": acc.object_count,
      "child_count": acc.child_files + len(acc.child_dirs),
      "last_modified": acc.last_modified,
      "name_lower": name.lower(),
      "object_key_lower": object_key.lower(),
    })

  bucket_stats: dict[str, dict[str, Any]] = {}
  for item in files:
    bucket = item["bucket"]
    stats = bucket_stats.setdefault(bucket, {
      "name": bucket,
      "total_size": 0,
      "object_count": 0,
      "created_at": created.get(bucket, _EPOCH),
      "last_modified": _EPOCH,
    })
    stats["total_size"] += item["size"]
    stats["object_count"] += 1
    if item["last_modified"] > stats["last_modified"]:
      stats["last_modified"] = item["last_modified"]
  for bucket, created_at in created.items():
    bucket_stats.setdefault(bucket, {
      "name": bucket,
      "total_size": 0,
      "object_count": 0,
      "created_at": created_at,
      "last_modified": created_at,
    })
    bucket_stats[bucket]["created_at"] = created_at

  buckets = sorted(bucket_stats.values(), key=lambda item: item["name"])
  return [*dir_docs, *files], buckets


def slice_child_page(
  items: list[dict[str, Any]],
  *,
  offset: int,
  limit: int,
  sort: SortKey = "size",
  order: SortOrder = "desc",
) -> tuple[list[dict[str, Any]], int]:
  reverse = order == "desc"

  def sort_key(item: dict[str, Any]) -> tuple[Any, str]:
    name = str(item.get("name") or "")
    if sort == "name" or sort == "object_key":
      primary: Any = name.lower()
    elif sort == "last_modified":
      primary = parse_inventory_datetime(item.get("last_modified"))
    else:
      primary = int(item.get("size") or 0)
    return (primary, name)

  keyed = sorted(items, key=sort_key, reverse=reverse)
  return keyed[offset:offset + limit], len(keyed)


def normalize_search_query(raw: str | None) -> str:
  text = (raw or "").strip()
  if len(text) > _SEARCH_QUERY_MAX:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "搜索关键字过长")
  return text


def search_regex(query: str) -> str:
  return re.escape(query.lower())


def _ttl_seconds() -> int:
  value = int(getattr(settings, "FILE_INVENTORY_SYNC_INTERVAL_SECONDS", 0) or 0)
  if value <= 0:
    value = int(settings.SERVER_DETAILS_CACHE_TTL_SECONDS)
  return max(value, 1)


def _validate_bucket(bucket: str | None) -> str:
  name = (bucket or "").strip()
  if not name:
    return ""
  if name.lower().startswith("bucket:"):
    name = name.split(":", 1)[1].strip()
  if not _BUCKET_NAME_RE.fullmatch(name) or ".." in name:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "存储桶名称不合法")
  return name


def _validate_prefix(prefix: str | None) -> str:
  value = (prefix or "").replace("\\", "/").strip("/")
  if "\x00" in value or len(value.encode("utf-8")) > 1024:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "目录前缀不合法")
  return value


def _sort_spec(sort: SortKey, order: SortOrder) -> list[tuple[str, int]]:
  direction = -1 if order == "desc" else 1
  if sort == "name":
    return [("name_lower", direction), ("name", 1)]
  if sort == "object_key":
    return [("bucket", 1), ("object_key_lower", direction), ("name", 1)]
  if sort == "last_modified":
    return [("last_modified", direction), ("name", 1)]
  return [("size", direction), ("name", 1)]


def public_node(doc: dict[str, Any]) -> dict[str, Any]:
  return {
    "name": doc.get("name") or "",
    "kind": doc.get("kind") or "file",
    "bucket": doc.get("bucket") or "",
    "object_key": doc.get("object_key") or "",
    "parent": doc.get("parent") or "",
    "size": int(doc.get("size") or 0),
    "object_count": int(doc.get("object_count") or 0),
    "child_count": int(doc.get("child_count") or 0),
    "last_modified": doc.get("last_modified") or _EPOCH,
  }


def public_bucket(item: dict[str, Any]) -> dict[str, Any]:
  created = parse_inventory_datetime(item.get("created_at"))
  return {
    "name": item.get("name") or "",
    "total_size": int(item.get("total_size") or 0),
    "object_count": int(item.get("object_count") or 0),
    "created_at": created,
    "last_modified": parse_inventory_datetime(item.get("last_modified") or created),
    "files": [],
  }


@dataclass(frozen=True)
class InventorySnapshot:
  server_id: str
  generation: str
  buckets: list[dict[str, Any]]
  object_count: int
  total_size: int
  fetched_at: datetime
  expires_at: datetime
  cache_hit: bool


def _meta_to_snapshot(meta: ServerFileInventoryMeta, *, cache_hit: bool) -> InventorySnapshot:
  return InventorySnapshot(
    server_id=meta.server_id,
    generation=meta.generation,
    buckets=list(meta.buckets or []),
    object_count=int(meta.object_count or 0),
    total_size=int(meta.total_size or 0),
    fetched_at=_aware_utc(meta.fetched_at),
    expires_at=_aware_utc(meta.expires_at),
    cache_hit=cache_hit,
  )


async def _read_meta(server_id: str) -> ServerFileInventoryMeta | None:
  return await ServerFileInventoryMeta.find_one(ServerFileInventoryMeta.server_id == server_id)


async def _delete_generation(server_id: str, generation: str | None = None) -> None:
  query: dict[str, Any] = {"server_id": server_id}
  if generation is not None:
    query["generation"] = generation
  await ServerFileNode.get_motor_collection().delete_many(query)


async def _write_inventory(
  server_id: str,
  documents: list[dict[str, Any]],
  buckets: list[dict[str, Any]],
  fetched_at: datetime,
  expires_at: datetime,
  generation: str,
) -> InventorySnapshot:
  collection = ServerFileNode.get_motor_collection()
  for offset in range(0, len(documents), _INSERT_BATCH):
    batch = documents[offset:offset + _INSERT_BATCH]
    if batch:
      await collection.insert_many(batch, ordered=False)
  meta = ServerFileInventoryMeta(
    server_id=server_id,
    generation=generation,
    buckets=buckets,
    object_count=sum(int(item.get("object_count") or 0) for item in buckets),
    total_size=sum(int(item.get("total_size") or 0) for item in buckets),
    fetched_at=fetched_at,
    expires_at=expires_at,
  )
  await ServerFileInventoryMeta.get_motor_collection().replace_one(
    {"server_id": server_id},
    {
      "server_id": server_id,
      "generation": generation,
      "buckets": buckets,
      "object_count": meta.object_count,
      "total_size": meta.total_size,
      "fetched_at": fetched_at,
      "expires_at": expires_at,
    },
    upsert=True,
  )
  await collection.delete_many({
    "server_id": server_id,
    "generation": {"$ne": generation},
  })
  stored = await _read_meta(server_id)
  if stored is None:
    raise CustomException(ErrorDesc.SYNC_FAILED, "对象索引写入后无法读取")
  return _meta_to_snapshot(stored, cache_hit=False)


async def _sync_from_minio(server: Any) -> InventorySnapshot:
  server_id = str(server.id)
  access_key, secret_key = storage_crud.plain_minio_credentials(server)
  client = get_minio_client(
    host=server.host,
    port=server.minio_port,
    access_key=access_key,
    secret_key=secret_key,
  )
  bucket_rows, object_rows = await list_server_object_rows(client)
  generation = uuid4().hex
  fetched_at = utc_now()
  expires_at = fetched_at + timedelta(seconds=_ttl_seconds())
  created = {
    str(item["name"]): parse_inventory_datetime(item.get("created_at"))
    for item in bucket_rows
    if item.get("name")
  }
  documents, buckets = build_inventory_documents(
    server_id,
    generation,
    object_rows,
    bucket_created=created,
  )
  return await _write_inventory(
    server_id,
    documents,
    buckets,
    fetched_at,
    expires_at,
    generation,
  )


def _empty_snapshot(server_id: str) -> InventorySnapshot:
  return InventorySnapshot(
    server_id=server_id,
    generation="",
    buckets=[],
    object_count=0,
    total_size=0,
    fetched_at=_EPOCH,
    expires_at=_EPOCH,
    cache_hit=True,
  )


async def ensure_server_file_inventory(server: Any) -> InventorySnapshot:
  """Read the Mongo index only. MinIO listing is a Celery job."""
  server_id = str(server.id)
  meta = await _read_meta(server_id)
  if meta is None:
    return _empty_snapshot(server_id)
  return _meta_to_snapshot(meta, cache_hit=True)


def _index_fields(snapshot: InventorySnapshot) -> dict[str, Any]:
  ready = bool(snapshot.generation)
  return {
    "cache_hit": snapshot.cache_hit,
    "index_ready": ready,
    "cached_at": snapshot.fetched_at,
    "expires_at": snapshot.expires_at,
    "ttl_seconds": _ttl_seconds(),
  }


async def inventory_summary(server: Any) -> dict[str, Any]:
  snapshot = await ensure_server_file_inventory(server)
  buckets = [public_bucket(item) for item in snapshot.buckets]
  return {
    "data": buckets,
    "object_count": snapshot.object_count,
    "total_size": snapshot.total_size,
    **_index_fields(snapshot),
  }


def _bucket_child_nodes(snapshot: InventorySnapshot, offset: int, limit: int, sort: SortKey, order: SortOrder) -> tuple[list[dict[str, Any]], int]:
  items = []
  for bucket in snapshot.buckets:
    created = parse_inventory_datetime(bucket.get("created_at"))
    items.append({
      "name": bucket.get("name") or "",
      "kind": "dir",
      "bucket": bucket.get("name") or "",
      "object_key": "",
      "parent": "",
      "size": int(bucket.get("total_size") or 0),
      "object_count": int(bucket.get("object_count") or 0),
      "child_count": int(bucket.get("object_count") or 0),
      "last_modified": parse_inventory_datetime(bucket.get("last_modified") or created),
    })
  page, total = slice_child_page(items, offset=offset, limit=limit, sort=sort, order=order)
  return page, total


async def list_inventory_children(
  server: Any,
  *,
  bucket: str | None = None,
  prefix: str | None = None,
  offset: int = 0,
  limit: int = _DEFAULT_CHILD_LIMIT,
  sort: SortKey = "size",
  order: SortOrder = "desc",
) -> dict[str, Any]:
  snapshot = await ensure_server_file_inventory(server)
  bucket_name = _validate_bucket(bucket)
  parent = _validate_prefix(prefix)
  offset = max(int(offset), 0)
  limit = min(max(int(limit), 1), _MAX_CHILD_LIMIT)
  if sort not in {"size", "name", "last_modified", "object_key"}:
    sort = "size"
  if order not in {"asc", "desc"}:
    order = "desc"
  if parent and not bucket_name:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "列出子目录时必须指定存储桶")

  if not bucket_name:
    items, total = _bucket_child_nodes(snapshot, offset, limit, sort, order)
    parent_size = snapshot.total_size
    parent_count = snapshot.object_count
    parent_children = total
  else:
    query = {
      "server_id": snapshot.server_id,
      "generation": snapshot.generation,
      "bucket": bucket_name,
      "parent": parent,
    }
    collection = ServerFileNode.get_motor_collection()
    total = int(await collection.count_documents(query))
    cursor = collection.find(query).sort(_sort_spec(sort, order)).skip(offset).limit(limit)
    docs = await cursor.to_list(length=limit)
    items = [public_node(doc) for doc in docs]
    if parent:
      parent_doc = await collection.find_one({
        "server_id": snapshot.server_id,
        "generation": snapshot.generation,
        "bucket": bucket_name,
        "kind": "dir",
        "object_key": parent,
      })
      parent_size = int((parent_doc or {}).get("size") or 0)
      parent_count = int((parent_doc or {}).get("object_count") or 0)
      parent_children = int((parent_doc or {}).get("child_count") or total)
    else:
      bucket_info = next((item for item in snapshot.buckets if item.get("name") == bucket_name), None)
      parent_size = int((bucket_info or {}).get("total_size") or 0)
      parent_count = int((bucket_info or {}).get("object_count") or 0)
      parent_children = total

  loaded = offset + len(items)
  remaining_count = max(total - loaded, 0)
  page_size_sum = sum(int(item.get("size") or 0) for item in items)
  return {
    "bucket": bucket_name,
    "prefix": parent,
    "items": items,
    "offset": offset,
    "limit": limit,
    "total": total,
    "has_more": remaining_count > 0,
    "remaining_count": remaining_count,
    "page_size_sum": page_size_sum,
    "parent_size": parent_size,
    "parent_object_count": parent_count,
    "parent_child_count": parent_children,
    **_index_fields(snapshot),
  }


async def search_inventory(
  server: Any,
  *,
  query: str | None = None,
  bucket: str | None = None,
  page: int = 1,
  page_size: int = _DEFAULT_SEARCH_PAGE_SIZE,
  sort: SortKey = "object_key",
  order: SortOrder = "asc",
) -> dict[str, Any]:
  snapshot = await ensure_server_file_inventory(server)
  bucket_name = _validate_bucket(bucket)
  needle = normalize_search_query(query)
  page = max(int(page), 1)
  page_size = min(max(int(page_size), 1), _MAX_SEARCH_PAGE_SIZE)
  match: dict[str, Any] = {
    "server_id": snapshot.server_id,
    "generation": snapshot.generation,
    "kind": "file",
  }
  if bucket_name:
    match["bucket"] = bucket_name
  if needle:
    pattern = search_regex(needle)
    match["$or"] = [
      {"name_lower": {"$regex": pattern}},
      {"object_key_lower": {"$regex": pattern}},
      {"bucket": {"$regex": pattern}},
    ]
  sort_key: SortKey = sort if sort in {"size", "name", "last_modified", "object_key"} else "object_key"
  collection = ServerFileNode.get_motor_collection()
  total = int(await collection.count_documents(match))
  page_count = max(1, (total + page_size - 1) // page_size) if total else 1
  page = min(page, page_count)
  skip = (page - 1) * page_size
  cursor = collection.find(match).sort(_sort_spec(sort_key, order)).skip(skip).limit(page_size)
  if hasattr(cursor, "allow_disk_use"):
    cursor = cursor.allow_disk_use(True)
  docs = await cursor.to_list(length=page_size)
  return {
    "q": needle,
    "bucket": bucket_name,
    "items": [public_node(doc) for doc in docs],
    "page": page,
    "page_size": page_size,
    "total": total,
    "page_count": page_count,
    **_index_fields(snapshot),
  }


async def find_inventory_file(server: Any, bucket: str, object_key: str) -> dict[str, Any] | None:
  snapshot = await ensure_server_file_inventory(server)
  parts = split_object_key(object_key)
  if not parts:
    return None
  doc = await ServerFileNode.get_motor_collection().find_one({
    "server_id": snapshot.server_id,
    "generation": snapshot.generation,
    "bucket": bucket,
    "kind": "file",
    "object_key": "/".join(parts),
  })
  return public_node(doc) if doc else None
