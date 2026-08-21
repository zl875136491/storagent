"""In-memory CORS origin allowlist shared by every request.

Static origins come from BACKEND_CORS_ORIGINS / FRONT_URL and never hit the
database. Application domains are rebuilt from Etcd application snapshots so
every region sees the same whitelist without querying Mongo on each request.
"""
from __future__ import annotations

import threading
from collections.abc import Iterable, Mapping
from urllib.parse import urlparse

from src.configs.configs import settings

MAX_DOMAINS_PER_APP = 32


def normalize_origin(value: str) -> str:
  """Return a canonical Origin (scheme://host[:port]) or raise ValueError."""
  raw = str(value or "").strip()
  if not raw:
    raise ValueError("来源不能为空")
  parsed = urlparse(raw)
  if parsed.scheme not in ("http", "https") or not parsed.netloc:
    raise ValueError("来源必须是 http(s)://host[:port] 形式的 Origin")
  if parsed.username or parsed.password:
    raise ValueError("来源不能包含用户名或密码")
  if parsed.path not in ("", "/") or parsed.params or parsed.query or parsed.fragment:
    raise ValueError("来源不能包含路径、查询或片段")
  return f"{parsed.scheme}://{parsed.netloc}"


def normalize_origin_list(values: Iterable[str] | None) -> list[str]:
  origins = coerce_origin_list(values, skip_invalid=False)
  if len(origins) > MAX_DOMAINS_PER_APP:
    raise ValueError(f"每个应用最多 {MAX_DOMAINS_PER_APP} 个来源")
  return origins


def coerce_origin_list(
  values: Iterable[str] | None,
  *,
  skip_invalid: bool = True,
) -> list[str]:
  origins: list[str] = []
  seen: set[str] = set()
  for item in values or []:
    try:
      origin = normalize_origin(item)
    except ValueError:
      if skip_invalid:
        continue
      raise
    if origin in seen:
      continue
    seen.add(origin)
    origins.append(origin)
  return origins


def origins_from_application_entries(applications: Mapping[str, object]) -> list[str]:
  origins: list[str] = []
  for entry in applications.values():
    if not isinstance(entry, dict):
      continue
    for item in entry.get("domains") or []:
      try:
        origins.append(normalize_origin(str(item)))
      except ValueError:
        continue
  return origins


class OriginAllowlist:
  """Sequence-like allowlist used by Starlette CORSMiddleware."""

  def __init__(self) -> None:
    self._lock = threading.Lock()
    static = [str(origin).rstrip("/") for origin in settings.BACKEND_CORS_ORIGINS]
    front = settings.FRONT_URL.rstrip("/")
    parsed_front = urlparse(front)
    if parsed_front.scheme in ("http", "https") and parsed_front.netloc:
      if front not in static:
        static.append(front)
    self._static = set(static)
    self._dynamic: set[str] = set()

  def replace_dynamic(self, origins: Iterable[str]) -> None:
    normalized: set[str] = set()
    for item in origins:
      try:
        normalized.add(normalize_origin(item))
      except ValueError:
        continue
    with self._lock:
      self._dynamic = normalized

  def snapshot(self) -> set[str]:
    with self._lock:
      return set(self._static) | set(self._dynamic)

  def __contains__(self, origin: object) -> bool:
    if not isinstance(origin, str):
      return False
    key = origin.rstrip("/")
    with self._lock:
      return key in self._static or key in self._dynamic

  def __iter__(self):
    return iter(self.snapshot())


allowlist = OriginAllowlist()


def refresh_from_application_entries(applications: Mapping[str, object]) -> None:
  allowlist.replace_dynamic(origins_from_application_entries(applications))


async def load_allowlist_from_mongo() -> None:
  from src.modules.public.model import Application

  origins: list[str] = []
  for application in await Application.find_all().to_list():
    origins.extend(getattr(application, "domains", None) or [])
  allowlist.replace_dynamic(origins)
