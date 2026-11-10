"""虚拟存储运行配置。只读取 VS_*，不回落到旧库连接。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _bool(name: str, default: bool) -> bool:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    return default
  return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int) -> int:
  value = os.getenv(name)
  if value is None or value.strip() == "":
    return default
  return int(value)


@dataclass(frozen=True)
class VSSettings:
  region: str
  home_region: str
  home_base_url: str
  mongo_url: str
  mongo_db: str
  minio_endpoint: str
  minio_access_key: str
  minio_secret_key: str
  minio_secure: bool
  minio_bucket: str
  require_https: bool
  delivery_window_seconds: int
  key_ttl_seconds: int
  lease_seconds: int
  bootstrap: bool
  bootstrap_app_name: str
  bootstrap_display_name: str
  bootstrap_credential: str
  bootstrap_quota_bytes: int
  bootstrap_quota_objects: int
  queue_prefix: str
  protocol_version: str
  celery_broker_url: str
  port: int


_cached: VSSettings | None = None


def get_settings() -> VSSettings:
  global _cached
  if _cached is None:
    mongo_url = os.getenv("VS_MONGO_URL", "").strip()
    if not mongo_url:
      raise RuntimeError("VS_MONGO_URL 未设置，拒绝回落到共享 MongoDB")
    region = os.getenv("VS_REGION", os.getenv("REGION", "")).strip().lower()
    if not region:
      raise RuntimeError("VS_REGION 未设置")
    _cached = VSSettings(
      region=region,
      home_region=os.getenv("VS_HOME_REGION", region).strip().lower(),
      home_base_url=os.getenv("VS_HOME_BASE_URL", "").strip().rstrip("/"),
      mongo_url=mongo_url,
      mongo_db=os.getenv("VS_MONGO_DB", "storagent_virtual_storage").strip(),
      minio_endpoint=os.getenv("VS_MINIO_ENDPOINT", "").strip(),
      minio_access_key=os.getenv("VS_MINIO_ACCESS_KEY", "").strip(),
      minio_secret_key=os.getenv("VS_MINIO_SECRET_KEY", "").strip(),
      minio_secure=_bool("VS_MINIO_SECURE", False),
      minio_bucket=os.getenv("VS_MINIO_BUCKET", "vs-pool-v1").strip(),
      require_https=_bool("VS_REQUIRE_HTTPS", True),
      delivery_window_seconds=_int("VS_DELIVERY_WINDOW_SECONDS", 600),
      key_ttl_seconds=_int("VS_KEY_TTL_SECONDS", 30 * 24 * 3600),
      lease_seconds=_int("VS_LEASE_SECONDS", 300),
      bootstrap=_bool("VS_BOOTSTRAP", False),
      bootstrap_app_name=os.getenv("VS_BOOTSTRAP_APP_NAME", "").strip(),
      bootstrap_display_name=os.getenv("VS_BOOTSTRAP_DISPLAY_NAME", "虚拟存储账号").strip(),
      bootstrap_credential=os.getenv("VS_BOOTSTRAP_CREDENTIAL", ""),
      bootstrap_quota_bytes=_int("VS_BOOTSTRAP_QUOTA_BYTES", 10 * 1024 ** 3),
      bootstrap_quota_objects=_int("VS_BOOTSTRAP_QUOTA_OBJECTS", 100000),
      queue_prefix=os.getenv("VS_QUEUE_PREFIX", "storagent-vs").strip(),
      protocol_version=os.getenv("VS_PROTOCOL_VERSION", "1").strip(),
      celery_broker_url=os.getenv("VS_CELERY_BROKER_URL", "").strip(),
      port=_int("VS_PORT", 8443),
    )
  return _cached


def reset_settings_cache() -> None:
  global _cached
  _cached = None
