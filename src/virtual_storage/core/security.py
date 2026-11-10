"""管理凭据与租户 Key 的哈希、交付加密和会话票据。明文不写审计。"""
from __future__ import annotations

import hashlib
import hmac
import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import timedelta

import bcrypt

from src.core.crypto import decrypt_secret, encrypt_secret
from src.utils.helpers import utc_now
from src.virtual_storage.settings import get_settings


def hash_management_credential(secret: str) -> str:
  return bcrypt.hashpw(secret.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_management_credential(secret: str, hashed: str) -> bool:
  if not secret or not hashed:
    return False
  try:
    return bcrypt.checkpw(secret.encode("utf-8"), hashed.encode("utf-8"))
  except ValueError:
    return False


def subject_hash(secret: str) -> str:
  return hashlib.sha256(b"vs-subject:" + secret.encode("utf-8")).hexdigest()


def hash_tenant_key(secret: str) -> str:
  return hashlib.sha256(b"vs-tenant-key:" + secret.encode("utf-8")).hexdigest()


def hint(secret: str) -> str:
  return "…" + secret[-4:]


def encrypt_delivery(secret: str) -> str:
  return encrypt_secret(secret)


def decrypt_delivery(value: str) -> str:
  return decrypt_secret(value)


def request_fingerprint(payload: dict) -> str:
  encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
  return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sign(body: bytes) -> str:
  from src.configs.configs import settings as app_settings
  digest = hmac.new(app_settings.SECRET_KEY.encode("utf-8"), body, hashlib.sha256).digest()
  return urlsafe_b64encode(digest).decode("ascii")


def issue_session(app_name: str, subject: str, hours: int = 8) -> str:
  payload = {
    "app_name": app_name,
    "subject": subject,
    "exp": int((utc_now() + timedelta(hours=hours)).timestamp()),
  }
  body = urlsafe_b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
  return body.decode("ascii") + "." + _sign(body)


def read_session(token: str) -> dict | None:
  if not token or "." not in token:
    return None
  body, signature = token.split(".", 1)
  if not hmac.compare_digest(signature, _sign(body.encode("ascii"))):
    return None
  try:
    payload = json.loads(urlsafe_b64decode(body.encode("ascii")))
  except (ValueError, json.JSONDecodeError):
    return None
  if int(payload.get("exp", 0)) < int(utc_now().timestamp()):
    return None
  return payload


def queue_name() -> str:
  from src.core.celery_routing import task_queue_name
  settings = get_settings()
  return task_queue_name(
    settings.region,
    queue_prefix=settings.queue_prefix,
    protocol_version=settings.protocol_version,
  )
