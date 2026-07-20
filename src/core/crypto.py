"""
Etcd / Mongo 机密字段加解密：使用 SECRET_KEY 派生 Fernet 密钥。
兼容未加密的历史明文（读取时原样返回）。
"""
import base64
import hashlib
import re
from cryptography.fernet import Fernet, InvalidToken

from src.configs.configs import settings

_ENC_PREFIX = "enc:v1:"

# mc alias set <name> <url> <user> <password>
_MC_ALIAS_SET_RE = re.compile(
  r"(mc\s+alias\s+set\s+\S+\s+\S+)\s+\S+\s+\S+",
  re.IGNORECASE,
)


def _fernet() -> Fernet:
  digest = hashlib.sha256(settings.SECRET_KEY.encode("utf-8")).digest()
  return Fernet(base64.urlsafe_b64encode(digest))


def encrypt_secret(plain: str) -> str:
  if not plain:
    return plain
  if plain.startswith(_ENC_PREFIX):
    return plain
  token = _fernet().encrypt(plain.encode("utf-8")).decode("utf-8")
  return f"{_ENC_PREFIX}{token}"


def decrypt_secret(value: str) -> str:
  if not value:
    return value
  if not value.startswith(_ENC_PREFIX):
    return value
  token = value[len(_ENC_PREFIX):]
  try:
    return _fernet().decrypt(token.encode("utf-8")).decode("utf-8")
  except InvalidToken as e:
    raise ValueError("Etcd 机密解密失败，请确认各节点 SECRET_KEY 一致") from e


def api_key_etcd_map_key(api_key: str) -> str:
  """Etcd 字典键使用哈希，避免明文 Key 出现在 key 名中。"""
  return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def decrypt_server_entry(server_data: dict) -> dict:
  """解密 servers 条目中的 access_key / secret_key（返回副本）。"""
  data = dict(server_data)
  if "access_key" in data:
    data["access_key"] = decrypt_secret(data["access_key"])
  if "secret_key" in data:
    data["secret_key"] = decrypt_secret(data["secret_key"])
  return data


def encrypt_server_entry(server_data: dict) -> dict:
  """加密 servers 条目中的 access_key / secret_key（返回副本）。"""
  data = dict(server_data)
  if "access_key" in data:
    data["access_key"] = encrypt_secret(data["access_key"])
  if "secret_key" in data:
    data["secret_key"] = encrypt_secret(data["secret_key"])
  return data


def redact_shell_command(command: str) -> str:
  """脱敏 shell 日志中的账号密码（如 mc alias set）。"""
  if not command:
    return command
  redacted, n = _MC_ALIAS_SET_RE.subn(r"\1 *** ***", command)
  if n:
    return redacted
  return command


def api_key_hint(plain_key: str) -> str:
  if len(plain_key) <= 12:
    return "************"
  return f"{plain_key[:7]}************{plain_key[-4:]}"


def is_sha256_hex(value: str) -> bool:
  return len(value) == 64 and all(c in "0123456789abcdef" for c in value.lower())


def token_sha256(token: str) -> str:
  return hashlib.sha256(token.encode("utf-8")).hexdigest()


def minio_server_plain_credentials(access_key: str, secret_key: str) -> tuple[str, str]:
  """从可能加密的字段得到明文凭证。"""
  return decrypt_secret(access_key), decrypt_secret(secret_key)
