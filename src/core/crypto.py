"""
Etcd 机密字段加解密：使用 SECRET_KEY 派生 Fernet 密钥。
兼容未加密的历史明文（读取时原样返回）。
"""
import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken

from src.configs.configs import settings

_ENC_PREFIX = "enc:v1:"


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
