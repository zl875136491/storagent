"""API Key 哈希 / 提示 与 token hash 工具。"""
from src.core.crypto import (
  api_key_etcd_map_key,
  api_key_hint,
  is_sha256_hex,
  token_sha256,
)


def test_api_key_hash_and_hint():
  plain = "sk_test_abcdefghijklmnop"
  hashed = api_key_etcd_map_key(plain)
  assert is_sha256_hex(hashed)
  assert hashed != plain
  hint = api_key_hint(plain)
  assert plain not in hint
  assert hint.startswith(plain[:7])


def test_token_sha256_stable():
  t = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig"
  assert token_sha256(t) == token_sha256(t)
  assert is_sha256_hex(token_sha256(t))
