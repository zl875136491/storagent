"""Etcd 机密加解密与 API Key 哈希键。"""
from src.core.crypto import (
  api_key_etcd_map_key,
  decrypt_secret,
  decrypt_server_entry,
  encrypt_secret,
  encrypt_server_entry,
)


def test_encrypt_decrypt_roundtrip(monkeypatch):
  monkeypatch.setattr("src.core.crypto.settings.SECRET_KEY", "unit-test-secret-key")
  plain = "minio-secret-value"
  enc = encrypt_secret(plain)
  assert enc.startswith("enc:v1:")
  assert enc != plain
  assert decrypt_secret(enc) == plain


def test_encrypt_idempotent(monkeypatch):
  monkeypatch.setattr("src.core.crypto.settings.SECRET_KEY", "unit-test-secret-key")
  once = encrypt_secret("abc")
  twice = encrypt_secret(once)
  assert once == twice


def test_decrypt_plaintext_passthrough(monkeypatch):
  monkeypatch.setattr("src.core.crypto.settings.SECRET_KEY", "unit-test-secret-key")
  assert decrypt_secret("legacy-plain") == "legacy-plain"
  assert decrypt_secret("") == ""


def test_api_key_map_key_is_sha256():
  key = "sk-test-key"
  hashed = api_key_etcd_map_key(key)
  assert len(hashed) == 64
  assert hashed != key
  assert hashed == api_key_etcd_map_key(key)


def test_server_entry_encrypt_decrypt(monkeypatch):
  monkeypatch.setattr("src.core.crypto.settings.SECRET_KEY", "unit-test-secret-key")
  raw = {"name": "s1", "access_key": "ak", "secret_key": "sk"}
  enc = encrypt_server_entry(raw)
  assert enc["access_key"].startswith("enc:v1:")
  assert enc["secret_key"].startswith("enc:v1:")
  assert enc["name"] == "s1"
  dec = decrypt_server_entry(enc)
  assert dec["access_key"] == "ak"
  assert dec["secret_key"] == "sk"


def test_redact_mc_alias_set():
  from src.core.crypto import redact_shell_command
  cmd = "mc alias set site1 http://1.2.3.4:9000 admin SuperSecret"
  out = redact_shell_command(cmd)
  assert "SuperSecret" not in out
  assert "admin" not in out or "***" in out
  assert out.startswith("mc alias set site1 http://1.2.3.4:9000")
  assert "*** ***" in out


def test_redact_leaves_other_commands():
  from src.core.crypto import redact_shell_command
  cmd = "mc ls site1/bucket"
  assert redact_shell_command(cmd) == cmd
