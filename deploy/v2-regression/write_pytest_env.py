"""Build a pytest env file from the running API env. Does not print values."""
import os
from pathlib import Path

source = {}
for line in Path("/opt/storagent-vs/env/api.env").read_text().splitlines():
  if not line or line.startswith("#") or "=" not in line:
    continue
  key, value = line.split("=", 1)
  source[key] = value
lines = [
  f"VS_TEST_MONGO_URL={source['VS_MONGO_URL']}",
  "VS_TEST_MONGO_DB=storagent_virtual_storage_test",
  f"VS_TEST_MINIO_ENDPOINT={source['VS_MINIO_ENDPOINT']}",
  f"VS_TEST_MINIO_ACCESS_KEY={source['VS_MINIO_ACCESS_KEY']}",
  f"VS_TEST_MINIO_SECRET_KEY={source['VS_MINIO_SECRET_KEY']}",
  "VS_TEST_MINIO_BUCKET=vs-pool-v1-test",
]
path = Path("/tmp/vs-pytest.env")
path.write_text("\n".join(lines) + "\n")
os.chmod(path, 0o600)
print("pytest env ready")
