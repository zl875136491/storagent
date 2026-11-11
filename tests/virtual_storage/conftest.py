"""真实 Mongo 副本集与专用 MinIO。缺少连接信息时失败，而不是跳过。"""
import os

import pytest

_REQUIRED = (
  "VS_TEST_MONGO_URL",
  "VS_TEST_MINIO_ENDPOINT",
  "VS_TEST_MINIO_ACCESS_KEY",
  "VS_TEST_MINIO_SECRET_KEY",
)


def _require_real_infra():
  missing = [name for name in _REQUIRED if not os.getenv(name, "").strip()]
  if missing:
    pytest.fail("缺少真实基础设施环境变量，不能把未执行的集成测试当作通过: " + ", ".join(missing))


_require_real_infra()
os.environ["VS_MONGO_URL"] = os.environ["VS_TEST_MONGO_URL"]
os.environ["VS_MONGO_DB"] = os.environ.get("VS_TEST_MONGO_DB", "storagent_virtual_storage_test")
os.environ["VS_REGION"] = os.environ.get("VS_TEST_REGION", "nuc-docker-a")
os.environ["VS_HOME_REGION"] = os.environ["VS_REGION"]
os.environ["VS_HOME_BASE_URL"] = ""
os.environ["VS_REQUIRE_HTTPS"] = "false"
os.environ["VS_BOOTSTRAP"] = "false"
os.environ["VS_CELERY_BROKER_URL"] = ""
os.environ["VS_MINIO_ENDPOINT"] = os.environ["VS_TEST_MINIO_ENDPOINT"]
os.environ["VS_MINIO_ACCESS_KEY"] = os.environ["VS_TEST_MINIO_ACCESS_KEY"]
os.environ["VS_MINIO_SECRET_KEY"] = os.environ["VS_TEST_MINIO_SECRET_KEY"]
os.environ["VS_MINIO_SECURE"] = "false"
os.environ["VS_MINIO_BUCKET"] = os.environ.get("VS_TEST_MINIO_BUCKET", "vs-pool-v1-test")
os.environ["VS_ENABLE_TEST_DRIVE"] = "true"
os.environ.setdefault("SECRET_KEY", "virtual-storage-test-secret")
os.environ.setdefault("VS_OAUTH_SECRET", "virtual-storage-oauth-test-secret")
os.environ.setdefault("VS_OAUTH_ISSUER", "https://virtual-storage.test")
os.environ.setdefault("REGION", os.environ["VS_REGION"])

from src.virtual_storage.settings import reset_settings_cache

reset_settings_cache()

from fastapi.testclient import TestClient
from src.virtual_storage.app import create_app


@pytest.fixture(scope="session")
def client():
  app = create_app()
  with TestClient(app) as test_client:
    yield test_client
