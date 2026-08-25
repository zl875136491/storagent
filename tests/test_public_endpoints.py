"""Public endpoint discovery must prefer the Nginx domain gateway."""
from unittest.mock import AsyncMock, patch

from src.modules.public.service import get_endpoints
from src.modules.public import service as public_service
from src.core.exception import CustomException
import pytest


def _server(*, region: str, domain: str = ""):
  return type("Server", (), {
    "region": type("Region", (), {"name": region, "shown_name": region})(),
    "id": "server-id",
    "name": region,
    "domain": domain,
    "host": "10.32.129.241",
    "server_port": 6783,
    "minio_port": 9000,
    "master": True,
  })()


async def test_endpoints_builds_gateway_url_from_server_domain(monkeypatch):
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_SCHEME", "http")
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_DOMAIN", "")
  server = _server(region="beijing", domain="stor.1oa.com.cn")

  with patch(
    "src.modules.storage.service.get_minio_server_list",
    new=AsyncMock(return_value={"data": [server]}),
  ):
    result = await get_endpoints(include_minio=True)

  endpoint = result["data"][0]
  assert endpoint["domain"] == "stor.1oa.com.cn"
  assert endpoint["endpoint"] == "http://stor.1oa.com.cn/server/bj"
  assert endpoint["minio_endpoint"] == "http://10.32.129.241:9000"


async def test_endpoints_hides_minio_endpoint_by_default(monkeypatch):
  """匿名/普通用户不返回 minio_endpoint，仅管理员（include_minio=True）可见。"""
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_SCHEME", "http")
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_DOMAIN", "")
  server = _server(region="beijing", domain="stor.1oa.com.cn")

  with patch(
    "src.modules.storage.service.get_minio_server_list",
    new=AsyncMock(return_value={"data": [server]}),
  ):
    result = await get_endpoints()

  endpoint = result["data"][0]
  assert "minio_endpoint" not in endpoint
  assert endpoint["endpoint"] == "http://stor.1oa.com.cn/server/bj"


async def test_endpoints_uses_public_domain_for_legacy_record(monkeypatch):
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_SCHEME", "https")
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_DOMAIN", "stor.1oa.com.cn")
  server = _server(region="tianjin")

  with patch(
    "src.modules.storage.service.get_minio_server_list",
    new=AsyncMock(return_value={"data": [server]}),
  ):
    result = await get_endpoints()

  endpoint = result["data"][0]
  assert endpoint["domain"] == "stor.1oa.com.cn"
  assert endpoint["endpoint"] == "https://stor.1oa.com.cn/server/tj"


async def test_endpoints_keeps_host_port_fallback_for_unknown_region(monkeypatch):
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_SCHEME", "http")
  monkeypatch.setattr("src.modules.public.service.settings.PUBLIC_DOMAIN", "stor.1oa.com.cn")
  server = _server(region="nuc-docker-a")

  with patch(
    "src.modules.storage.service.get_minio_server_list",
    new=AsyncMock(return_value={"data": [server]}),
  ):
    result = await get_endpoints()

  endpoint = result["data"][0]
  assert endpoint["domain"] == "stor.1oa.com.cn"
  assert endpoint["endpoint"] == "http://10.32.129.241:6783"


@pytest.mark.asyncio
async def test_application_name_cannot_claim_reserved_archive_bucket(monkeypatch):
  monkeypatch.setattr(public_service.settings, "OBJECT_ARCHIVE_BUCKET", "storagent-expired-archive")

  with pytest.raises(CustomException, match="归档存储桶"):
    await public_service._validate_application_name("storagent-expired-archive")
