"""locate 指引 URL 使用 PUBLIC_SCHEME 和区域网关域名。"""
from src.modules.files.locate import _build_api_url, _scheme
from src.modules.files.locate import _build_location_item


def test_scheme_defaults_http(monkeypatch):
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_SCHEME", "http")
  assert _scheme() == "http"


def test_scheme_https(monkeypatch):
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_SCHEME", "HTTPS")
  assert _scheme() == "https"


def test_build_api_url_https(monkeypatch):
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_SCHEME", "https")
  url = _build_api_url(
    "node.example",
    9443,
    "/api/v1/files/object/download",
    {"object_key": "path/to.bin", "offset": 0, "length": 0},
  )
  assert url.startswith("https://node.example:9443/api/v1/files/object/download?")
  assert "object_key=path%2Fto.bin" in url


def test_location_stat_instruction_uses_post_body(monkeypatch):
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_SCHEME", "http")
  server = type("Server", (), {
    "region": type("Region", (), {"name": "beijing", "shown_name": "北京"})(),
    "name": "beijing",
    "host": "10.32.129.241",
    "domain": "",
    "server_port": 6783,
    "master": True,
  })()
  item = _build_location_item(server, "path/to.bin")
  assert item.stat_url == "http://10.32.129.241:6783/api/v1/files/object/stat"
  assert item.stat_method == "POST"
  assert item.stat_body == {"object_key": "path/to.bin"}
  assert "object_key" not in item.stat_url


def test_location_uses_server_domain_gateway_when_region_is_known(monkeypatch):
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_SCHEME", "https")
  server = type("Server", (), {
    "region": type("Region", (), {"name": "beijing", "shown_name": "北京"})(),
    "name": "beijing",
    "domain": "stor.1oa.com.cn",
    "host": "10.32.129.241",
    "server_port": 6783,
    "master": True,
  })()

  item = _build_location_item(server, "path/to.bin", offset=8, length=16)

  assert item.stat_url == "https://stor.1oa.com.cn/server/bj/api/v1/files/object/stat"
  assert item.download_url == (
    "https://stor.1oa.com.cn/server/bj/api/v1/files/object/download?"
    "object_key=path%2Fto.bin&offset=8&length=16"
  )


def test_location_uses_configured_domain_for_legacy_server(monkeypatch):
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_SCHEME", "http")
  monkeypatch.setattr("src.modules.files.locate.settings.PUBLIC_DOMAIN", "stor.1oa.com.cn")
  server = type("Server", (), {
    "region": type("Region", (), {"name": "tianjin", "shown_name": "天津"})(),
    "name": "tianjin",
    "domain": "",
    "host": "10.17.158.115",
    "server_port": 6783,
    "master": False,
  })()

  item = _build_location_item(server, "path/to.bin")

  assert item.stat_url == "http://stor.1oa.com.cn/server/tj/api/v1/files/object/stat"
