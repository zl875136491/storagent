"""locate 指引 URL 使用 PUBLIC_SCHEME。"""
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
    "server_port": 6783,
    "master": True,
  })()
  item = _build_location_item(server, "path/to.bin")
  assert item.stat_url == "http://10.32.129.241:6783/api/v1/files/object/stat"
  assert item.stat_method == "POST"
  assert item.stat_body == {"object_key": "path/to.bin"}
  assert "object_key" not in item.stat_url
