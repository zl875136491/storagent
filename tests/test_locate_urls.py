"""locate 指引 URL 使用 PUBLIC_SCHEME。"""
from src.modules.files.locate import _build_api_url, _scheme


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
    "/api/files/object/download",
    {"object_key": "path/to.bin", "offset": 0, "length": 0},
  )
  assert url.startswith("https://node.example:9443/api/files/object/download?")
  assert "object_key=path%2Fto.bin" in url
