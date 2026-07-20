"""生产配置校验。"""
import sys

from src.configs.configs import Settings


def test_validate_runtime_skips_under_pytest():
  from src.configs.configs import settings
  settings.validate_runtime()


def test_validate_rejects_bad_prod_config(monkeypatch):
  # 暂时伪装非 pytest，以触发真实校验
  monkeypatch.delitem(sys.modules, "pytest", raising=False)
  s = Settings(
    REGION="undefined",
    DEBUG=False,
    SECRET_KEY="short",
    BACKEND_CORS_ORIGINS=["*"],
  )
  try:
    s.validate_runtime()
    raised = False
  except RuntimeError as e:
    raised = True
    msg = str(e)
    assert "REGION" in msg
    assert "SECRET_KEY" in msg
    assert "CORS" in msg
  finally:
    # 恢复 pytest 模块引用（由 pytest 自身再注入）
    pass
  assert raised


def test_docs_and_reload_defaults():
  from src.configs.configs import settings
  assert hasattr(settings, "ENABLE_DOCS")
  assert isinstance(settings.RELOAD, bool)
