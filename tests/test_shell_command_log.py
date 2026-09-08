"""Mongo shell_command persistence can be disabled."""
import pytest

from src.modules.public import crud as public_crud


@pytest.mark.asyncio
async def test_create_shell_command_log_skips_when_disabled(monkeypatch):
  monkeypatch.setattr("src.configs.configs.settings.SHELL_COMMAND_LOG_ENABLED", False)

  class Boom:
    def __init__(self, **kwargs):
      raise AssertionError("shell_command must not be written when disabled")

    async def save(self):
      raise AssertionError("shell_command must not be written when disabled")

  monkeypatch.setattr(public_crud, "ShellCommandLog", Boom)
  result = await public_crud.create_shell_command_log("mc ls local/bucket", "ok", "")
  assert result is None


@pytest.mark.asyncio
async def test_create_shell_command_log_writes_when_enabled(monkeypatch):
  recorded = {}

  class FakeLog:
    def __init__(self, **kwargs):
      recorded.update(kwargs)

    async def save(self):
      recorded["saved"] = True

  monkeypatch.setattr("src.configs.configs.settings.SHELL_COMMAND_LOG_ENABLED", True)
  monkeypatch.setattr(public_crud, "ShellCommandLog", FakeLog)
  result = await public_crud.create_shell_command_log("mc ls local/bucket", "out", "err")
  assert isinstance(result, FakeLog)
  assert recorded["command"] == "mc ls local/bucket"
  assert recorded["stdout"] == "out"
  assert recorded["stderr"] == "err"
  assert recorded["saved"] is True
