"""一次性把已存在的引导账号补成系统迁移申请。

默认只打印计划。缺少归属用户时退出且不写库。不打印密钥或哈希。
"""
from __future__ import annotations

import argparse
import asyncio
import json

from src.virtual_storage.core.database import close_virtual_storage_db, init_virtual_storage_db
from src.virtual_storage.services.legacy_migration import MigrationStopped, migrate_legacy_account


def _parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(description="补齐历史引导账号的申请关联")
  parser.add_argument("--public-id", default="apps")
  parser.add_argument("--owner-user-id", default="")
  parser.add_argument("--owner-username", default="")
  parser.add_argument("--write", action="store_true", help="真正写入。默认只演练")
  return parser


async def _run(args: argparse.Namespace) -> int:
  await init_virtual_storage_db()
  try:
    result = await migrate_legacy_account(
      public_id=args.public_id,
      owner_user_id=args.owner_user_id,
      owner_username=args.owner_username,
      dry_run=not args.write,
    )
  except MigrationStopped as exc:
    print(json.dumps({"ok": False, "stopped": str(exc)}, ensure_ascii=False))
    return 2
  finally:
    await close_virtual_storage_db()
  print(json.dumps({"ok": True, **result}, ensure_ascii=False))
  return 0


def main() -> None:
  raise SystemExit(asyncio.run(_run(_parser().parse_args())))


if __name__ == "__main__":
  main()
