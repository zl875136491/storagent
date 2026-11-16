"""独立库连接。启动时用真实写入确认多文档事务可用。"""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import OperationFailure

from src.utils.helpers import utc_now
from src.virtual_storage.core.exception import InfrastructureError
from src.virtual_storage.settings import get_settings

_client: Optional[AsyncIOMotorClient] = None


def get_client() -> AsyncIOMotorClient:
  if _client is None:
    raise InfrastructureError("虚拟存储数据库尚未初始化")
  return _client


async def init_virtual_storage_db() -> None:
  global _client
  settings = get_settings()
  _client = AsyncIOMotorClient(
    settings.mongo_url,
    serverSelectionTimeoutMS=8000,
    retryWrites=True,
  )
  from src.virtual_storage.models.documents import (
    AuditRecord,
    BucketPool,
    KeyDelivery,
    MinioCall,
    OperationRecord,
    QuotaLedgerEntry,
    Tenant,
    TenantAPIKey,
    UploadSession,
    VirtualAccount,
    VirtualAccountApplication,
    VirtualAccountGrant,
    OAuthRefreshToken,
    VirtualObject,
  )
  from beanie import init_beanie

  database = _client[settings.mongo_db]
  await init_beanie(
    database=database,
    document_models=[
      BucketPool,
      VirtualAccount,
      VirtualAccountApplication,
      VirtualAccountGrant,
      OAuthRefreshToken,
      Tenant,
      TenantAPIKey,
      KeyDelivery,
      UploadSession,
      VirtualObject,
      QuotaLedgerEntry,
      AuditRecord,
      MinioCall,
      OperationRecord,
    ],
  )
  await assert_transactions_work()


async def assert_transactions_work() -> None:
  """插入探针文档并回滚。单机 Mongo 会在这里失败，服务不得假装事务可用。"""
  client = get_client()
  settings = get_settings()
  probe = client[settings.mongo_db]["vs_txn_probe"]
  try:
    async with await client.start_session() as session:
      session.start_transaction()
      await probe.insert_one({"probe": True, "at": utc_now()}, session=session)
      visible = await probe.count_documents({"probe": True}, session=session)
      await session.abort_transaction()
  except OperationFailure as exc:
    raise InfrastructureError(
      "MongoDB 不支持多文档事务。虚拟存储账本不能降级为单文档或内存账本。"
    ) from exc
  if visible != 1:
    raise InfrastructureError("事务内读不到探针写入，不能确认多文档事务")
  leftover = await probe.count_documents({"probe": True})
  if leftover:
    raise InfrastructureError("事务回滚后探针仍在，不能把该库当作账本")


async def close_virtual_storage_db() -> None:
  global _client
  if _client is not None:
    _client.close()
    _client = None


@asynccontextmanager
async def transaction():
  client = get_client()
  async with await client.start_session() as session:
    async with session.start_transaction():
      yield session


async def run_transaction(work, attempts: int = 5):
  """对写冲突做有限次重试，让失败方重新检查配额而不是直接 500。"""
  import asyncio
  from pymongo.errors import OperationFailure

  last_error = None
  for attempt in range(attempts):
    try:
      async with transaction() as session:
        return await work(session)
    except OperationFailure as exc:
      last_error = exc
      if exc.code != 112 or attempt == attempts - 1:
        raise
      await asyncio.sleep(0.02 * (attempt + 1))
  raise last_error
