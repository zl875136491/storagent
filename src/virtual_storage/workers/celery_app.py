"""独立 Celery 应用。队列名和 broker 账号都与旧 storagent worker 分开。"""
from __future__ import annotations

import asyncio

from celery import Celery
from celery.schedules import schedule
from kombu import Queue

from src.core.celery_routing import task_headers, validate_task_headers
from src.virtual_storage.core.security import queue_name
from src.virtual_storage.settings import get_settings

_settings = get_settings()
if not _settings.celery_broker_url:
  raise RuntimeError("VS_CELERY_BROKER_URL 未设置，拒绝复用旧 broker 凭据")

_queue = queue_name()
celery_app = Celery(
  "storagent_vs",
  broker=_settings.celery_broker_url,
  backend=_settings.celery_broker_url,
)
celery_app.conf.update(
  task_default_queue=_queue,
  task_queues=(Queue(_queue, routing_key=_queue),),
  task_ignore_result=True,
  broker_connection_retry_on_startup=True,
  beat_schedule_filename="/app/logs/vs-celerybeat-schedule",
  beat_schedule={
    "virtual-storage-scan": {
      "task": "virtual_storage.scan",
      "schedule": schedule(run_every=5.0),
      "options": {
        "queue": _queue,
        "headers": task_headers(_settings.region, protocol_version=_settings.protocol_version),
      },
    }
  },
)


async def _scan() -> None:
  from src.virtual_storage.core.database import close_virtual_storage_db, init_virtual_storage_db
  from src.virtual_storage.core.minio_gateway import MinioGateway
  from src.virtual_storage.models.documents import Tenant, UploadSession, VirtualObject
  from src.virtual_storage.services.lifecycle import advance_retire
  from src.virtual_storage.services.objects import purge_object
  from src.virtual_storage.services.reconcile import reconcile_tenant
  from src.virtual_storage.services.uploads import drive_session
  from src.utils.helpers import utc_now

  await init_virtual_storage_db()
  try:
    gateway = MinioGateway()
    worker_id = f"vs-{_settings.region}"
    sessions = await UploadSession.find(
      {"reservation_held": True, "state": {"$in": ["initializing", "completing", "aborting", "reconciling"]}},
    ).to_list()
    for item in sessions:
      if item.home_region != _settings.region:
        continue
      await drive_session(item.upload_id, gateway, worker_id)
    from src.virtual_storage.models.documents import VirtualAccount
    tenants = await Tenant.find({"state": {"$in": ["retiring", "purge_pending"]}}).to_list()
    for tenant in tenants:
      account = await VirtualAccount.find_one(VirtualAccount.account_id == tenant.account_id)
      if account is None or account.home_region != _settings.region:
        continue
      await advance_retire(tenant.tenant_id, gateway, worker_id)
      await reconcile_tenant(tenant.tenant_id)
    due = await VirtualObject.find(
      {"state": "soft_deleted", "purge_after": {"$lte": utc_now()}},
    ).to_list()
    for obj in due:
      account = await VirtualAccount.find_one(VirtualAccount.account_id == obj.account_id)
      if account is None or account.home_region != _settings.region:
        continue
      await purge_object(obj.object_id, gateway)
    from src.virtual_storage.services.applications import advance_pending
    await advance_pending()
  finally:
    await close_virtual_storage_db()


@celery_app.task(name="virtual_storage.scan")
def scan_task():
  request = scan_task.request
  headers = dict(getattr(request, "headers", None) or {})
  delivery = getattr(request, "delivery_info", None) or {}
  if isinstance(delivery, dict) and isinstance(delivery.get("headers"), dict):
    headers.update(delivery["headers"])
  validate_task_headers(
    headers,
    worker_region=_settings.region,
    protocol_version=_settings.protocol_version,
  )
  asyncio.run(_scan())
