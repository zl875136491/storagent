"""python -m src.virtual_storage.workers"""
from src.virtual_storage.core.security import queue_name
from src.virtual_storage.settings import get_settings
from src.virtual_storage.workers.celery_app import celery_app

settings = get_settings()
argv = [
  "worker",
  f"--queues={queue_name()}",
  f"--hostname=storagent-vs-{settings.region}@%h",
  "--loglevel=INFO",
  "--concurrency=2",
]
if settings.region == settings.home_region:
  argv.append("--beat")
celery_app.worker_main(argv=argv)
