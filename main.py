from fastapi import FastAPI
from contextlib import asynccontextmanager
from starlette.middleware.cors import CORSMiddleware

from src.api import register_api
from src.core.database import init_db, close_db
from src.configs.configs import settings
from src.utils.logger import setup_logging
from src.core.initialization import init_project, init_service
from src.core.etcd_op import close_shared_etcd_client, get_etcd_client, watch_etcd_task
from src.core.cors_origins import allowlist, load_allowlist_from_mongo
from src.core.exception import register_exception
from src.core.middleware import RequestContextMiddleware, UploadBodyLimitMiddleware
from src.modules.storage.operations import (
  shutdown_background_operations,
)
import asyncio

app_description = """
Storagent — 多区域 MinIO 对象存储管理后端

提供用户认证、应用/API Key 管理、S3 分片上传下载、存储拓扑可视化等能力。
"""

@asynccontextmanager
async def lifespan(app: FastAPI):
  """
  应用生命周期管理
  """
  settings.validate_runtime()

  # 1. 设置日志
  setup_logging()

  # 2. 初始化数据库
  await init_db()
  
  # 3. 初始化项目
  await init_project()

  # 4. 初始化服务（Region/MinIO 注册到 Etcd 并同步 MongoDB）
  if settings.INIT_SERVICE:
    try:
      await init_service()
    except Exception as e:
      from src.utils.logger import logger
      logger.warning(f"init_service 失败（服务仍可启动）: {e}")

  try:
    await load_allowlist_from_mongo()
  except Exception as e:
    from src.utils.logger import logger
    logger.warning(f"加载应用来源白名单失败: {e}")
  
  # 5. Etcd 监听。Watch uses a dedicated connection so reconnect can close
  # it without dropping the request-path pooled client.
  etcd_client = await get_etcd_client(dedicated=True)
  watch_job = asyncio.create_task(watch_etcd_task(etcd_client))

  # 周期维护、长耗时运维和 Etcd 全量校准由 Celery Beat/Worker 承担。

  yield
  
  watch_job.cancel()
  await shutdown_background_operations()
  try:
    await etcd_client.close()
  except Exception:
    pass
  try:
    await close_shared_etcd_client()
  except Exception:
    pass
  try:
    await close_db()
  except Exception:
    pass

def create_app() -> FastAPI:
  """
  创建 FastAPI 应用实例
  """
  enable_docs = settings.DEBUG or settings.ENABLE_DOCS
  app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    debug=settings.DEBUG,
    lifespan=lifespan,
    docs_url="/docs" if enable_docs else None,
    redoc_url="/redoc" if enable_docs else None,
    openapi_url="/openapi.json" if enable_docs else None,
    description=app_description
  )

  register_exception(app)
  register_api(app)
  app.add_middleware(RequestContextMiddleware)

  if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
      CORSMiddleware,
      allow_origins=allowlist,
      allow_credentials=True,
      # 收窄为实际用到的方法/请求头（最小权限），而不是笼统的 "*"。
      # 这不会改变浏览器是否发起预检的判断（凡是非"简单请求"仍会预检），
      # 但配合下面的 max_age，同一 origin+method+header 组合的预检结果会
      # 被浏览器缓存复用，避免每次业务请求都重新走一次 OPTIONS 预检。
      allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
      allow_headers=["Authorization", "Content-Type", "x-api-key", "x-demo-api-key-id"],
      max_age=settings.BACKEND_CORS_MAX_AGE_SECONDS,
    )

  # Keep this outermost so chunked oversized bodies cannot be converted into
  # a generic parser error by an inner middleware.
  app.add_middleware(UploadBodyLimitMiddleware)
  
  return app

# 创建主应用实例
app = create_app()
