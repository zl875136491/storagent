from fastapi import FastAPI
from contextlib import asynccontextmanager
from starlette.middleware.cors import CORSMiddleware

from src.api import register_api
from src.core.database import init_db
from src.configs.configs import settings
from src.utils.logger import setup_logging
from src.core.initialization import init_project, init_service
from src.core.etcd_op import get_etcd_client, watch_etcd_task
from src.core.exception import register_exception
from src.modules.auth.crud import cleanup_expired_tokens_task
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
  
  # 5. Etcd 监听
  etcd_client = await get_etcd_client()
  watch_job = asyncio.create_task(watch_etcd_task(etcd_client))

  # 6. 过期 token 清理后台任务
  cleanup_job = asyncio.create_task(cleanup_expired_tokens_task())

  yield
  
  watch_job.cancel()
  cleanup_job.cancel()
  try:
    await etcd_client.close()
  except Exception:
    pass

def create_app() -> FastAPI:
  """
  创建 FastAPI 应用实例
  """
  app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    debug=settings.DEBUG,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    description=app_description
  )

  register_exception(app)
  register_api(app)

  if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
      CORSMiddleware,
      allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
      allow_credentials=True,
      allow_methods=["*"],
      allow_headers=["*"],
    )
  
  return app

# 创建主应用实例
app = create_app()