from fastapi import FastAPI
from contextlib import asynccontextmanager
from starlette.middleware.cors import CORSMiddleware

from src.api import register_api
from src.core.database import init_db
from src.configs.configs import settings
from src.utils.logger import setup_logging
from src.core.initialization import init_project, init_service
from src.core.exception import register_exception


app_description = """
Manufacture Management Backend
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

  # 4. 初始化服务
  await init_service()

  yield

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