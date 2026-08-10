from fastapi import FastAPI
from fastapi import APIRouter
from src.configs.consts import API_V1_PREFIX, API_V2_PREFIX
from src.modules.public.route import router as public_router
from src.modules.auth.route import router as auth_router
from src.modules.storage.route import router as storage_router
from src.modules.files.route import router as files_router
from src.modules.graph.route import router as graph_router
from src.modules.health.route import router as health_router
from src.modules.ai.route import router as ai_router
from src.modules.usage.route import router as usage_router
from src.modules.demo.route import router as demo_router
from src.modules.files.v2.route import router as files_v2_router
from src.modules.storage.v2.route import router as storage_v2_router
from src.modules.public.v2.route import router as public_v2_router
from src.modules.auth.v2.route import router as auth_v2_router
from src.modules.graph.v2.route import router as graph_v2_router
from src.modules.usage.v2.route import router as usage_v2_router
from src.modules.ai.v2.route import router as ai_v2_router
from src.modules.demo.v2.route import router as demo_v2_router

# 在 main.py 中调用后, 会将所有 API 路由注册到 FastAPI 应用实例中
def register_api(app: FastAPI):
  """
  注册路由。

  所有业务接口统一挂载在 API_V1_PREFIX（/api/v1）下。历史未带版本号的 `/api/*`
  路径已完全下线：其鉴权模型（前端直接持有 x-api-key）不安全，v1 起由能力令牌
  机制（见 src/core/capability_token.py）取代，不再兼容旧路径。
  """
  # 总路由实例
  api_router = APIRouter(prefix=API_V1_PREFIX)
  
  api_router.include_router(public_router, tags=["公共模块"], prefix="/public")
  api_router.include_router(auth_router, tags=["认证模块"], prefix="/auth")
  api_router.include_router(storage_router, tags=["存储模块"], prefix="/storage")
  api_router.include_router(files_router, tags=["文件模块"], prefix="/files")
  api_router.include_router(graph_router, tags=["拓扑模块"], prefix="/graph")
  api_router.include_router(ai_router, tags=["AI 助手"], prefix="/ai")
  api_router.include_router(usage_router, tags=["用量统计"], prefix="/usage")
  # Console demos are authenticated with the user's JWT and an opaque APIKey ID.
  # This keeps APIKey plaintext out of browser storage, headers, and URLs.
  api_router.include_router(demo_router, tags=["控制台演示"], prefix="/demo")
  app.include_router(health_router, tags=["健康检查"])
  
  app.include_router(api_router)

  v2_router = APIRouter(prefix=API_V2_PREFIX)
  # Unchanged domains keep their existing request/response contract behind a
  # version-specific route+service pair; clients can upgrade URLs uniformly.
  v2_router.include_router(public_v2_router, tags=["公共模块 v2"], prefix="/public")
  v2_router.include_router(auth_v2_router, tags=["认证模块 v2"], prefix="/auth")
  v2_router.include_router(files_v2_router, tags=["文件模块 v2"], prefix="/files")
  v2_router.include_router(storage_v2_router, tags=["存储模块 v2"], prefix="/storage")
  v2_router.include_router(graph_v2_router, tags=["拓扑模块 v2"], prefix="/graph")
  v2_router.include_router(ai_v2_router, tags=["AI 助手 v2"], prefix="/ai")
  v2_router.include_router(usage_v2_router, tags=["用量统计 v2"], prefix="/usage")
  v2_router.include_router(demo_v2_router, tags=["控制台演示 v2"], prefix="/demo")
  app.include_router(v2_router)
