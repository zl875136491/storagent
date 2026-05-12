from fastapi import FastAPI
from fastapi import APIRouter
from src.modules.public.route import router as public_router
from src.modules.auth.route import router as auth_router
from src.modules.storage.route import router as storage_router
from src.modules.files.route import router as files_router
from src.modules.graph.route import router as graph_router

# 在 main.py 中调用后, 会将所有 API 路由注册到 FastAPI 应用实例中
def register_api(app: FastAPI):
  """注册路由"""
  # 总路由实例
  api_router = APIRouter(prefix="/api")
  
  api_router.include_router(public_router, tags=["公共模块"], prefix="/public")
  api_router.include_router(auth_router, tags=["认证模块"], prefix="/auth")
  api_router.include_router(storage_router, tags=["存储模块"], prefix="/storage")
  api_router.include_router(files_router, tags=["文件模块"], prefix="/files")
  api_router.include_router(graph_router, tags=["拓扑模块"], prefix="/graph")
  
  app.include_router(api_router)