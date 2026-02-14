from fastapi import FastAPI
from fastapi import APIRouter

# 在 main.py 中调用后, 会将所有 API 路由注册到 FastAPI 应用实例中
def register_api(app: FastAPI):
  """注册路由"""
  # 总路由实例
  api_router = APIRouter(prefix="/api")
  
  app.include_router(api_router)