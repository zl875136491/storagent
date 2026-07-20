from fastapi import APIRouter
from src.configs.configs import settings

router = APIRouter()

@router.get(
  path="/health",
  summary="健康检查")
async def health_check():
  """
  服务健康检查，用于负载均衡和容器编排探针
  """
  return {
    "status": "ok",
    "app": settings.APP_NAME,
    "version": settings.APP_VERSION,
    "region": settings.REGION,
  }

@router.get(
  path="/ready",
  summary="就绪检查")
async def readiness_check():
  """
  就绪检查，验证数据库连接是否可用
  """
  from src.core.database import get_motor_client
  client = get_motor_client()
  if client is None:
    return {"status": "not_ready", "reason": "database not initialized"}
  try:
    await client.admin.command("ping")
    return {"status": "ready", "region": settings.REGION}
  except Exception as e:
    return {"status": "not_ready", "reason": str(e)}
