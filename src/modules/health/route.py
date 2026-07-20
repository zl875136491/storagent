from fastapi import APIRouter
from fastapi.responses import JSONResponse
from src.configs.configs import settings

router = APIRouter()

@router.get(
  path="/health",
  summary="健康检查")
async def health_check():
  """
  存活探针：进程正常即可（不依赖外部依赖）
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
  就绪探针：MongoDB / Etcd 不可用时返回 HTTP 503，便于编排摘流
  """
  from src.core.database import get_motor_client
  from src.core.etcd_op import get_etcd_client

  client = get_motor_client()
  if client is None:
    return JSONResponse(
      status_code=503,
      content={"status": "not_ready", "reason": "database not initialized", "region": settings.REGION},
    )
  try:
    await client.admin.command("ping")
  except Exception as e:
    return JSONResponse(
      status_code=503,
      content={"status": "not_ready", "reason": f"database: {e}", "region": settings.REGION},
    )

  etcd = None
  try:
    etcd = await get_etcd_client()
    await etcd.status()
  except Exception as e:
    return JSONResponse(
      status_code=503,
      content={"status": "not_ready", "reason": f"etcd: {e}", "region": settings.REGION},
    )
  finally:
    if etcd is not None:
      try:
        await etcd.close()
      except Exception:
        pass

  return {"status": "ready", "region": settings.REGION}
