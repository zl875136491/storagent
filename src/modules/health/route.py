from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
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
    from src.core.etcd_op import (
      get_etcd_client,
      is_recoverable_etcd_pool_error,
      refresh_shared_etcd_client,
    )

    etcd = await get_etcd_client()
    try:
      await etcd.status()
    except Exception as error:
      if not is_recoverable_etcd_pool_error(error):
        raise
      await refresh_shared_etcd_client()
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


@router.get(
  path="/metrics",
  summary="运行指标",
  response_class=Response,
)
async def metrics_endpoint(format: str = "prometheus"):
  """
  暴露进程内指标。默认 Prometheus text；`?format=json` 返回 JSON。
  """
  from src.core import metrics as metrics_mod

  try:
    from src.modules.etcd import service as etcd_service
    await etcd_service.get_status()
  except Exception:
    pass

  if format == "json":
    return JSONResponse(
      content={"region": settings.REGION, **metrics_mod.snapshot()},
    )
  body = metrics_mod.render_prometheus(settings.REGION)
  return Response(content=body, media_type="text/plain; version=0.0.4; charset=utf-8")
