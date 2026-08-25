from fastapi import APIRouter, Depends, Request
from fastapi.responses import PlainTextResponse

from src.core.auth import check_permissions, get_current_app_context, get_current_user
from src.modules.auth.model import User
from src.modules.diagnostics import schema, service

router = APIRouter()


@router.get("/{version}/self-diagnosis", response_class=PlainTextResponse, summary="下载调用方自诊断脚本")
async def download_script(version: str):
  return PlainTextResponse(
    service.render_script(version),
    media_type="text/x-shellscript; charset=utf-8",
    headers={"Content-Disposition": "attachment; filename=storagent-" + service.validate_version(version) + "-self-diagnosis.sh"},
  )


@router.get("/{version}/probe", summary="自诊断认证与版本契约探针")
async def probe(
  version: str,
  _context: dict = Depends(get_current_app_context),
):
  return {"authenticated": True, "api_version": service.validate_version(version)}


@router.get("/{version}/quota-capacity-probe", summary="自诊断应用配额与集群容量预检")
async def quota_capacity_probe(version: str, context: dict = Depends(get_current_app_context)):
  service.validate_version(version)
  return await service.quota_capacity_probe(context)


@router.post("/{version}/storage-probe", summary="自诊断临时存储读写")
async def storage_probe(version: str, payload: dict, context: dict = Depends(get_current_app_context)):
  service.validate_version(version)
  return await service.storage_probe(context, str(payload.get("run_id") or ""))


@router.post("/{version}/report", response_model=schema.DiagnosticRunItem, summary="回传自诊断日志")
async def report(version: str, payload: schema.DiagnosticReportCreate, request: Request, context: dict = Depends(get_current_app_context)):
  return await service.save_report(version, payload, context, request.client.host if request.client else "")


@router.get("/runs", response_model=schema.DiagnosticRunListResponse, summary="运维人员查询调用方自诊断记录")
async def list_runs(current_user: User = Depends(get_current_user)):
  await check_permissions(current_user, ["storage_operations_manage"])
  return await service.list_runs()
