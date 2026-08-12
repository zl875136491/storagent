from datetime import datetime

from fastapi import APIRouter, Depends, Query

from src.core.auth import require_admin
from src.core.exception import CustomException, ErrorDesc
from src.modules.audit import schema, service
from src.modules.auth.model import User


router = APIRouter()


@router.get("/events", response_model=schema.AuditEventListResponse, summary="管理员：查询审计日志")
async def list_audit_events(
  start_at: datetime | None = Query(None),
  end_at: datetime | None = Query(None),
  action: str | None = Query(None, min_length=1, max_length=128),
  actor: str | None = Query(None, min_length=1, max_length=128),
  region: str | None = Query(None, min_length=1, max_length=128),
  resource: str | None = Query(None, min_length=1, max_length=256),
  success: bool | None = Query(None),
  page: int = Query(1, ge=1),
  page_size: int = Query(50, ge=1, le=100),
  current_user: User = Depends(require_admin),
) -> schema.AuditEventListResponse:
  try:
    return await service.list_events(
      start_at=start_at, end_at=end_at, action=action, actor=actor,
      region=region, resource=resource, success=success,
      page=page, page_size=page_size,
    )
  except ValueError as exc:
    raise CustomException(ErrorDesc.INVALID_PARAMS, str(exc)) from exc


@router.get("/options", response_model=schema.AuditEventOptionsResponse, summary="管理员：获取审计筛选项")
async def audit_event_options(
  current_user: User = Depends(require_admin),
) -> schema.AuditEventOptionsResponse:
  return await service.get_options()
