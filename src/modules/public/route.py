from fastapi import APIRouter, Depends
from src.core.auth import _is_superadmin, get_current_user, check_permissions
from src.modules.auth.model import User
from src.modules.public import service as public_service
from src.modules.public import schema as public_schema
from fastapi.responses import Response
from fastapi.responses import StreamingResponse

router = APIRouter()

@router.get(
  path="/endpoints",
  response_model=public_schema.EndpointsResponse,
  summary="获取端点列表")
async def get_endpoints() -> public_schema.EndpointsResponse:
  """
  获取端点列表
  """
  return await public_service.get_endpoints()

@router.get(
  path="/endpoints/test",
  summary="测试端点")
async def test_endpoints():
  """
  测试端点
  """
  return Response(
    content=await public_service.test_endpoints(),
    media_type="application/octet-stream"
  )
    
@router.post(
  path="/region",
  response_model=public_schema.RegionResponse,
  summary="创建区域")
async def create_region(
  payload: public_schema.RegionCreateRequest,
  current_user: User = Depends(get_current_user),
) -> public_schema.RegionResponse:
  """
  创建区域（需 region_manage）
  """
  await check_permissions(current_user, ["region_manage"])
  name = payload.name.strip()
  shown_name = payload.shown_name.strip()
  return await public_service.create_region(name, shown_name)

@router.get(
  path="/region",
  response_model=public_schema.RegionListResponse,
  summary="获取区域列表")
async def get_region_list(
  current_user: User = Depends(get_current_user),
) -> public_schema.RegionListResponse:
  """
  获取区域列表（需登录）
  """
  return await public_service.get_region_list()

@router.delete(
  path="/region/{region_id}",
  response_model=public_schema.SimpleMessageResponse,
  summary="下线区域")
async def offline_region(
  region_id: public_schema.PydanticObjectId,
  current_user: User = Depends(get_current_user),
) -> public_schema.SimpleMessageResponse:
  """
  从 Etcd 拓扑与本地库下线区域（需 region_manage；不可下线本节点）
  """
  await check_permissions(current_user, ["region_manage"])
  return await public_service.offline_region(region_id)

@router.post(
  path="/application",
  response_model=public_schema.ApplicationResponse,
  summary="创建应用")
async def create_application(
  payload: public_schema.ApplicationCreateRequest,
  current_user: User = Depends(get_current_user)) -> public_schema.ApplicationResponse:
  """
  创建应用
  """
  name = payload.name.strip()
  shown_name = payload.shown_name.strip().lower()
  description = payload.description.strip()
  return await public_service.create_application(
    name=name,
    shown_name=shown_name,
    description=description,
    current_user=current_user
  )

@router.get(
  path="/application",
  response_model=public_schema.ApplicationListResponse,
  summary="获取应用列表")
async def get_application_list(
  current_user: User = Depends(get_current_user),
) -> public_schema.ApplicationListResponse:
  """
  获取应用列表（需登录）
  """
  return await public_service.get_application_list()


@router.put(
  path="/application/{application_id}/quota",
  response_model=public_schema.ApplicationResponse,
  summary="更新应用存储配额",
)
async def update_application_quota(
  application_id: public_schema.PydanticObjectId,
  payload: public_schema.ApplicationQuotaUpdateRequest,
  current_user: User = Depends(get_current_user),
) -> public_schema.ApplicationResponse:
  await check_permissions(current_user, ["application_quota_manage"])
  return await public_service.update_application_quota(
    application_id,
    payload.quota_bytes,
    current_user,
  )


@router.get(
  path="/quota-alert-rule",
  response_model=public_schema.QuotaAlertRuleResponse,
  summary="获取全局配额告警规则",
)
async def get_quota_alert_rule(
  current_user: User = Depends(get_current_user),
) -> public_schema.QuotaAlertRuleResponse:
  from src.modules.public import quota_alert
  return quota_alert.rule_response(await quota_alert.get_rule())


@router.put(
  path="/quota-alert-rule",
  response_model=public_schema.QuotaAlertRuleResponse,
  summary="更新全局配额告警规则",
)
async def update_quota_alert_rule(
  payload: public_schema.QuotaAlertRuleUpdateRequest,
  current_user: User = Depends(get_current_user),
) -> public_schema.QuotaAlertRuleResponse:
  from src.modules.public import quota_alert
  await check_permissions(current_user, ["application_manage"])
  return await quota_alert.update_rule(payload, current_user)


@router.post(
  path="/application/{application_id}/expansion-requests",
  response_model=public_schema.ExpansionRequestResponse,
  summary="为自己的应用提交扩容申请",
)
async def create_expansion_request(
  application_id: public_schema.PydanticObjectId,
  payload: public_schema.ExpansionRequestCreate,
  current_user: User = Depends(get_current_user),
) -> public_schema.ExpansionRequestResponse:
  from src.modules.public import quota_alert
  return await quota_alert.create_expansion_request(application_id, payload, current_user)


@router.get(
  path="/application/expansion-requests",
  response_model=public_schema.ExpansionRequestListResponse,
  summary="查询应用扩容申请",
)
async def get_expansion_requests(
  current_user: User = Depends(get_current_user),
) -> public_schema.ExpansionRequestListResponse:
  from src.modules.public import quota_alert
  can_review = (
    await _is_superadmin(current_user)
    or "application_manage" in set(current_user.permissions or [])
  )
  return await quota_alert.list_expansion_requests(current_user, all_requests=can_review)


@router.put(
  path="/application/expansion-requests/{request_id}/review",
  response_model=public_schema.ExpansionRequestResponse,
  summary="审批应用扩容申请",
)
async def review_expansion_request(
  request_id: str,
  payload: public_schema.ExpansionRequestReview,
  current_user: User = Depends(get_current_user),
) -> public_schema.ExpansionRequestResponse:
  from src.modules.public import quota_alert
  await check_permissions(current_user, ["application_manage"])
  return await quota_alert.review_expansion_request(request_id, payload, current_user)

@router.post(
  path="/application/{application_id}/approval",
  summary="授权应用（SSE 进度）")
async def approval_application(
  application_id: public_schema.PydanticObjectId,
  current_user: User = Depends(get_current_user)):
  """
  授权应用；响应为 text/event-stream，每条事件为 JSON（含 step、status、message 等）。
  """
  await check_permissions(current_user, ["application_manage"])
  return StreamingResponse(
    public_service.enable_application(application_id, current_user),
    media_type="text/event-stream",
    headers={
      "Cache-Control": "no-cache",
      "Connection": "keep-alive",
      "X-Accel-Buffering": "no",
    },
  )

@router.get(
  path="/application/enabled",
  response_model=public_schema.SimpleApplicationListResponse,
  summary="用户启用的应用列表")
async def get_users_enabled_application_list(
  current_user: User = Depends(get_current_user)) -> public_schema.SimpleApplicationListResponse:
  """
  用户启用的应用列表
  """
  return await public_service.get_users_enabled_application_list(current_user)

@router.post(
  path="/api-key",
  response_model=public_schema.APIKeyResponse,
  summary="创建API密钥")
async def create_api_key(
  payload: public_schema.APIKeyCreateRequest,
  current_user: User = Depends(get_current_user)) -> public_schema.APIKeyResponse:
  """
  创建API密钥
  """
  application_id = payload.application_id
  expired_at = payload.expired_at
  return await public_service.create_api_key(
    application_id=application_id,
    expired_at=expired_at,
    current_user=current_user
  )

@router.get(
  path="/api-key",
  response_model=public_schema.APIKeyListResponse,
  summary="获取API密钥列表")
async def get_api_key_list(
  current_user: User = Depends(get_current_user)) -> public_schema.APIKeyListResponse:
  """
  获取API密钥列表
  """
  return await public_service.get_api_key_list(current_user)

@router.delete(
  path="/api-key/{api_key_id}",
  response_model=public_schema.SimpleMessageResponse,
  summary="吊销API密钥")
async def revoke_api_key(
  api_key_id: public_schema.PydanticObjectId,
  current_user: User = Depends(get_current_user)) -> public_schema.SimpleMessageResponse:
  """
  吊销 API 密钥（软删除，不可恢复）
  """
  return await public_service.revoke_api_key(api_key_id, current_user)
