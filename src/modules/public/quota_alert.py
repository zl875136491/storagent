"""Global quota alerting and application expansion approval workflow."""
from __future__ import annotations

import asyncio
from datetime import timedelta

from beanie.odm.fields import Link
from beanie.exceptions import CollectionWasNotInitialized

from src.configs.configs import settings
from src.core.exception import CustomException, ErrorDesc
from src.modules.auth import oa as oa_service
from src.modules.auth.model import User
from src.modules.public.model import (
  Application,
  ApplicationExpansionRequest,
  QuotaAlertEvent,
  QuotaAlertRule,
)
from src.utils.helpers import utc_now
from src.utils.logger import logger


DEFAULT_TEMPLATE = (
  "应用 {app_name} 当前配额使用率为 {usage_percent}%，请关注容量并按需提交扩容申请。"
)


async def get_rule() -> QuotaAlertRule:
  # Upload admission is also exercised by isolated unit tests that do not
  # initialize Beanie. In that context the persisted rule is unavailable, so
  # retain the historical 100% quota limit through an in-memory default.
  try:
    rule = await QuotaAlertRule.find_one()
  except CollectionWasNotInitialized:
    return QuotaAlertRule(message_template=DEFAULT_TEMPLATE)
  if rule:
    return rule
  rule = QuotaAlertRule(message_template=DEFAULT_TEMPLATE)
  await rule.insert()
  return rule


def rule_response(rule: QuotaAlertRule) -> dict:
  return {
    "low_percent": rule.low_percent,
    "medium_percent": rule.medium_percent,
    "high_percent": rule.high_percent,
    "block_percent": rule.block_percent,
    "message_template": rule.message_template,
    "updated_at": rule.updated_at,
    "updated_by": rule.updated_by,
  }


async def update_rule(payload, actor: User) -> dict:
  values = [payload.low_percent, payload.medium_percent, payload.high_percent, payload.block_percent]
  if values != sorted(values) or len(set(values)) != len(values):
    raise CustomException(ErrorDesc.INVALID_PARAMS, "阈值必须按低、中、高、阻断严格递增")
  rule = await get_rule()
  rule.low_percent = payload.low_percent
  rule.medium_percent = payload.medium_percent
  rule.high_percent = payload.high_percent
  rule.block_percent = payload.block_percent
  rule.message_template = payload.message_template.strip()
  rule.updated_at = utc_now()
  rule.updated_by = actor.username
  await rule.save()
  return rule_response(rule)


async def _application_owner(application: Application) -> User | None:
  owner = application.author
  if isinstance(owner, Link):
    return await owner.fetch()
  if hasattr(owner, "fetch") and not hasattr(owner, "username"):
    return await owner.fetch()
  return owner if hasattr(owner, "username") else None


def _level(rule: QuotaAlertRule, ratio: float) -> str | None:
  percent = ratio * 100
  if percent >= rule.block_percent:
    return "blocked"
  if percent >= rule.high_percent:
    return "high"
  if percent >= rule.medium_percent:
    return "medium"
  if percent >= rule.low_percent:
    return "low"
  return None


async def evaluate_upload_warning(
  application: Application,
  *,
  usage_bytes: int,
  declared_size_bytes: int,
) -> dict | None:
  """Return admission warning and deliver one OA alert per level/cooldown window."""
  quota = max(int(application.quota_bytes), 1)
  projected = max(int(usage_bytes), 0) + max(int(declared_size_bytes), 0)
  ratio = projected / quota
  rule = await get_rule()
  level = _level(rule, ratio)
  if not level:
    return None
  percent = round(ratio * 100, 2)
  owner = await _application_owner(application)
  message = rule.message_template.format(
    app_name=application.shown_name or application.name,
    usage_percent=f"{percent:g}",
    quota_bytes=quota,
    projected_usage_bytes=projected,
  )
  cooldown_at = utc_now() - timedelta(seconds=max(settings.QUOTA_ALERT_COOLDOWN_SECONDS, 60))
  recent = await QuotaAlertEvent.find_one(
    QuotaAlertEvent.application_name == application.name,
    QuotaAlertEvent.level == level,
    QuotaAlertEvent.created_at >= cooldown_at,
  )
  if recent is None:
    event = QuotaAlertEvent(
      application_name=application.name,
      owner_username=owner.username if owner else "",
      level=level,
      usage_bytes=max(int(usage_bytes), 0),
      projected_usage_bytes=projected,
      quota_bytes=quota,
      usage_percent=percent,
      message=message,
    )
    await event.insert()
    if owner:
      link = f"{settings.FRONT_URL.rstrip('/')}/data/basic/application?expand={application.id}"
      try:
        result = await oa_service.send_agenda_message(
          owner.username,
          f"Storagent 配额{ {'low': '低', 'medium': '中', 'high': '高', 'blocked': '阻断'}[level] }级告警",
          message,
          link,
        )
        if not result.accepted:
          logger.warning("配额告警 OA 发送失败 app=%s: %s", application.name, result.detail)
      except Exception as error:
        logger.warning("配额告警 OA 发送异常 app=%s: %s", application.name, error)
  return {"level": level, "usage_percent": percent, "message": message}


def request_response(item: ApplicationExpansionRequest) -> dict:
  return {
    "id": str(item.id),
    "application_name": item.application_name,
    "application_shown_name": item.application_shown_name,
    "applicant_username": item.applicant_username,
    "reason": item.reason,
    "add_size_bytes": item.add_size_bytes,
    "status": item.status,
    "reviewer_username": item.reviewer_username,
    "review_note": item.review_note,
    "created_at": item.created_at,
    "reviewed_at": item.reviewed_at,
  }


async def create_expansion_request(application_id, payload, actor: User) -> dict:
  from src.modules.public import crud
  application = await crud.read_application_by_id(application_id)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  owner = await _application_owner(application)
  if not owner or owner.username != actor.username:
    raise CustomException(ErrorDesc.INSUFFICIENT_PERMISSIONS, "只能为自己创建的应用提交扩容申请")
  pending = await ApplicationExpansionRequest.find_one(
    ApplicationExpansionRequest.application_name == application.name,
    ApplicationExpansionRequest.status == "pending",
  )
  if pending:
    raise CustomException(ErrorDesc.STATUS_ERR, "该应用已有待处理的扩容申请")
  item = ApplicationExpansionRequest(
    application_name=application.name,
    application_shown_name=application.shown_name or application.name,
    applicant_username=actor.username,
    reason=payload.reason.strip(),
    add_size_bytes=payload.add_size_bytes,
  )
  await item.insert()
  return request_response(item)


async def list_expansion_requests(actor: User, *, all_requests: bool) -> dict:
  query = ApplicationExpansionRequest.find_all() if all_requests else ApplicationExpansionRequest.find(
    ApplicationExpansionRequest.applicant_username == actor.username,
  )
  rows = await query.sort("-created_at").to_list()
  return {"data": [request_response(item) for item in rows]}


async def review_expansion_request(request_id, payload, reviewer: User) -> dict:
  from bson import ObjectId
  from src.modules.public import crud, service
  item = await ApplicationExpansionRequest.find_one(ApplicationExpansionRequest.id == ObjectId(str(request_id)))
  if not item:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "扩容申请不存在")
  if item.status != "pending":
    raise CustomException(ErrorDesc.STATUS_ERR, "扩容申请已经处理")
  if payload.approved:
    application = await crud.read_application_by_name(item.application_name)
    if not application:
      raise CustomException(ErrorDesc.RES_NOT_FOUND, "申请关联的应用不存在")
    await service.update_application_quota(
      application.id,
      int(application.quota_bytes) + int(item.add_size_bytes),
      reviewer,
    )
    item.status = "approved"
  else:
    item.status = "rejected"
  item.reviewer_username = reviewer.username
  item.review_note = payload.review_note.strip()
  item.reviewed_at = utc_now()
  await item.save()
  return request_response(item)
