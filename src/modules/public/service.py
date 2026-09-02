import json
import logging
import asyncio
import weakref
from collections.abc import AsyncGenerator
from typing import List

from bson import ObjectId
from datetime import datetime, timedelta, timezone
from os import urandom

logger = logging.getLogger(__name__)

_quota_usage_refresh_tasks = weakref.WeakKeyDictionary()
_quota_usage_command_semaphores = weakref.WeakKeyDictionary()

from src.modules.auth.model import User
from src.modules.public.model import Region
from src.core import minio_op
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.core.exception import CustomException, ErrorDesc
from src.utils.helpers import (
  utc_now,
  generate_api_key
)
from src.modules.public.model import (
  APIKey,
  Region,
  Application,
  DEFAULT_APPLICATION_QUOTA_BYTES,
)
from src.core import sync as sync_module
from src.core.cors_origins import (
  MAX_DOMAINS_PER_APP,
  coerce_origin_list,
  normalize_origin,
  normalize_origin_list,
  refresh_from_application_entries,
)
from src.configs.configs import settings

async def get_endpoints(include_minio: bool = False) -> dict[str, List[str]]:
  """
  获取对外网关端点列表。

  `host` 是 MinIO 内网管理地址；浏览器和 App 后端应以 `domain` 为核心，
  经宿主 Nginx 的 /server/{region} 路由访问 Storagent。

  `minio_endpoint` 为内网直连地址，仅对管理员（include_minio=True）返回，
  匿名与普通登录用户不返回该字段，避免内网拓扑随公共接口外泄。
  """
  from src.modules.storage import service as storage_service
  minio_server_objs = await storage_service.get_minio_server_list()
  gateway_segments = {
    "beijing": "bj",
    "tianjin": "tj",
    "kunshan": "ks",
    "shenzhen": "sz",
    "hangzhou": "hz",
  }

  def public_domain(server) -> str:
    # `PUBLIC_DOMAIN` lets old Mongo records migrate without re-registering
    # every MinIO service. A record-level domain wins once synchronization has
    # written it, while host:port remains the final legacy fallback.
    return str(
      getattr(server, "domain", "") or settings.PUBLIC_DOMAIN or ""
    ).strip().rstrip("/")

  def public_endpoint(server) -> str:
    domain = public_domain(server)
    segment = gateway_segments.get(server.region.name)
    if domain and segment:
      return f"{settings.PUBLIC_SCHEME}://{domain}/server/{segment}"
    # Records from before `domain` was introduced stay usable during migration.
    return f"{settings.PUBLIC_SCHEME}://{server.host}:{server.server_port}"

  data = []
  for minio_server_obj in minio_server_objs["data"]:
    # Test doubles and pre-link records may not expose Region.id; the public
    # endpoint contract keeps the field nullable until the region is hydrated.
    region_id = getattr(minio_server_obj.region, "id", None)
    item = {
      "region_id": region_id,
      "server_id": minio_server_obj.id,
      "name": minio_server_obj.region.name,
      "shown_name": minio_server_obj.region.shown_name,
      "master": minio_server_obj.master,
      "domain": public_domain(minio_server_obj),
      "endpoint": public_endpoint(minio_server_obj),
    }
    if include_minio:
      item["minio_endpoint"] = f"{settings.PUBLIC_SCHEME}://{minio_server_obj.host}:{minio_server_obj.minio_port}"
    data.append(item)
  return dict[str, List[dict]](data=data)

async def test_endpoints() -> bytes:
  """
  测试端点
  """
  # 返回一个 512 Byte 的文件流
  # 使用 随机字符串, 防止缓存
  file_content: bytes = urandom(512)
  return file_content

async def _validate_application_name(name: str) -> None:
  """
  验证应用显示名称
  """

  if len(name) < 3 or len(name) > 32:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "应用别名长度不能小于3或大于32")
  # 符号仅允许连字符, 其他 deny
  for char in name:
    if char.isalnum() or char == "-":
      continue
    else:
      raise CustomException(ErrorDesc.INVALID_PARAMS, "应用别名只能包含连字符和字母数字")
  if name.strip().lower() == settings.OBJECT_ARCHIVE_BUCKET.strip().lower():
    raise CustomException(
      ErrorDesc.INVALID_PARAMS,
      "应用别名与系统归档存储桶保留名称冲突",
    )

async def create_region(
  name: str,
  shown_name: str) -> Region:
  """
  创建区域

  Args:
    name: 区域名称

  Returns:
    Region: 区域
  """
  region = await public_crud.create_region(name, shown_name)
  try:
    await sync_module.publish_region(name, shown_name)
  except Exception as e:
    await public_crud.delete_region_by_id(region.id)
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit("region.create", resource=name, detail=str(e), success=False)
    raise CustomException(ErrorDesc.SYNC_FAILED, f"Region 同步到 Etcd 失败: {e}")
  from src.core import audit
  audit.audit("region.create", resource=name, detail={"shown_name": shown_name})
  return region

async def offline_region(region_id) -> dict:
  """
  下线区域：从 Etcd 拓扑移除并删除本地 Region / MinIO 记录（禁止下线本节点 REGION）
  """
  from src.modules.storage import crud as storage_crud

  region = await public_crud.read_region_by_id(region_id)
  if not region:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "Region")
  if region.name == settings.REGION:
    raise CustomException(ErrorDesc.OPERATION_NOT_ALLOWED, "不能下线本节点区域")

  try:
    await sync_module.unpublish_server(region.name)
    await sync_module.unpublish_region(region.name)
    await sync_module.unpublish_topology_server(region.name)
  except Exception as e:
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit("region.offline", resource=region.name, detail=str(e), success=False)
    raise CustomException(ErrorDesc.SYNC_FAILED, f"Etcd 下线失败: {e}")

  server = await storage_crud.read_minio_server_by_region(region)
  if server:
    await server.delete()
  name = region.name
  await region.delete()
  from src.core import audit
  audit.audit("region.offline", resource=name)
  return {"message": f"区域 {name} 已下线"}

async def get_region_list() -> dict[str, List[Region]]:
  """
  获取区域列表

  Returns:
    List[Region]: 区域列表
  """
  region_objs = await public_crud.read_region_list()
  return dict[str, List[Region]](data=region_objs)


async def _notify_application_managers(
  application: Application,
  author: User,
) -> None:
  """Best-effort OA notification after the application is durable in Etcd."""
  from src.modules.auth import crud as auth_crud
  from src.modules.auth import oa as oa_service

  try:
    recipients = await auth_crud.list_users_with_permission("application_manage")
    link = f"{settings.FRONT_URL.rstrip('/')}/data/basic/application"
    deliveries = await asyncio.gather(*(
      oa_service.send_agenda_message(
        recipient.username,
        "Storagent 新应用待审批",
        (
          f"{author.name or author.username} 创建了应用 "
          f"{application.shown_name}（{application.name}），请及时处理。"
        ),
        link,
      )
      for recipient in recipients
    ), return_exceptions=True)
    for recipient, result in zip(recipients, deliveries):
      if isinstance(result, Exception):
        logger.warning(
          f"应用创建 OA 通知异常 app={application.name} "
          f"recipient={recipient.username}: {result}"
        )
      elif not result.accepted:
        logger.warning(
          f"应用创建 OA 通知失败 app={application.name} "
          f"recipient={recipient.username}: {result.detail or result.status}"
        )
  except Exception as error:
    logger.warning(f"应用创建 OA 通知失败 app={application.name}: {error}")


async def create_application(
  name: str,
  shown_name: str,
  description: str,
  current_user: User,
  domains: list[str] | None = None) -> dict:
  """
  创建应用
  """
  # 因为应用名用于存储桶创建, 所以需要一定的约束条件
  await _validate_application_name(name)
  # region_objs = await public_crud.read_many_region_by_ids(regions)
  # if len(region_objs) != len(regions):
  #   raise CustomException(ErrorDesc.RES_NOT_FOUND, "Region.id")
  # Claim the cross-region business identity before materializing the local
  # Mongo projection. CAS retries make APPID and shown_name globally unique.
  try:
    normalized_domains = normalize_origin_list(domains or [])
  except ValueError as error:
    raise CustomException(ErrorDesc.INVALID_PARAMS, str(error)) from error
  entry = {
    "shown_name": shown_name,
    "description": description,
    "enabled": False,
    "provisioning_status": "pending",
    "provisioning_error": "",
    "provisioning_updated_at": None,
    "quota_bytes": DEFAULT_APPLICATION_QUOTA_BYTES,
    "author_username": current_user.username,
    "author_name": current_user.name,
    "approver_username": "",
    "enabled_at": None,
    "updated_at": utc_now().isoformat(),
    "origin_region": settings.REGION,
    "domains": normalized_domains,
  }
  try:
    from src.core import etcd_op

    def create_mutator(applications: dict) -> dict:
      if name in applications:
        raise CustomException(ErrorDesc.NAME_EXISTED, "Application.name")
      for existing in applications.values():
        if (
          isinstance(existing, dict)
          and str(existing.get("shown_name") or "") == shown_name
        ):
          raise CustomException(ErrorDesc.NAME_EXISTED, "Application.shown_name")
      applications[name] = dict(entry)
      return applications

    applications = await etcd_op.merge_update_etcd_key(
      sync_module.ETCD_KEY_APPLICATIONS,
      create_mutator,
    )
    refresh_from_application_entries(applications)
    authoritative = applications[name]
  except CustomException:
    raise
  except Exception as error:
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "application.create",
      actor=current_user.username,
      resource=name,
      detail=str(error),
      success=False,
    )
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"Application 同步到 Etcd 失败: {error}",
    ) from error

  try:
    app, _created = await sync_module.upsert_application_from_etcd(
      name,
      authoritative,
    )
  except Exception as error:
    # The Etcd claim is already authoritative; periodic reconciliation will
    # repair this node without risking a second owner for the same bucket.
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "application.create",
      actor=current_user.username,
      resource=name,
      detail=f"本地投影失败: {error}",
      success=False,
    )
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      "应用已写入跨节点配置，本地数据正在自动收敛，请稍后刷新",
    ) from error

  await _notify_application_managers(app, current_user)
  from src.core import audit
  audit.audit("application.create", actor=current_user.username, resource=name)
  return await _application_response(app)


async def _require_can_manage_application(
  application: Application,
  current_user: User,
) -> None:
  from src.core.auth import _is_superadmin

  if await _is_superadmin(current_user):
    return
  if "application_manage" in set(getattr(current_user, "permissions", None) or []):
    return
  author = application.author
  if getattr(author, "id", None) is not None and str(author.id) == str(current_user.id):
    return
  if getattr(author, "username", None) == current_user.username:
    return
  raise CustomException(
    ErrorDesc.INSUFFICIENT_PERMISSIONS,
    "无权管理该应用的来源或删除该应用",
  )


async def _load_application_domains(application: Application) -> list[str]:
  """Etcd domains win when present; Mongo-only apps fall back to the local list."""
  from src.core import etcd_op

  try:
    applications = await etcd_op.pull_from_etcd_by_key(
      sync_module.ETCD_KEY_APPLICATIONS,
    )
  except CustomException:
    raise
  except Exception as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"读取应用跨节点配置失败: {error}",
    ) from error
  entry = applications.get(application.name)
  if isinstance(entry, dict):
    return coerce_origin_list(entry.get("domains") or [])
  return coerce_origin_list(getattr(application, "domains", None) or [])


async def _write_authoritative_application_domains(
  app_name: str,
  domains: list[str],
  *,
  base_entry: dict | None = None,
) -> dict:
  from src.core import etcd_op

  updated_at = utc_now().isoformat()
  normalized = list(domains)

  def mutator(applications: dict) -> dict:
    current = applications.get(app_name)
    if not isinstance(current, dict):
      if not isinstance(base_entry, dict):
        raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
      current = dict(base_entry)
      applications[app_name] = current
    current["domains"] = normalized
    current["updated_at"] = updated_at
    return applications

  applications = await etcd_op.merge_update_etcd_key(
    sync_module.ETCD_KEY_APPLICATIONS,
    mutator,
  )
  refresh_from_application_entries(applications)
  return dict(applications[app_name])


async def _project_authoritative_application(
  app_name: str,
  authoritative: dict,
  current: Application,
) -> Application:
  try:
    projected, _created = await sync_module.upsert_application_from_etcd(
      app_name,
      authoritative,
    )
    return projected
  except Exception as error:
    logger.warning(f"应用本地投影延迟 app={app_name}: {error}")
    current.domains = coerce_origin_list(authoritative.get("domains") or [])
    return current


async def add_application_domain(
  application_id: ObjectId,
  domain: str,
  current_user: User,
) -> dict:
  application = await public_crud.read_application_by_id(application_id)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  await _require_can_manage_application(application, current_user)
  try:
    origin = normalize_origin(domain)
  except ValueError as error:
    raise CustomException(ErrorDesc.INVALID_PARAMS, str(error)) from error

  current_domains = await _load_application_domains(application)
  if origin in current_domains:
    raise CustomException(ErrorDesc.RES_DATA_NOT_CHANGED, "该来源已存在")
  if len(current_domains) >= MAX_DOMAINS_PER_APP:
    raise CustomException(
      ErrorDesc.INVALID_PARAMS,
      f"每个应用最多 {MAX_DOMAINS_PER_APP} 个来源",
    )
  current_domains.append(origin)
  try:
    authoritative = await _write_authoritative_application_domains(
      application.name,
      current_domains,
      base_entry=sync_module.application_to_etcd_entry(application),
    )
  except CustomException:
    raise
  except Exception as error:
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "application.domain.add",
      actor=current_user.username,
      resource=application.name,
      detail=str(error),
      success=False,
    )
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"来源同步到 Etcd 失败: {error}",
    ) from error

  application = await _project_authoritative_application(
    application.name,
    authoritative,
    application,
  )
  from src.core import audit
  audit.audit(
    "application.domain.add",
    actor=current_user.username,
    resource=application.name,
    detail=origin,
  )
  return await _application_response(application)


async def delete_application_domain(
  application_id: ObjectId,
  domain: str,
  current_user: User,
) -> dict:
  application = await public_crud.read_application_by_id(application_id)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  await _require_can_manage_application(application, current_user)
  try:
    origin = normalize_origin(domain)
  except ValueError as error:
    raise CustomException(ErrorDesc.INVALID_PARAMS, str(error)) from error

  current_domains = await _load_application_domains(application)
  if origin not in current_domains:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "来源不存在")
  remaining = [item for item in current_domains if item != origin]
  try:
    authoritative = await _write_authoritative_application_domains(
      application.name,
      remaining,
      base_entry=sync_module.application_to_etcd_entry(application),
    )
  except CustomException:
    raise
  except Exception as error:
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "application.domain.delete",
      actor=current_user.username,
      resource=application.name,
      detail=str(error),
      success=False,
    )
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"来源同步到 Etcd 失败: {error}",
    ) from error

  application = await _project_authoritative_application(
    application.name,
    authoritative,
    application,
  )
  from src.core import audit
  audit.audit(
    "application.domain.delete",
    actor=current_user.username,
    resource=application.name,
    detail=origin,
  )
  return await _application_response(application)


async def delete_application(
  application_id: ObjectId,
  current_user: User,
) -> dict:
  application = await public_crud.read_application_by_id(application_id)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  await _require_can_manage_application(application, current_user)
  app_name = application.name
  try:
    await sync_module.unpublish_application(app_name)
  except Exception as error:
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "application.delete",
      actor=current_user.username,
      resource=app_name,
      detail=str(error),
      success=False,
    )
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"应用删除同步到 Etcd 失败: {error}",
    ) from error

  deleted = await public_crud.delete_application_by_id(application_id)
  if not deleted:
    logger.warning(f"应用已从 Etcd 移除，本地投影缺失 app={app_name}")
  from src.core import audit
  audit.audit("application.delete", actor=current_user.username, resource=app_name)
  return {"message": "应用已删除"}

async def get_application_list() -> dict[str, List[Application]]:
  """
  获取应用列表
  """
  application_objs = await public_crud.read_application_list()
  data = await asyncio.gather(*(
    _application_response(application)
    for application in application_objs
  ))
  return {"data": list(data)}


def _quota_cache_is_fresh(application: Application) -> bool:
  updated_at = getattr(application, "quota_usage_updated_at", None)
  if not updated_at:
    return False
  if updated_at.tzinfo is None:
    updated_at = updated_at.replace(tzinfo=timezone.utc)
  else:
    updated_at = updated_at.astimezone(timezone.utc)
  age = (utc_now() - updated_at).total_seconds()
  return age <= max(float(settings.APPLICATION_QUOTA_USAGE_CACHE_SECONDS), 0.0)


def _quota_usage_command_semaphore() -> asyncio.Semaphore:
  loop = asyncio.get_running_loop()
  limit = max(int(settings.APPLICATION_QUOTA_USAGE_MAX_CONCURRENCY), 1)
  existing = _quota_usage_command_semaphores.get(loop)
  if existing is None or existing[0] != limit:
    existing = (limit, asyncio.Semaphore(limit))
    _quota_usage_command_semaphores[loop] = existing
  return existing[1]


async def _get_bucket_usage_limited(
  server_name: str,
  bucket_name: str,
) -> tuple[bool, int, str]:
  async with _quota_usage_command_semaphore():
    return await minio_op.get_bucket_usage_bytes(
      server_name,
      bucket_name,
      timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
    )


async def _collect_application_quota_usage(
  application: Application,
) -> tuple[int, datetime | None, dict[str, str]]:
  cached = max(int(getattr(application, "quota_usage_bytes", 0) or 0), 0)
  if not application.enabled:
    return cached, getattr(application, "quota_usage_updated_at", None), {}

  from src.modules.files import quota as upload_quota
  failures: dict[str, str] = {}
  try:
    observed = await upload_quota.get_observed_usage_bytes(application.name)
  except Exception as error:
    observed = cached
    failures["quota_state"] = str(error)

  server_names = sorted(set(await storage_crud.read_minio_server_names()))
  if not server_names:
    failures["*"] = "没有可读取用量的 MinIO 站点"
    return max(cached, observed), getattr(
      application,
      "quota_usage_updated_at",
      None,
    ), failures
  results = await asyncio.gather(*(
    _get_bucket_usage_limited(
      server_name,
      application.name,
    )
    for server_name in server_names
  ))
  failures.update({
    server_name: error
    for server_name, (success, _usage, error) in zip(server_names, results)
    if not success
  })
  usages = [usage for success, usage, _error in results if success]
  if not usages:
    return max(cached, observed), getattr(
      application,
      "quota_usage_updated_at",
      None,
    ), failures
  usage = max(cached, observed, *(max(int(value), 0) for value in usages))
  updated_at = utc_now()
  application.quota_usage_bytes = usage
  application.quota_usage_updated_at = updated_at
  await application.save()
  return usage, updated_at, failures


async def refresh_application_quota_usage(
  application: Application,
  *,
  force: bool = False,
  require_all: bool = False,
) -> int:
  cached = max(int(getattr(application, "quota_usage_bytes", 0) or 0), 0)
  if not force and _quota_cache_is_fresh(application):
    return cached

  loop = asyncio.get_running_loop()
  tasks = _quota_usage_refresh_tasks.setdefault(loop, {})
  task = tasks.get(application.name)
  if task is None or task.done():
    task = asyncio.create_task(_collect_application_quota_usage(application))
    tasks[application.name] = task
  try:
    usage, updated_at, failures = await asyncio.shield(task)
  finally:
    if task.done() and tasks.get(application.name) is task:
      tasks.pop(application.name, None)

  application.quota_usage_bytes = usage
  if updated_at is not None:
    application.quota_usage_updated_at = updated_at
  if failures and require_all:
    detail = "; ".join(
      f"{server_name}: {error}" for server_name, error in failures.items()
    )
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, f"读取五地存储用量失败: {detail}")
  return usage


async def refresh_application_quota_aggregates_once() -> dict[str, int | str]:
  """Refresh the authoritative quota aggregate outside caller request paths."""
  if str(settings.REGION).strip().lower() != str(settings.SYNC_AUTHORITY_REGION).strip().lower():
    return {
      "status": "skipped",
      "processed": 0,
      "succeeded": 0,
      "failed": 0,
      "skipped": 0,
      "deferred": 0,
    }

  from src.modules.files import quota as upload_quota

  batch_size = min(max(int(settings.APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE), 1), 1000)
  enabled_count, selected_applications = await asyncio.gather(
    public_crud.count_enabled_applications(),
    public_crud.read_quota_refresh_candidates(batch_size),
  )
  result: dict[str, int | str] = {
    "status": "completed",
    "processed": 0,
    "succeeded": 0,
    "failed": 0,
    "skipped": 0,
    "deferred": max(int(enabled_count) - len(selected_applications), 0),
  }
  for application in selected_applications:
    result["processed"] = int(result["processed"]) + 1
    try:
      usage = await refresh_application_quota_usage(
        application,
        force=True,
        require_all=True,
      )
      await upload_quota.reconcile_usage_aggregate(
        application.name,
        usage,
        quota_bytes=int(
          getattr(application, "quota_bytes", DEFAULT_APPLICATION_QUOTA_BYTES),
        ),
      )
      result["succeeded"] = int(result["succeeded"]) + 1
    except Exception as error:
      result["failed"] = int(result["failed"]) + 1
      logger.warning(
        "应用配额聚合刷新失败 app=%s error=%s",
        application.name,
        type(error).__name__,
      )
    finally:
      # Record attempts independently from successful samples. Otherwise one
      # persistently failing application remains first forever and prevents
      # later applications from ever receiving a bounded refresh slot.
      application.quota_usage_attempted_at = utc_now()
      try:
        await application.save()
      except Exception as error:
        logger.warning(
          "应用配额刷新尝试时间写入失败 app=%s error=%s",
          application.name,
          type(error).__name__,
        )
  if result["deferred"]:
    logger.info(
      "应用配额聚合本轮已按批次限流，等待后续周期 app_count=%s",
      result["deferred"],
    )
  return result


async def _application_response(
  application: Application,
  *,
  force_usage: bool = False,
  require_all_usage: bool = False,
) -> dict:
  author = application.author
  # Mongo projections created by the cross-region sync worker can retain a
  # Beanie Link even when the normal CRUD path requested fetch_links=True.
  # Resolve it before returning the public response so FastAPI never tries to
  # validate a Link against SimpleUserResponse.
  if not hasattr(author, "username") and hasattr(author, "fetch"):
    resolved_author = await author.fetch()
    if resolved_author is not author:
      author = resolved_author
  if not hasattr(author, "username"):
    raise CustomException(
      ErrorDesc.RES_NOT_FOUND,
      "应用创建者信息暂时不可用，请稍后刷新重试",
    )
  usage = await refresh_application_quota_usage(
    application,
    force=force_usage,
    require_all=require_all_usage,
  )
  quota = max(int(application.quota_bytes), 1)
  return {
    "id": application.id,
    "name": application.name,
    "shown_name": application.shown_name,
    "created_at": application.created_at,
    "updated_at": application.updated_at,
    "description": application.description,
    "enabled": application.enabled,
    "enabled_at": application.enabled_at,
    "provisioning_status": application.provisioning_status,
    "provisioning_error": application.provisioning_error,
    "provisioning_updated_at": application.provisioning_updated_at,
    "quota_bytes": quota,
    "quota_usage_bytes": usage,
    "quota_usage_ratio": usage / quota,
    "quota_usage_updated_at": application.quota_usage_updated_at,
    "domains": list(getattr(application, "domains", None) or []),
    "author": {
      "id": author.id,
      "username": author.username,
      "name": author.name,
    },
  }


async def get_application_quota_usage(
  app_name: str,
  *,
  force: bool = True,
  require_all: bool = True,
) -> tuple[int, int]:
  application = await public_crud.read_application_by_name(app_name)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  usage = await refresh_application_quota_usage(
    application,
    force=force,
    require_all=require_all,
  )
  return int(application.quota_bytes), usage


def _newest_timestamp(*values: datetime | None) -> datetime | None:
  normalized = []
  for value in values:
    if value is None:
      continue
    normalized.append(
      value.replace(tzinfo=timezone.utc)
      if value.tzinfo is None else value.astimezone(timezone.utc)
    )
  return max(normalized) if normalized else None


async def get_application_quota_usage_aggregate(app_name: str) -> dict:
  """Serve diagnostics from persisted App/Etcd aggregates, never a MinIO scan."""
  application = await public_crud.read_application_by_name(app_name)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")

  cached_usage = max(int(getattr(application, "quota_usage_bytes", 0) or 0), 0)
  cached_at = getattr(application, "quota_usage_updated_at", None)
  state: dict = {}
  aggregate_error = ""
  try:
    from src.modules.files import quota as upload_quota
    state = await upload_quota.get_usage_aggregate(app_name)
  except Exception as error:
    aggregate_error = str(error)

  state_usage = max(int(state.get("usage_bytes") or 0), 0)
  logical_usage_initialized = bool(state.get("initialized"))
  updated_at = _newest_timestamp(cached_at, state.get("updated_at"))
  now = utc_now()
  max_age = max(float(settings.DIAGNOSTIC_QUOTA_AGGREGATE_MAX_AGE_SECONDS), 0.0)
  # Once the logical counter has been initialized, every supported upload,
  # deletion and recovery path updates it transactionally in Etcd. Its
  # timestamp represents the last mutation, not a periodic sample, so an idle
  # application must not fail caller diagnostics merely because no object has
  # changed recently. An uninitialized counter still relies on a sampled
  # projection and remains subject to the freshness window.
  if logical_usage_initialized and not aggregate_error:
    fresh = True
    source = "logical_usage_aggregate"
    freshness_basis = "event_sourced"
  else:
    fresh = bool(
      updated_at is not None
      and (now - updated_at).total_seconds() <= max_age
    )
    source = "application_usage_snapshot"
    freshness_basis = "sampled"
  return {
    "limit_bytes": max(int(application.quota_bytes or 0), 0),
    # Choose the safe high watermark when a Mongo projection lags the Etcd
    # logical counter; diagnostics must never promise quota that admission
    # would subsequently reject.
    "used_bytes": max(cached_usage, state_usage),
    "updated_at": updated_at,
    "fresh": fresh,
    "source": source,
    "freshness_basis": freshness_basis,
    "aggregate_error": aggregate_error,
  }


async def get_application_quota_limit(app_name: str, *, client=None) -> int:
  """Read the cross-region authoritative quota directly from Etcd."""
  from src.core import etcd_op

  try:
    applications = await etcd_op.pull_from_etcd_by_key(
      sync_module.ETCD_KEY_APPLICATIONS,
      client=client,
    )
    entry = applications.get(app_name)
    if not isinstance(entry, dict):
      raise ValueError("应用未写入跨节点配置")
    quota_bytes = int(entry.get("quota_bytes"))
    if quota_bytes <= 0:
      raise ValueError("应用配额不是正整数")
    return quota_bytes
  except CustomException:
    raise
  except Exception as error:
    raise CustomException(
      ErrorDesc.SYNC_FAILED,
      f"无法读取应用权威配额，上传已安全拒绝: {error}",
    ) from error


async def _read_authoritative_application(app_name: str) -> dict:
  from src.core import etcd_op

  applications = await etcd_op.pull_from_etcd_by_key(
    sync_module.ETCD_KEY_APPLICATIONS,
  )
  entry = applications.get(app_name)
  if not isinstance(entry, dict):
    raise CustomException(ErrorDesc.SYNC_FAILED, "应用未写入跨节点配置")
  return dict(entry)


async def _write_authoritative_application_quota(
  app_name: str,
  quota_bytes: int,
) -> dict:
  """Update only quota fields so a delayed Mongo projection cannot overwrite APP state."""
  from src.core import etcd_op

  updated_at = utc_now().isoformat()

  def mutator(applications: dict) -> dict:
    current = applications.get(app_name)
    if not isinstance(current, dict):
      raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
    current["quota_bytes"] = int(quota_bytes)
    current["updated_at"] = updated_at
    return applications

  applications = await etcd_op.merge_update_etcd_key(
    sync_module.ETCD_KEY_APPLICATIONS,
    mutator,
  )
  return dict(applications[app_name])


async def _restore_bucket_quotas(
  bucket_name: str,
  previous: dict[str, int | None],
) -> dict[str, str]:
  rollback_errors: dict[str, str] = {}
  for server_name, quota in previous.items():
    if quota is None:
      success, error = await minio_op.clear_bucket_hard_quota(
        server_name,
        bucket_name,
        timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
      )
    else:
      success, error = await minio_op.set_bucket_hard_quota(
        server_name,
        bucket_name,
        quota,
        timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
      )
    if not success:
      rollback_errors[server_name] = error
  return rollback_errors


async def update_application_quota(
  application_id: ObjectId,
  quota_bytes: int,
  current_user: User,
) -> dict:
  from src.modules.files import quota as upload_quota

  application = await public_crud.read_application_by_id(application_id)
  if not application:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  application_name = application.name

  try:
    async with sync_module.application_replication_lock(application_name):
      authoritative = await _read_authoritative_application(application_name)
      enabled = bool(authoritative.get("enabled", False))

      async def load_current_usage() -> int:
        nonlocal application
        application = await public_crud.read_application_by_id(application_id)
        if not application:
          raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
        return await refresh_application_quota_usage(
          application,
          force=enabled,
          require_all=enabled,
        )

      async with upload_quota.quota_update_guard(
        application_name,
        usage_loader=load_current_usage,
      ) as quota_state:
        usage, active_reserved = quota_state
        committed_and_reserved = usage + active_reserved
        if quota_bytes < committed_and_reserved:
          raise CustomException(
            ErrorDesc.INVALID_PARAMS,
            (
              f"配额不能低于当前使用量与活动上传预留之和 "
              f"{committed_and_reserved} 字节"
            ),
          )

        server_names = (
          sorted(set(await storage_crud.read_minio_server_names()))
          if enabled else []
        )
        previous: dict[str, int | None] = {}
        if server_names:
          current_results = await asyncio.gather(*(
            minio_op.get_bucket_hard_quota(
              server_name,
              application_name,
              timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
            )
            for server_name in server_names
          ))
          read_failures = {
            server_name: error
            for server_name, (success, _current, error) in zip(
              server_names,
              current_results,
            )
            if not success
          }
          if read_failures:
            detail = "; ".join(
              f"{server_name}: {error}"
              for server_name, error in read_failures.items()
            )
            raise CustomException(
              ErrorDesc.MINIO_ACCESS_FAILED,
              f"读取原配额失败: {detail}",
            )
          previous = {
            server_name: current
            for server_name, (_success, current, _error) in zip(
              server_names,
              current_results,
            )
          }

        changed: dict[str, int | None] = {}
        authoritative_committed = False
        try:
          for server_name in server_names:
            if previous[server_name] == quota_bytes:
              continue
            changed[server_name] = previous[server_name]
            success, error = await minio_op.set_bucket_hard_quota(
              server_name,
              application_name,
              quota_bytes,
              timeout=settings.MINIO_OPERATION_TIMEOUT_SECONDS,
            )
            if not success:
              raise RuntimeError(f"{server_name}: {error}")

          upload_quota.raise_if_quota_lock_lost()
          authoritative = await _write_authoritative_application_quota(
            application_name,
            quota_bytes,
          )
          authoritative_committed = True
          # Keep the compact request-time admission document in lockstep with
          # the authoritative quota. The helper is a no-op before the
          # authority worker has migrated this application.
          await upload_quota.set_admission_quota(
            application_name,
            quota_bytes,
          )
          upload_quota.raise_if_quota_lock_lost()
        except BaseException as error:
          compensation_errors = []
          if not authoritative_committed:
            try:
              rollback_errors = await _restore_bucket_quotas(application_name, changed)
              if rollback_errors:
                compensation_errors.append(f"MinIO 回滚失败: {rollback_errors}")
            except BaseException as rollback_error:
              compensation_errors.append(f"MinIO 回滚异常: {rollback_error}")

          if authoritative_committed:
            raise CustomException(
              ErrorDesc.SYNC_FAILED,
              "配额已写入跨节点权威配置，站点正在自动收敛，请稍后刷新确认",
            ) from error

          if isinstance(error, asyncio.CancelledError):
            if compensation_errors:
              raise CustomException(
                ErrorDesc.SYNC_FAILED,
                "应用配额锁已失效且回滚不完整: " + "；".join(compensation_errors),
              ) from error
            raise
          detail = str(error)
          if compensation_errors:
            detail += "；" + "；".join(compensation_errors)
          raise CustomException(
            ErrorDesc.MINIO_ACCESS_FAILED,
            f"应用配额更新失败: {detail}",
          ) from error

        try:
          projected, _created = await sync_module.upsert_application_from_etcd(
            application_name,
            authoritative,
          )
          application = projected
        except Exception as error:
          logger.warning(
            f"应用配额本地投影延迟 app={application_name}: {error}"
          )
          application.quota_bytes = quota_bytes

        from src.core import audit
        audit.audit(
          "application.quota.update",
          actor=current_user.username,
          resource=application_name,
          detail={
            "quota_bytes": quota_bytes,
            "usage_bytes": usage,
            "active_reserved_bytes": active_reserved,
          },
        )
        return await _application_response(application)
  except sync_module.ReplicationLockBusyError as error:
    raise CustomException(ErrorDesc.STATUS_ERR, str(error))

def _sse_line(payload: dict) -> bytes:
  return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8")


async def enable_application(
  application_id: ObjectId,
  current_user: User) -> AsyncGenerator[bytes, None]:
  """
  启用应用：只有全连接复制策略严格验收通过后才开放应用。
  """

  async def _emit(payload: dict) -> AsyncGenerator[bytes, None]:
    logger.info(
      "enable_application SSE step=%s status=%s server=%s msg=%s",
      payload.get("step"),
      payload.get("status"),
      payload.get("server_name"),
      payload.get("message"),
    )
    yield _sse_line(payload)

  async def _save_state(
    application: Application,
    status: str,
    *,
    error: str = "",
    enabled: bool | None = None,
  ) -> None:
    now = utc_now()
    application.provisioning_status = status
    application.provisioning_error = error
    application.provisioning_updated_at = now
    application.updated_at = now
    if enabled is not None:
      application.enabled = enabled
      if not enabled:
        application.enabled_at = None
    await application.save()
    await sync_module.publish_application(application)

  application_obj = await public_crud.read_application_by_id(application_id)
  if not application_obj:
    async for chunk in _emit({
      "step": "validate",
      "server_name": None,
      "status": "failed",
      "message": "应用不存在，无法授权",
    }):
      yield chunk
    async for chunk in _emit({
      "step": "done",
      "server_name": None,
      "status": "failed",
      "message": "授权失败",
    }):
      yield chunk
    return
  if application_obj.enabled:
    async for chunk in _emit({
      "step": "validate",
      "server_name": None,
      "status": "failed",
      "message": "应用已启用，无需重复授权",
    }):
      yield chunk
    async for chunk in _emit({
      "step": "done",
      "server_name": None,
      "status": "failed",
      "message": "授权失败",
    }):
      yield chunk
    return

  async for chunk in _emit({
    "step": "start",
    "server_name": None,
    "status": "running",
    "message": "开始授权并初始化全连接复制策略",
  }):
    yield chunk

  failure_step = "validate"
  try:
    async with sync_module.application_replication_lock(application_obj.name):
      application_obj = await public_crud.read_application_by_id(application_id)
      if not application_obj:
        raise RuntimeError("应用在授权期间被删除")
      if application_obj.enabled:
        async for chunk in _emit({
          "step": "done",
          "server_name": None,
          "status": "success",
          "message": "应用已由其他节点完成授权",
        }):
          yield chunk
        return

      application_obj.approver = current_user
      failure_step = "persist"
      await _save_state(
        application_obj,
        "provisioning",
        enabled=False,
      )

      server_names = sorted(set(await storage_crud.read_minio_server_names()))
      if len(server_names) < 2:
        raise RuntimeError("至少需要两个 MinIO 站点才能初始化全连接复制策略")
      async for chunk in _emit({
        "step": "bucket_phase",
        "server_name": None,
        "status": "running",
        "message": f"检查 {len(server_names)} 个站点的存储桶 {application_obj.name}",
      }):
        yield chunk

      failure_step = "bucket_create"
      create_errors: dict[str, str] = {}
      for server_name in server_names:
        async for chunk in _emit({
          "step": "bucket_check",
          "server_name": server_name,
          "status": "running",
          "message": f"检查站点 {server_name} 的存储桶",
        }):
          yield chunk
        existed = await minio_op.check_server_bucket_existed(
          server_name,
          application_obj.name,
        )
        if existed:
          async for chunk in _emit({
            "step": "bucket_check",
            "server_name": server_name,
            "status": "skipped",
            "message": f"站点 {server_name} 的存储桶已存在",
          }):
            yield chunk
          continue
        success, err = await minio_op.create_bucket(server_name, application_obj.name)
        if not success:
          create_errors[server_name] = str(err)
          async for chunk in _emit({
            "step": "bucket_create",
            "server_name": server_name,
            "status": "failed",
            "message": f"站点 {server_name} 创建存储桶失败: {err}",
          }):
            yield chunk
        else:
          async for chunk in _emit({
            "step": "bucket_create",
            "server_name": server_name,
            "status": "ok",
            "message": f"站点 {server_name} 创建存储桶成功",
          }):
            yield chunk
      if create_errors:
        raise RuntimeError(f"部分站点创建存储桶失败: {create_errors}")

      failure_step = "bucket_versioning"
      for server_name in server_names:
        async for chunk in _emit({
          "step": "bucket_versioning",
          "server_name": server_name,
          "status": "running",
          "message": f"启用站点 {server_name} 的存储桶版本控制",
        }):
          yield chunk
        success, err = await minio_op.enable_bucket_versioning(
          server_name,
          application_obj.name,
        )
        if not success:
          raise RuntimeError(f"站点 {server_name} 开启版本控制失败: {err}")
        async for chunk in _emit({
          "step": "bucket_versioning",
          "server_name": server_name,
          "status": "ok",
          "message": f"站点 {server_name} 已启用版本控制",
        }):
          yield chunk

      failure_step = "bucket_quota"
      async for chunk in _emit({
        "step": "bucket_quota",
        "server_name": None,
        "status": "running",
        "message": f"设置 {len(server_names)} 个站点的应用存储配额",
      }):
        yield chunk
      await sync_module.ensure_bucket_quotas(
        application_obj.name,
        int(getattr(
          application_obj,
          "quota_bytes",
          DEFAULT_APPLICATION_QUOTA_BYTES,
        )),
        server_names,
      )
      async for chunk in _emit({
        "step": "bucket_quota",
        "server_name": None,
        "status": "ok",
        "message": "各站点应用存储配额已生效",
      }):
        yield chunk

      failure_step = "replicate"
      async for chunk in _emit({
        "step": "replicate",
        "server_name": None,
        "status": "running",
        "message": f"补齐并验收 {len(server_names) * (len(server_names) - 1)} 条有向复制规则",
      }):
        yield chunk
      policy = await sync_module.setup_bucket_replication(
        application_obj.name,
        server_names,
      )
      async for chunk in _emit({
        "step": "replicate",
        "server_name": None,
        "status": "ok",
        "message": "全连接复制策略验收通过",
        "detail": policy,
      }):
        yield chunk

      failure_step = "persist"
      await storage_crud.bulk_create_minio_bucket(application_obj)
      application_obj.enabled_at = utc_now()
      await _save_state(application_obj, "ready", enabled=True)
      async for chunk in _emit({
        "step": "persist",
        "server_name": None,
        "status": "ok",
        "message": "应用已启用并同步到所有节点",
      }):
        yield chunk

  except sync_module.ReplicationLockBusyError as e:
    async for chunk in _emit({
      "step": "validate",
      "server_name": None,
      "status": "failed",
      "message": str(e),
    }):
      yield chunk
    async for chunk in _emit({
      "step": "done",
      "server_name": None,
      "status": "failed",
      "message": "授权任务已在其他节点执行",
    }):
      yield chunk
    return
  except Exception as e:
    logger.warning(f"应用授权失败 {application_obj.name}: {e}")
    try:
      await _save_state(
        application_obj,
        "failed",
        error=str(e),
        enabled=False,
      )
    except Exception as state_error:
      logger.warning(f"应用授权失败状态同步异常 {application_obj.name}: {state_error}")
    async for chunk in _emit({
      "step": failure_step,
      "server_name": None,
      "status": "failed",
      "message": str(e),
    }):
      yield chunk
    async for chunk in _emit({
      "step": "done",
      "server_name": None,
      "status": "failed",
      "message": "授权失败，可修复后直接重试",
    }):
      yield chunk
    return

  async for chunk in _emit({
    "step": "done",
    "server_name": None,
    "status": "success",
    "message": "授权成功",
  }):
    yield chunk

async def get_users_enabled_application_list(
  current_user: User) -> List[Application]:
  """
  获取用户启用的应用列表
  """
  app_objs = await public_crud.read_users_enabled_application_list(current_user)
  return dict[str, List[Application]](data=app_objs)
  
async def create_api_key(
  application_id: ObjectId,
  expired_at: datetime | None,
  current_user: User) -> APIKey:
  """
  创建API密钥
  """
  application_obj = await public_crud.read_application_by_id(application_id)
  if not application_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "应用不存在")
  if (
    not application_obj.enabled
    or application_obj.provisioning_status != "ready"
  ):
    raise CustomException(ErrorDesc.STATUS_ERR, "应用复制策略尚未就绪")
  if application_obj.author.id != current_user.id:
    raise CustomException(ErrorDesc.RES_NOT_BELONG_TO_USER, "应用不属于当前用户")
  if expired_at:
    expired_at = expired_at.replace(tzinfo=utc_now().tzinfo)
    if expired_at < utc_now():
      raise CustomException(ErrorDesc.INVALID_PARAMS, "过期时间不能小于当前时间")
  else:
    expired_at = utc_now() + timedelta(days=36500) # 100年, 设置一个特别大的时间, 视同为永久有效
    # 只精确到秒, 与用户设置时间保持一致
    expired_at = expired_at.replace(second=0, microsecond=0)
  key = generate_api_key()
  # 生成一个唯一的API密钥
  while await public_crud.read_api_key_by_key(key):
    key = generate_api_key()
  api_key_obj = await public_crud.create_api_key(application_obj, key, expired_at)
  try:
    await sync_module.publish_api_key(api_key_obj)
  except Exception as e:
    await api_key_obj.delete()
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "api_key.create",
      actor=current_user.username,
      resource=str(application_id),
      detail=str(e),
      success=False,
    )
    raise CustomException(ErrorDesc.SYNC_FAILED, f"API Key 同步到 Etcd 失败: {e}")
  from src.core import audit
  audit.audit("api_key.create", actor=current_user.username, resource=str(api_key_obj.id))
  # 仅创建响应返回一次明文
  api_key_obj.key = key
  return api_key_obj

async def _resolve_linked_application(application) -> Application | None:
  """Return a fetched Application, even when Beanie left an unresolved Link."""
  if application is None:
    return None
  if hasattr(application, "name"):
    return application
  if hasattr(application, "fetch"):
    try:
      resolved = await application.fetch()
    except Exception:
      resolved = None
    if resolved is not None and hasattr(resolved, "name"):
      return resolved
  application_id = sync_module._linked_document_id(application)
  if not application_id:
    return None
  return await public_crud.read_application_by_id(application_id)


def _api_key_application_summary(application, fallback_id: str | None) -> dict:
  if application is not None and hasattr(application, "name"):
    return {
      "id": application.id,
      "name": application.name,
      "shown_name": application.shown_name or application.name,
    }
  return {
    "id": fallback_id,
    "name": "",
    "shown_name": "应用已删除",
  }


async def get_api_key_list(
  current_user: User) -> List[APIKey]:
  """
  获取API密钥列表。
  普通用户：本人启用应用下的有效密钥 + 被管理员吊销的密钥。
  管理员：全部有效密钥 + 管理员吊销记录（可吊销他人密钥）。
  """
  from src.modules.auth import crud as user_crud

  admin_role = await user_crud.get_admin_role()
  is_admin = bool(
    admin_role
    and any(getattr(role, "id", None) == admin_role.id for role in (current_user.roles or []))
  )
  if is_admin:
    api_key_objs = await public_crud.read_all_api_keys(include_admin_destroyed=True)
  else:
    users_app_objs = await public_crud.read_users_enabled_application_list(current_user)
    api_key_objs = await public_crud.read_api_key_by_app(
      users_app_objs,
      include_admin_destroyed=True,
    )
  data = []
  for api_key_obj in api_key_objs:
    hint = api_key_obj.key_hint or "************"
    application = await _resolve_linked_application(api_key_obj.application)
    fallback_id = sync_module._linked_document_id(api_key_obj.application)
    if application is None and fallback_id is None:
      continue
    data.append({
      "id": api_key_obj.id,
      "key": hint,
      "application": _api_key_application_summary(application, fallback_id),
      "expired_at": api_key_obj.expired_at,
      "deleted": bool(api_key_obj.deleted),
      "destory_by_admin": bool(getattr(api_key_obj, "destory_by_admin", False)),
    })
  return dict[str, List[APIKey]](data=data)

async def revoke_api_key(
  api_key_id: ObjectId,
  current_user: User) -> dict:
  """
  吊销 API 密钥。所有者可吊销本人密钥；管理员可吊销任意密钥（标注 destory_by_admin）。
  """
  from src.modules.auth import crud as user_crud

  api_key_obj = await public_crud.read_api_key_by_id(api_key_id)
  if not api_key_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "API密钥不存在")
  application_obj = await _resolve_linked_application(api_key_obj.application)
  is_owner = bool(
    application_obj
    and sync_module._linked_document_id(application_obj.author) == str(current_user.id)
  )
  admin_role = await user_crud.get_admin_role()
  is_admin = bool(
    admin_role
    and any(getattr(role, "id", None) == admin_role.id for role in (current_user.roles or []))
  )
  if not is_owner and not is_admin:
    raise CustomException(ErrorDesc.RES_NOT_BELONG_TO_USER, "API密钥不属于当前用户")
  if api_key_obj.deleted:
    raise CustomException(ErrorDesc.STATUS_ERR, "API密钥已吊销")
  destory_by_admin = bool(is_admin and not is_owner)
  await public_crud.delete_api_key_by_id(api_key_id, destory_by_admin=destory_by_admin)
  try:
    revoked = await public_crud.read_api_key_by_id(api_key_id)
    if revoked:
      await sync_module.publish_api_key(revoked)
  except Exception as e:
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit("api_key.revoke", actor=current_user.username, resource=str(api_key_id), detail=str(e), success=False)
    raise CustomException(ErrorDesc.SYNC_FAILED, f"API Key 吊销同步到 Etcd 失败: {e}")
  from src.core import audit
  audit.audit(
    "api_key.revoke",
    actor=current_user.username,
    resource=str(api_key_id),
    detail="destory_by_admin" if destory_by_admin else "owner",
  )
  return {"message": "API密钥已吊销"}
