import json
import logging
from collections.abc import AsyncGenerator
from typing import List

from bson import ObjectId
from datetime import datetime, timedelta
from os import urandom

logger = logging.getLogger(__name__)

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
  Application
)
from src.core import sync as sync_module
from src.configs.configs import settings

async def get_endpoints() -> dict[str, List[str]]:
  """
  获取端点列表
  """
  from src.modules.storage import service as storage_service
  minio_server_objs = await storage_service.get_minio_server_list()
  data = []
  for minio_server_obj in minio_server_objs["data"]:
    data.append({
      "region_id": minio_server_obj.region.id,
      "server_id": minio_server_obj.id,
      "name": minio_server_obj.region.name,
      "shown_name": minio_server_obj.region.shown_name,
      "master": minio_server_obj.master,
      "endpoint": f"{settings.PUBLIC_SCHEME}://{minio_server_obj.host}:{minio_server_obj.server_port}",
      "minio_endpoint": f"{settings.PUBLIC_SCHEME}://{minio_server_obj.host}:{minio_server_obj.minio_port}",
    })
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

async def create_application(
  name: str,
  shown_name: str,
  description: str,
  current_user: User) -> Application:
  """
  创建应用
  """
  # 因为应用名用于存储桶创建, 所以需要一定的约束条件
  await _validate_application_name(name)
  # region_objs = await public_crud.read_many_region_by_ids(regions)
  # if len(region_objs) != len(regions):
  #   raise CustomException(ErrorDesc.RES_NOT_FOUND, "Region.id")
  existed_name = await public_crud.read_application_by_name(name)
  if existed_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Application.name")
  existed_shown_name = await public_crud.read_application_by_shown_name(shown_name)
  if existed_shown_name:
    raise CustomException(ErrorDesc.NAME_EXISTED, "Application.shown_name")
  # 创建所有桶, 并且开启版本控制
  app = await public_crud.create_application(name, shown_name, description, current_user)
  try:
    await sync_module.publish_application(app)
  except Exception as e:
    await app.delete()
    from src.core import audit, metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
    audit.audit(
      "application.create",
      actor=current_user.username,
      resource=name,
      detail=str(e),
      success=False,
    )
    raise CustomException(ErrorDesc.SYNC_FAILED, f"Application 同步到 Etcd 失败: {e}")
  from src.core import audit
  audit.audit("application.create", actor=current_user.username, resource=name)
  return app

async def get_application_list() -> dict[str, List[Application]]:
  """
  获取应用列表
  """
  application_objs = await public_crud.read_application_list()
  return dict[str, List[Application]](data=application_objs)

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
  if application_obj.author != current_user:
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
    data.append({
      "id": api_key_obj.id,
      "key": hint,
      "application": {
        "id": api_key_obj.application.id,
        "name": api_key_obj.application.name,
        "shown_name": api_key_obj.application.shown_name
      },
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
  application_obj = await public_crud.read_application_by_id(api_key_obj.application.id)
  is_owner = bool(application_obj and application_obj.author.id == current_user.id)
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
