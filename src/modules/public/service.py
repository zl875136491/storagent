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
  启用应用（SSE 流式进度：按 MinIO 服务器逐步处理桶，最终成功或失败并结束连接）
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

  application_obj = await public_crud.read_application_by_id(application_id)
  if not application_obj:
    msg = "应用不存在，无法授权"
    async for chunk in _emit({
      "step": "validate",
      "server_name": None,
      "status": "failed",
      "message": msg,
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
    msg = "应用已启用，无需重复授权"
    async for chunk in _emit({
      "step": "validate",
      "server_name": None,
      "status": "failed",
      "message": msg,
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
    "message": "开始授权流程",
  }):
    yield chunk

  application_obj.enabled = True
  application_obj.enabled_at = utc_now()
  application_obj.approver = current_user

  server_names = await storage_crud.read_minio_server_names()
  async for chunk in _emit({
    "step": "bucket_phase",
    "server_name": None,
    "status": "running",
    "message": f"共 {len(server_names)} 台 MinIO 服务器待处理桶: {application_obj.name}",
  }):
    yield chunk

  # 逐个创建桶
  errors: dict[str, str] = {}
  for server_name in server_names:
    async for chunk in _emit({
      "step": "bucket_check",
      "server_name": server_name,
      "status": "running",
      "message": f"检查服务器 {server_name} 上桶是否存在",
    }):
      yield chunk
    existed = await minio_op.check_server_bucket_existed(server_name, application_obj.name)
    if existed:
      async for chunk in _emit({
        "step": "bucket_check",
        "server_name": server_name,
        "status": "skipped",
        "message": f"服务器 {server_name} 上桶已存在，跳过创建",
      }):
        yield chunk
      continue
    async for chunk in _emit({
      "step": "bucket_create",
      "server_name": server_name,
      "status": "running",
      "message": f"在服务器 {server_name} 上创建桶",
    }):
      yield chunk
    success, err = await minio_op.create_bucket(server_name, application_obj.name)
    if not success:
      errors[server_name] = err
      async for chunk in _emit({
        "step": "bucket_create",
        "server_name": server_name,
        "status": "failed",
        "message": f"服务器 {server_name} 创建桶失败: {err}",
      }):
        yield chunk
    else:
      async for chunk in _emit({
        "step": "bucket_create",
        "server_name": server_name,
        "status": "ok",
        "message": f"服务器 {server_name} 创建桶成功",
      }):
        yield chunk

  # 如果部分服务器创建桶失败，则终止授权
  if errors:
    async for chunk in _emit({
      "step": "bucket_create",
      "server_name": None,
      "status": "failed",
      "message": "部分服务器创建桶失败，终止授权",
      "detail": errors,
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

  # 逐个开启桶版本控制
  for server_name in server_names:
    async for chunk in _emit({
      "step": "bucket_versioning",
      "server_name": server_name,
      "status": "running",
      "message": f"在服务器 {server_name} 上开启桶版本控制",
    }):
      yield chunk
    success, err = await minio_op.enable_bucket_versioning(server_name, application_obj.name)
    if not success:
      async for chunk in _emit({
        "step": "bucket_versioning",
        "server_name": server_name,
        "status": "failed",
        "message": f"服务器 {server_name} 开启版本控制失败: {err}",
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
      "step": "bucket_versioning",
      "server_name": server_name,
      "status": "ok",
      "message": f"服务器 {server_name} 开启版本控制成功",
    }):
      yield chunk
      
  # 更改应用启用状态
  async for chunk in _emit({
    "step": "persist",
    "server_name": None,
    "status": "running",
    "message": "保存应用启用状态",
  }):
    yield chunk
  await application_obj.save()
  async for chunk in _emit({
    "step": "persist",
    "server_name": None,
    "status": "ok",
    "message": "应用状态已保存",
  }):
    yield chunk

  # 跨节点同步：发布 Application 到 Etcd
  async for chunk in _emit({
    "step": "sync",
    "server_name": None,
    "status": "running",
    "message": "同步应用信息到其他节点",
  }):
    yield chunk
  try:
    await sync_module.publish_application(application_obj)
    sync_msg = "应用信息已同步到其他节点"
    sync_status = "ok"
  except Exception as e:
    sync_msg = f"应用信息同步失败: {e}"
    sync_status = "failed"
    logger.warning(sync_msg)
    from src.core import metrics as metrics_mod
    metrics_mod.incr("sync_failures_total")
  async for chunk in _emit({
    "step": "sync",
    "server_name": None,
    "status": sync_status,
    "message": sync_msg,
  }):
    yield chunk

  # 配置 bucket 级复制规则
  async for chunk in _emit({
    "step": "replicate",
    "server_name": None,
    "status": "running",
    "message": "配置存储桶跨节点复制规则",
  }):
    yield chunk
  try:
    await sync_module.setup_bucket_replication(application_obj.name, server_names)
    rep_msg = "存储桶复制规则配置完成"
    rep_status = "ok"
  except Exception as e:
    rep_msg = f"存储桶复制规则配置失败: {e}"
    rep_status = "failed"
    logger.warning(rep_msg)
  async for chunk in _emit({
    "step": "replicate",
    "server_name": None,
    "status": rep_status,
    "message": rep_msg,
  }):
    yield chunk

  # 持久化桶记录
  try:
    await storage_crud.bulk_create_minio_bucket(application_obj)
  except Exception as e:
    logger.warning(f"MinioBucket 记录写入失败: {e}")

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
  if not application_obj.enabled:
    raise CustomException(ErrorDesc.STATUS_ERR, "应用未启用")
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
