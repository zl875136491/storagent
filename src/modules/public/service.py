import json
import logging
from collections.abc import AsyncGenerator
from typing import List

from bson import ObjectId
from datetime import datetime, timedelta

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
# from src.core.redis_op import RedisOp

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
      "endpoint": f"http://{minio_server_obj.host}:{minio_server_obj.server_port}"
    })
  return dict[str, List[dict]](data=data)

async def test_endpoints() -> bytes:
  """
  测试端点
  """
  # 返回一个 512 Byte 的文件流
  file_content = b"Storagent" * 56
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
  return await public_crud.create_region(name, shown_name)

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
  return await public_crud.create_application(name, shown_name, description, current_user)

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
  # 将 API Key 数据同步到 Agent 中
  # async with RedisOp() as redis_op:
  #   await redis_op.publish_api_key_create_patch(
  #     api_key=api_key_obj.key,
  #     app_name=application_obj.name,
  #     expired_at=expired_at,
  #   )
  return api_key_obj

async def get_api_key_list(
  current_user: User) -> List[APIKey]:
  """
  获取API密钥列表
  """
  users_app_objs = await public_crud.read_users_enabled_application_list(current_user)
  api_key_objs = await public_crud.read_api_key_by_app(users_app_objs)
  data = []
  for api_key_obj in api_key_objs:
    data.append({
      "id": api_key_obj.id,
      "key": f"{api_key_obj.key[:7]}************{api_key_obj.key[-4:]}",
      "application": {
        "id": api_key_obj.application.id,
        "name": api_key_obj.application.name,
        "shown_name": api_key_obj.application.shown_name
      },
      "expired_at": api_key_obj.expired_at
    })
  return dict[str, List[APIKey]](data=data)