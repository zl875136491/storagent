import asyncio
import re
from typing import List
from datetime import datetime, timedelta, timezone

from bson import ObjectId

from src.core.exception import CustomException, ErrorDesc
from src.modules.public import crud as public_crud
from src.modules.storage import crud as storage_crud
from src.modules.storage import schema as storage_schema
from src.modules.storage.model import MinioServer
from src.modules.graph import crud as graph_crud
from src.modules.graph import service as graph_service
from src.core.minio_op import (
  test_minio_server,
  set_site_alias,
  get_buckets_info,
  check_server_bucket_existed,
  get_minio_client,
  get_server_buckets,
  get_bucket_replicate_status_result,
  get_bucket_replicate_entries_result,
  get_site_alias,
  create_bucket_replicate as create_minio_bucket_replicate,
  delete_bucket_replicate as delete_minio_bucket_replicate,
)
from src.core import sync as sync_module
from src.configs.configs import settings
from src.utils.helpers import utc_now


_server_details_locks: dict[str, asyncio.Lock] = {}


def _server_details_lock(server_id: str) -> asyncio.Lock:
  lock = _server_details_locks.get(server_id)
  if lock is None:
    lock = asyncio.Lock()
    _server_details_locks[server_id] = lock
  return lock


def _aware_utc(value: datetime) -> datetime:
  return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

async def _connect_minio_server(
  host: str,
  port: int,
  access_key: str,
  secret_key: str):
  """
  连接 Minio 服务器

  Args:
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥
  """
  test_minio_server(host, port, access_key, secret_key)
  return None

async def create_minio_server(
  region: str | ObjectId,
  name: str,
  domain: str,
  host: str,
  server_port: int,
  minio_port: int,
  access_key: str,
  secret_key: str,
  replicate_weight: int = 0) -> MinioServer:
  """
  创建 Minio 服务器

  Args:
    region: 区域ID
    name: 服务器名称
    domain: 对外 Nginx 网关域名
    host: MinIO 内网连接主机
    server_port: 服务器端口
    minio_port: Minio 端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    MinioServer: Minio 服务器
  """
  # 验证区域
  domain = domain.strip().lower().replace("http://", "").replace("https://", "").strip("/")
  if not domain or "/" in domain:
    raise CustomException(ErrorDesc.INVALID_PARAMS, "domain 必须是无协议、无路径的网关域名")
  region_obj = await public_crud.read_region_by_id(region)
  if not region_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND)
  exister_minio_region = await storage_crud.read_minio_server_by_region(region_obj)
  if exister_minio_region:
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "MinioServer.region")
  # 验证是否存在
  existed_minio_server = await storage_crud.read_minio_server_by_fqdn(host, minio_port)
  if existed_minio_server:
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "MinioServer.host:port")
  # 验证连接性
  await _connect_minio_server(host, minio_port, access_key, secret_key)
  # 创建别名
  success, res = await set_site_alias(
    site_name=region_obj.name,
    endpoint=f"{host}:{minio_port}",
    admin_user=access_key,
    admin_password=secret_key
  )
  if not success:
    raise CustomException(ErrorDesc.MINIO_ALIAS_FAILED, res)
  # 创建 Minio 服务器数据
  minio_server_obj = await storage_crud.create_minio_server(
    region=region_obj,
    name=name,
    domain=domain,
    host=host,
    server_port=server_port,
    minio_port=minio_port,
    access_key=access_key,
    secret_key=secret_key,
    replicate_weight=replicate_weight
  )
  try:
    await sync_module.publish_server_entry(
      region_name=region_obj.name,
      domain=domain,
      host=host,
      server_port=server_port,
      minio_port=minio_port,
      access_key=access_key,
      secret_key=secret_key,
      replicate_weight=replicate_weight,
    )
  except Exception as e:
    await minio_server_obj.delete()
    from src.core import audit, metrics as metrics_mod
    from src.core.exception import CustomException, ErrorDesc
    metrics_mod.incr("sync_failures_total")
    audit.audit("minio_server.create", resource=region_obj.name, detail=str(e), success=False)
    raise CustomException(ErrorDesc.SYNC_FAILED, f"Server 同步到 Etcd 失败: {e}")
  from src.core import audit
  audit.audit("minio_server.create", resource=region_obj.name)
  return minio_server_obj

async def update_minio_server(
  minio_server_id: ObjectId,
  payload) -> MinioServer:
  """
  更新 Minio 服务器
  """
  minio_server_obj = await storage_crud.read_minio_server_by_id(minio_server_id)
  if not minio_server_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  result = await storage_crud.update_minio_server(
    minio_server_obj,
    domain=minio_server_obj.domain,
    host=minio_server_obj.host,
    server_port=minio_server_obj.server_port,
    minio_port=minio_server_obj.minio_port,
    access_key=minio_server_obj.access_key,
    secret_key=minio_server_obj.secret_key,
    replicate_weight=payload.replicate_weight
  )
  try:
    region_obj = minio_server_obj.region
    region_name = region_obj.name if hasattr(region_obj, "name") else minio_server_obj.name
    access_key, secret_key = storage_crud.plain_minio_credentials(result)
    await sync_module.publish_server_entry(
      region_name=region_name,
      domain=result.domain,
      host=result.host,
      server_port=result.server_port,
      minio_port=result.minio_port,
      access_key=access_key,
      secret_key=secret_key,
      replicate_weight=result.replicate_weight,
    )
  except Exception as e:
    from src.core import audit, metrics as metrics_mod
    from src.core.exception import CustomException, ErrorDesc
    metrics_mod.incr("sync_failures_total")
    audit.audit("minio_server.update", resource=str(minio_server_id), detail=str(e), success=False)
    raise CustomException(ErrorDesc.SYNC_FAILED, f"Server 更新同步到 Etcd 失败: {e}")
  from src.core import audit
  audit.audit("minio_server.update", resource=str(minio_server_id))
  return result

async def get_minio_server_list() -> dict[str, List[MinioServer]]:
  """
  获取 Minio 服务器列表

  Returns:
    List[MinioServer]: Minio 服务器列表
  """
  minio_server_objs = await storage_crud.read_minio_server_list()
  return dict[str, List[MinioServer]](data=minio_server_objs)

async def get_buckets() -> List[str]:
  """
  获取存储桶列表
  """
  bucket_data = {}
  server_names = await storage_crud.read_minio_server_names()
  app_infos = {}
  app_objs = await public_crud.read_application_list()
  for app_obj in app_objs:
    app_infos[app_obj.name] = {
      "shown_name": app_obj.shown_name,
      "description": app_obj.description
    }
  for server_name in server_names:
    buckets = await get_server_buckets(server_name)
    for bucket_name in buckets:
      if bucket_name not in bucket_data:
        if bucket_name in app_infos:
          app_info = app_infos[bucket_name]
        else:
          app_info = {}
        bucket_data[bucket_name] = {
          "name": bucket_name,
          "servers": [server_name],
          "app": app_info
        }
      else:
        bucket_data[bucket_name]["servers"].append(server_name)
  return dict[str, List](data=list(bucket_data.values()))

async def get_server_details(
  minio_server: ObjectId,
  force_refresh: bool = False,
) -> dict:
  """Return a recursive inventory cached in MongoDB for ten minutes."""
  minio_server_obj = await storage_crud.read_minio_server_by_id(minio_server)
  if not minio_server_obj:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "MinioServer")
  server_id = str(minio_server_obj.id)
  now = utc_now()
  await storage_crud.delete_expired_server_file_details(now)
  if force_refresh:
    await storage_crud.delete_server_file_details_cache(server_id)

  cache = await storage_crud.read_server_file_details_cache(server_id)
  if cache and _aware_utc(cache.expires_at) > now:
    return {
      "data": cache.data,
      "cache_hit": True,
      "cached_at": cache.fetched_at,
      "expires_at": cache.expires_at,
      "ttl_seconds": settings.SERVER_DETAILS_CACHE_TTL_SECONDS,
    }

  async with _server_details_lock(server_id):
    # Recheck after acquiring the lock so concurrent cache misses use one fetch.
    now = utc_now()
    await storage_crud.delete_expired_server_file_details(now)
    if not force_refresh:
      cache = await storage_crud.read_server_file_details_cache(server_id)
      if cache and _aware_utc(cache.expires_at) > now:
        return {
          "data": cache.data,
          "cache_hit": True,
          "cached_at": cache.fetched_at,
          "expires_at": cache.expires_at,
          "ttl_seconds": settings.SERVER_DETAILS_CACHE_TTL_SECONDS,
        }

    access_key, secret_key = storage_crud.plain_minio_credentials(minio_server_obj)
    minio_client = get_minio_client(
      host=minio_server_obj.host,
      port=minio_server_obj.minio_port,
      access_key=access_key,
      secret_key=secret_key
    )
    buckets = await get_buckets_info(minio_client)
    fetched_at = utc_now()
    ttl_seconds = max(int(settings.SERVER_DETAILS_CACHE_TTL_SECONDS), 1)
    expires_at = fetched_at + timedelta(seconds=ttl_seconds)
    cache = await storage_crud.write_server_file_details_cache(
      server_id,
      buckets,
      fetched_at,
      expires_at,
    )
    return {
      "data": cache.data,
      "cache_hit": False,
      "cached_at": cache.fetched_at,
      "expires_at": cache.expires_at,
      "ttl_seconds": ttl_seconds,
    }

async def format_replicate_status(status: dict) -> dict:
  """
  格式化复制状态
  """
  data = {}
  if "status" in status:
    data["status"] = status["status"]
  if "rule" in status:
    if "Priority" in status["rule"]:
      data["priority"] = status["rule"]["Priority"]
    if "Status" in status["rule"]:
      data["rule_status"] = status["rule"]["Status"]
    if "DeleteMarkerReplication" in status["rule"]:
      data["delete_marker_replication"] = status["rule"]["DeleteMarkerReplication"]["Status"]
    if "ExistingObjectReplication" in status["rule"]:
      data["existing_object_replication"] = status["rule"]["ExistingObjectReplication"]["Status"]
    if "SourceSelectionCriteria" in status["rule"]:
      if "ReplicaModifications" in status["rule"]["SourceSelectionCriteria"]:
        data["source_selection_criteria"] = status["rule"]["SourceSelectionCriteria"]["ReplicaModifications"]["Status"]
  return data


def _replication_setting_enabled(value: object) -> bool:
  return str(value or "").strip().lower() == "enabled"


def is_replication_rule_healthy(rule: dict) -> bool:
  status = rule.get("status") or {}
  return (
    str(status.get("status") or "").lower() == "success"
    and _replication_setting_enabled(status.get("rule_status"))
    and _replication_setting_enabled(status.get("delete_marker_replication"))
    and _replication_setting_enabled(status.get("existing_object_replication"))
    and _replication_setting_enabled(status.get("source_selection_criteria"))
  )


def build_full_mesh_policy(
  server_names: list[str],
  replicates: list[dict],
  *,
  read_errors: dict[str, str] | None = None,
  unmapped_rule_count: int = 0,
) -> dict:
  sites = sorted(set(server_names))
  expected_pairs = {
    (from_server, to_server)
    for from_server in sites
    for to_server in sites
    if from_server != to_server
  }
  rules_by_pair: dict[tuple[str, str], list[dict]] = {}
  for rule in replicates:
    pair = (str(rule.get("from") or ""), str(rule.get("to") or ""))
    rules_by_pair.setdefault(pair, []).append(rule)

  missing = sorted(expected_pairs - set(rules_by_pair))
  duplicates = sorted(
    pair for pair, rules in rules_by_pair.items()
    if pair in expected_pairs and len(rules) > 1
  )
  unexpected = sorted(set(rules_by_pair) - expected_pairs)
  unhealthy = sorted(
    pair for pair in expected_pairs
    if len(rules_by_pair.get(pair, [])) == 1
    and not is_replication_rule_healthy(rules_by_pair[pair][0])
  )
  read_errors = read_errors or {}
  actual_count = len(replicates) + unmapped_rule_count
  healthy_count = sum(is_replication_rule_healthy(rule) for rule in replicates)
  expected_count = len(expected_pairs)
  complete = (
    len(sites) >= 2
    and actual_count == expected_count
    and not missing
    and not duplicates
    and not unexpected
    and not unhealthy
    and not read_errors
    and unmapped_rule_count == 0
  )
  return {
    "type": "full_mesh",
    "site_count": len(sites),
    "expected_rule_count": expected_count,
    "actual_rule_count": actual_count,
    "healthy_rule_count": healthy_count,
    "complete": complete,
    "status": "ready" if complete else "degraded",
    "missing_rules": [
      {"from": from_server, "to": to_server}
      for from_server, to_server in missing
    ],
    "duplicate_rules": [
      {"from": from_server, "to": to_server, "count": len(rules_by_pair[(from_server, to_server)])}
      for from_server, to_server in duplicates
    ],
    "unhealthy_rules": [
      {"from": from_server, "to": to_server}
      for from_server, to_server in unhealthy
    ],
    "unexpected_rules": [
      {"from": from_server, "to": to_server}
      for from_server, to_server in unexpected
    ],
    "unmapped_rule_count": unmapped_rule_count,
    "read_errors": read_errors,
  }

async def get_bucket_replicate_infos(bucket_name) -> dict:
  """
  获取存储桶复制信息
  """
  replicates = []
  alias_datas = await get_site_alias()
  # 获取服务器别名与服务器地址的映射关系
  # 因为 mc replicate ls 命令返回的是服务器地址, 而不是服务器别名
  mappings = {}
  for alias_name, alias_data in alias_datas.items():
    raw_url = str(alias_data.get("URL") or "")
    endpoint = raw_url.split("://", 1)[-1].rsplit("@", 1)[-1].rstrip("/")
    if endpoint:
      mappings[endpoint] = alias_name
  server_names = await storage_crud.read_minio_server_names()
  # 获取拓扑图的边和节点的位置信息
  nodes = {}
  edges = {}
  read_errors: dict[str, str] = {}
  unmapped_rule_count = 0
  node_objs = await graph_crud.read_many_bucket_node_positions(bucket_name)
  edge_objs = await graph_crud.read_many_bucket_edge_positions(bucket_name)
  for node_item in node_objs:
    # 仅返回 Mongo 中真实落盘的坐标；缺失节点由前端做环形默认布局。
    # 切勿用 (0,0) 填充未布局节点：会把未布局站点叠在原点，并干扰像素/百分比坐标推断。
    nodes[node_item.server] = {
      "position_x": node_item.position_x,
      "position_y": node_item.position_y
    }
  for edge_item in edge_objs:
    from_server = edge_item.from_server
    to_server = edge_item.to_server
    from_position = edge_item.from_position
    to_position = edge_item.to_position
    temp_id = f"{from_server}-{to_server}"
    edges[temp_id] = {
      "from_position": from_position,
      "to_position": to_position
    } 
  for server_name in server_names:
    entries_ok, to_server_entries, entries_error = await get_bucket_replicate_entries_result(
      server_name,
      bucket_name,
    )
    status_ok, status_infos, status_error = await get_bucket_replicate_status_result(
      server_name,
      bucket_name,
    )
    if not entries_ok or not status_ok:
      read_errors[server_name] = entries_error or status_error or "读取复制规则失败"
      continue
    for entry in to_server_entries:
      endpoint = entry["endpoint"]
      to_server_name = mappings.get(endpoint)
      if not to_server_name:
        unmapped_rule_count += 1
        continue
      rule_id = entry["rule_id"]
      status_info = await format_replicate_status(status_infos.get(rule_id, {}))
      temp_id = f"{server_name}-{to_server_name}"
      if temp_id in edges:
        from_position = edges[temp_id]["from_position"]
        to_position = edges[temp_id]["to_position"]
      else:
        from_position = "up"
        to_position = "down"
      replicates.append({
        "from": server_name,
        "from_position": from_position,
        "to": to_server_name,
        "to_position": to_position,
        "status": status_info,
        "rule_id": rule_id
      })
  # 遗留百分比坐标（全部落在 0–100）迁移为像素，避免前后端启发式互相误判。
  if _looks_like_percent_positions(nodes):
    migrated = _percent_nodes_to_pixels(nodes)
    for server, pos in migrated.items():
      try:
        await graph_service.set_bucket_node_position(
          bucket_name,
          server,
          pos["position_x"],
          pos["position_y"],
        )
      except Exception:
        pass
    nodes = migrated
  policy = build_full_mesh_policy(
    server_names,
    replicates,
    read_errors=read_errors,
    unmapped_rule_count=unmapped_rule_count,
  )
  return dict(
    servers=nodes,
    replicates=replicates,
    server_ids=server_names,
    policy=policy,
  )


_BUCKET_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")
_SIDE_TO_POSITION = {
  "top": "up",
  "right": "right",
  "bottom": "down",
  "left": "left",
}
# 与前端画布基准一致：遗留百分比坐标迁移为像素
_GRAPH_AREA_W = 900
_GRAPH_AREA_H = 560


def _looks_like_percent_positions(nodes: dict) -> bool:
  vals = list(nodes.values())
  if not vals:
    return False
  return all(
    0 <= int(v.get("position_x", -1)) <= 100 and 0 <= int(v.get("position_y", -1)) <= 100
    for v in vals
  )


def _percent_nodes_to_pixels(nodes: dict) -> dict:
  converted = {}
  for server, pos in nodes.items():
    converted[server] = {
      "position_x": round(int(pos["position_x"]) / 100 * _GRAPH_AREA_W),
      "position_y": round(int(pos["position_y"]) / 100 * _GRAPH_AREA_H),
    }
  return converted


def _validate_bucket_name(bucket_name: str) -> str:
  name = bucket_name.strip()
  if (
    not _BUCKET_NAME_RE.fullmatch(name)
    or ".." in name
    or ".-" in name
    or "-." in name
  ):
    raise CustomException(ErrorDesc.INVALID_PARAMS, "存储桶名称不合法")
  return name


async def create_bucket_replicate(bucket_name: str, payload) -> dict:
  """创建单向复制规则，并保存拓扑连线端口。"""
  from src.core import audit

  bucket = _validate_bucket_name(bucket_name)
  from_server = payload.from_server.strip()
  to_server = payload.to_server.strip()
  server_names = set(await storage_crud.read_minio_server_names())
  if from_server == to_server:
    raise CustomException(ErrorDesc.INVALID_RULE_PARAMS, "源站点与目标站点不能相同")
  if from_server not in server_names or to_server not in server_names:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "源站点或目标站点不存在")

  source_exists, target_exists = await asyncio.gather(
    check_server_bucket_existed(from_server, bucket),
    check_server_bucket_existed(to_server, bucket),
  )
  if not source_exists or not target_exists:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "源站点或目标站点不存在该存储桶")

  current = await get_bucket_replicate_infos(bucket)
  if any(
    item.get("from") == from_server and item.get("to") == to_server
    for item in current.get("replicates", [])
  ):
    raise CustomException(ErrorDesc.RES_ALREADY_EXISTS, "复制连接已存在")

  requested_status = payload.status or storage_schema.BucketReplicateRuleStatus()
  replicate_options = ["delete"]
  if requested_status.delete_marker_replication.lower() == "enabled":
    replicate_options.append("delete-marker")
  if requested_status.existing_object_replication.lower() == "enabled":
    replicate_options.append("existing-objects")
  if requested_status.source_selection_criteria.lower() == "enabled":
    replicate_options.append("metadata-sync")

  success, detail = await create_minio_bucket_replicate(
    from_server,
    to_server,
    bucket,
    priority=requested_status.priority,
    enabled=requested_status.status.lower() != "disabled",
    replicate_options=replicate_options,
  )
  if not success:
    audit.audit(
      "bucket_replicate.create",
      resource=f"{bucket}:{from_server}->{to_server}",
      detail=detail,
      success=False,
    )
    raise CustomException(ErrorDesc.MINIO_REPLICATE_FAILED, detail)

  from_position = _SIDE_TO_POSITION[payload.from_side]
  to_position = _SIDE_TO_POSITION[payload.to_side]
  try:
    await graph_service.set_bucket_edge_position(
      bucket,
      from_server,
      to_server,
      from_position,
      to_position,
    )
  except Exception as e:
    # 复制规则已经在 MinIO 生效；拓扑位置可由后续编辑补写。
    audit.audit(
      "bucket_replicate.position",
      resource=f"{bucket}:{from_server}->{to_server}",
      detail=str(e),
      success=False,
    )

  refreshed = await get_bucket_replicate_infos(bucket)
  created = next((
    item for item in refreshed.get("replicates", [])
    if item.get("from") == from_server and item.get("to") == to_server
  ), None)
  if not created:
    created = {
      "from": from_server,
      "to": to_server,
      "from_position": from_position,
      "to_position": to_position,
      "status": {
        "status": "pending",
        "priority": requested_status.priority,
        "delete_marker_replication": requested_status.delete_marker_replication,
        "existing_object_replication": requested_status.existing_object_replication,
        "source_selection_criteria": requested_status.source_selection_criteria,
      },
      "rule_id": "",
    }
    audit.audit(
      "bucket_replicate.readback",
      resource=f"{bucket}:{from_server}->{to_server}",
      detail="复制规则已创建，状态读取尚未就绪",
      success=False,
    )

  audit.audit(
    "bucket_replicate.create",
    resource=f"{bucket}:{from_server}->{to_server}",
  )
  return created


async def delete_bucket_replicate(
  bucket_name: str,
  from_server: str,
  to_server: str,
  rule_id: str | None = None,
) -> dict:
  """删除单向复制规则，并清理拓扑边位置。"""
  from src.core import audit

  bucket = _validate_bucket_name(bucket_name)
  from_server = from_server.strip()
  to_server = to_server.strip()
  if from_server == to_server:
    raise CustomException(ErrorDesc.INVALID_RULE_PARAMS, "源站点与目标站点不能相同")

  server_names = set(await storage_crud.read_minio_server_names())
  if from_server not in server_names or to_server not in server_names:
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "源站点或目标站点不存在")

  resolved_rule_id = (rule_id or "").strip()
  current = await get_bucket_replicate_infos(bucket)
  match = next((
    item for item in current.get("replicates", [])
    if item.get("from") == from_server and item.get("to") == to_server
  ), None)
  if not match or not match.get("rule_id"):
    raise CustomException(ErrorDesc.RES_NOT_FOUND, "复制连接不存在")
  matched_rule_id = str(match["rule_id"])
  if resolved_rule_id and resolved_rule_id != matched_rule_id:
    raise CustomException(
      ErrorDesc.INVALID_RULE_PARAMS,
      "rule_id 与 from/to 对应的复制规则不一致",
    )
  resolved_rule_id = matched_rule_id

  success, detail = await delete_minio_bucket_replicate(
    from_server,
    bucket,
    resolved_rule_id,
  )
  if not success:
    audit.audit(
      "bucket_replicate.delete",
      resource=f"{bucket}:{from_server}->{to_server}",
      detail=detail,
      success=False,
    )
    raise CustomException(ErrorDesc.MINIO_REPLICATE_FAILED, detail)

  try:
    await graph_service.delete_bucket_edge_position(bucket, from_server, to_server)
  except Exception as e:
    audit.audit(
      "bucket_replicate.edge_position_delete",
      resource=f"{bucket}:{from_server}->{to_server}",
      detail=str(e),
      success=False,
    )

  audit.audit(
    "bucket_replicate.delete",
    resource=f"{bucket}:{from_server}->{to_server}",
    detail=resolved_rule_id,
  )
  return {
    "message": "ok",
    "from": from_server,
    "to": to_server,
    "rule_id": resolved_rule_id,
  }
