import json
import asyncio
import re
import shlex
import subprocess
from time import monotonic, perf_counter
from minio import Minio
from typing import Any, List

from src.core.exception import CustomException, ErrorDesc
from src.utils.helpers import build_file_tree
from src.modules.public.crud import create_shell_command_log


_MC_ALIAS_REFRESH_INTERVAL_SECONDS = 30.0
_MC_ALIAS_RETRY_INTERVAL_SECONDS = 5.0
_mc_alias_refresh_deadline = 0.0


async def _refresh_mc_aliases_if_due(*, force: bool = False) -> bool:
  """Keep per-process ``mc`` aliases aligned with the shared server registry."""
  global _mc_alias_refresh_deadline

  now = monotonic()
  if not force and now < _mc_alias_refresh_deadline:
    return True

  try:
    from src.core import sync as sync_module

    await sync_module.ensure_mc_aliases_from_etcd()
  except Exception:
    # Keep a usable cached alias available when Etcd is temporarily unavailable.
    _mc_alias_refresh_deadline = monotonic() + _MC_ALIAS_RETRY_INTERVAL_SECONDS
    return False

  _mc_alias_refresh_deadline = monotonic() + _MC_ALIAS_REFRESH_INTERVAL_SECONDS
  return True


async def _run_shell_command(cmd: str) -> tuple[bool, str, str]:
  try:
    result = await asyncio.to_thread(
      subprocess.run,
      cmd,
      shell=True,
      check=False,
      capture_output=True,
      text=True,
    )
  except Exception as error:
    return False, "", str(error)
  return result.returncode == 0, result.stdout, result.stderr


def _looks_like_missing_mc_alias(output: str) -> bool:
  lowered = output.lower()
  return (
    "/app/" in lowered
    or "alias does not exist" in lowered
    or "unable to initialize new alias" in lowered
  )


async def _run_cmd(cmd):
  """
  执行 shell 命令并返回结果
  
  Args:
    cmd: 命令

  Returns:
    Tuple[bool, str]: 执行结果
    True: 执行成功
    False: 执行失败
    str: 执行结果
  """
  stripped = cmd.lstrip()
  is_mc_command = stripped.startswith("mc ")
  is_alias_command = stripped.startswith("mc alias ")
  if is_mc_command and not is_alias_command:
    await _refresh_mc_aliases_if_due()

  success, stdout, stderr = await _run_shell_command(cmd)
  output = f"{stderr}\n{stdout}"
  if (
    not success
    and is_mc_command
    and not is_alias_command
    and _looks_like_missing_mc_alias(output)
  ):
    await _refresh_mc_aliases_if_due(force=True)
    success, stdout, stderr = await _run_shell_command(cmd)

  await create_shell_command_log(cmd, stdout, stderr)
  if success:
    return True, stdout
  return False, stderr or stdout or "MinIO 命令执行失败"


def _mc_error_message(items: list[dict[str, Any]], fallback: str) -> str:
  for item in reversed(items):
    if item.get("status") != "error":
      continue
    error = item.get("error")
    if isinstance(error, dict):
      message = error.get("message")
      cause = error.get("cause")
      if isinstance(cause, dict) and cause.get("message"):
        return f"{message}: {cause['message']}" if message else str(cause["message"])
      if message:
        return str(message)
    if error:
      return str(error)
  return fallback.strip() or "MinIO 命令执行失败"


async def run_mc_json(
  args: list[str],
  *,
  timeout: float = 20.0,
  record: bool = True,
  allow_empty: bool = False,
) -> tuple[bool, list[dict[str, Any]], str, float]:
  """Run an mc command without a shell and parse its JSON-lines output."""
  cmd = ["mc", *args]
  if "--json" not in cmd:
    cmd.append("--json")
  command_text = shlex.join(cmd)
  started = perf_counter()
  process = None
  try:
    process = await asyncio.create_subprocess_exec(
      *cmd,
      stdout=asyncio.subprocess.PIPE,
      stderr=asyncio.subprocess.PIPE,
    )
    try:
      stdout_bytes, stderr_bytes = await asyncio.wait_for(
        process.communicate(),
        timeout=max(float(timeout), 1.0),
      )
    except TimeoutError:
      process.kill()
      stdout_bytes, stderr_bytes = await process.communicate()
      stdout = stdout_bytes.decode("utf-8", errors="replace")
      stderr = stderr_bytes.decode("utf-8", errors="replace")
      elapsed_ms = (perf_counter() - started) * 1000
      await create_shell_command_log(command_text, stdout, stderr or "command timed out")
      return False, [], f"MinIO 命令超时（{timeout:g} 秒）", elapsed_ms
  except asyncio.CancelledError:
    if process is not None and process.returncode is None:
      process.kill()
      await process.communicate()
    raise
  except Exception as e:
    elapsed_ms = (perf_counter() - started) * 1000
    await create_shell_command_log(command_text, "", str(e))
    return False, [], str(e), elapsed_ms

  stdout = stdout_bytes.decode("utf-8", errors="replace")
  stderr = stderr_bytes.decode("utf-8", errors="replace")
  elapsed_ms = (perf_counter() - started) * 1000
  items: list[dict[str, Any]] = []
  for line in stdout.splitlines():
    line = line.strip()
    if not line:
      continue
    try:
      item = json.loads(line)
    except json.JSONDecodeError:
      continue
    if isinstance(item, dict):
      items.append(item)
  has_error = any(item.get("status") == "error" for item in items)
  if record or process.returncode != 0 or has_error:
    await create_shell_command_log(command_text, stdout, stderr)
  if process.returncode != 0 or has_error:
    return False, items, _mc_error_message(items, stderr or stdout), elapsed_ms
  if not items:
    if allow_empty:
      return True, [], "", elapsed_ms
    return False, [], "MinIO 命令未返回 JSON 数据", elapsed_ms
  return True, items, "", elapsed_ms


def _parse_mc_size_bytes(value: Any) -> int | None:
  if isinstance(value, bool):
    return None
  if isinstance(value, (int, float)):
    return max(int(value), 0)
  if not isinstance(value, str):
    return None
  normalized = value.strip().replace(" ", "")
  match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)([A-Za-z]*)", normalized)
  if not match:
    return None
  amount = float(match.group(1))
  unit = match.group(2).lower()
  factors = {
    "": 1,
    "b": 1,
    "k": 1000,
    "kb": 1000,
    "ki": 1024,
    "kib": 1024,
    "m": 1000 ** 2,
    "mb": 1000 ** 2,
    "mi": 1024 ** 2,
    "mib": 1024 ** 2,
    "g": 1000 ** 3,
    "gb": 1000 ** 3,
    "gi": 1024 ** 3,
    "gib": 1024 ** 3,
    "t": 1000 ** 4,
    "tb": 1000 ** 4,
    "ti": 1024 ** 4,
    "tib": 1024 ** 4,
    "p": 1000 ** 5,
    "pb": 1000 ** 5,
    "pi": 1024 ** 5,
    "pib": 1024 ** 5,
  }
  factor = factors.get(unit)
  return int(amount * factor) if factor is not None else None


async def set_bucket_hard_quota(
  server_name: str,
  bucket_name: str,
  quota_bytes: int,
  *,
  timeout: float = 20.0,
) -> tuple[bool, str]:
  if quota_bytes <= 0:
    return False, "存储桶配额必须大于 0"
  success, _, error, _ = await run_mc_json(
    [
      "quota", "set", f"{server_name}/{bucket_name}",
      "--size", f"{int(quota_bytes)}B",
    ],
    timeout=timeout,
  )
  return success, error


async def clear_bucket_hard_quota(
  server_name: str,
  bucket_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, str]:
  success, _, error, _ = await run_mc_json(
    ["quota", "clear", f"{server_name}/{bucket_name}"],
    timeout=timeout,
  )
  return success, error


async def get_bucket_hard_quota(
  server_name: str,
  bucket_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, int | None, str]:
  success, items, error, _ = await run_mc_json(
    ["quota", "info", f"{server_name}/{bucket_name}"],
    timeout=timeout,
    record=False,
  )
  if not success:
    quota_error = f"{error} {json.dumps(items, ensure_ascii=False)}".lower()
    if (
      "xminioadminnosuchquotaconfiguration" in quota_error
      or "quota configuration does not exist" in quota_error
      or "quota is not set" in quota_error
    ):
      return True, None, ""
    return False, None, error
  for item in reversed(items):
    for field in ("quota", "size", "hardQuota", "hard_quota"):
      if field not in item:
        continue
      parsed = _parse_mc_size_bytes(item.get(field))
      if parsed is not None:
        return True, parsed, ""
  return True, None, ""


async def get_bucket_usage_bytes(
  server_name: str,
  bucket_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, int, str]:
  success, items, error, _ = await run_mc_json(
    ["du", "--recursive", "--versions", f"{server_name}/{bucket_name}"],
    timeout=timeout,
    record=False,
  )
  if not success:
    return False, 0, error
  sizes = [
    parsed
    for item in items
    if (parsed := _parse_mc_size_bytes(item.get("size"))) is not None
  ]
  if not sizes:
    return False, 0, "MinIO 用量命令未返回 size"
  # mc du normally returns one summary line. max also tolerates clients that
  # emit both intermediate and final summary records.
  return True, max(sizes), ""


async def ensure_bucket_hard_quota(
  server_name: str,
  bucket_name: str,
  quota_bytes: int,
  *,
  timeout: float = 20.0,
) -> tuple[bool, str]:
  success, current, error = await get_bucket_hard_quota(
    server_name,
    bucket_name,
    timeout=timeout,
  )
  if not success:
    return False, error
  if current == quota_bytes:
    return True, ""
  return await set_bucket_hard_quota(
    server_name,
    bucket_name,
    quota_bytes,
    timeout=timeout,
  )


async def get_cluster_admin_info(
  server_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, dict[str, Any], str, float]:
  success, items, error, elapsed_ms = await run_mc_json(
    ["admin", "info", server_name],
    timeout=timeout,
    record=False,
  )
  return success, (items[-1] if items else {}), error, elapsed_ms


async def get_cluster_heal_info(
  server_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, dict[str, Any], str, float]:
  success, items, error, elapsed_ms = await run_mc_json(
    ["admin", "heal", f"{server_name}/"],
    timeout=timeout,
    record=False,
  )
  return success, (items[-1] if items else {}), error, elapsed_ms


async def inspect_cluster_heal(
  server_name: str,
  *,
  timeout: float = 3600.0,
) -> tuple[bool, list[dict[str, Any]], str, float]:
  return await run_mc_json(
    ["admin", "heal", "--force", f"{server_name}/"],
    timeout=timeout,
    record=False,
  )


async def get_bucket_replication_metrics(
  server_name: str,
  bucket_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, dict[str, Any], str, float]:
  success, items, error, elapsed_ms = await run_mc_json(
    ["replicate", "status", f"{server_name}/{bucket_name}"],
    timeout=timeout,
    record=False,
  )
  return success, (items[-1] if items else {}), error, elapsed_ms


async def start_bucket_replication_resync(
  server_name: str,
  bucket_name: str,
  remote_arn: str,
  *,
  older_than: str | None = None,
  timeout: float = 20.0,
) -> tuple[bool, dict[str, Any], str, float]:
  args = [
    "replicate", "resync", "start", f"{server_name}/{bucket_name}",
    "--remote-bucket", remote_arn,
  ]
  if older_than:
    args.extend(["--older-than", older_than])
  success, items, error, elapsed_ms = await run_mc_json(args, timeout=timeout)
  return success, (items[-1] if items else {}), error, elapsed_ms


async def get_bucket_replication_resync_status(
  server_name: str,
  bucket_name: str,
  remote_arn: str | None = None,
  *,
  timeout: float = 20.0,
) -> tuple[bool, dict[str, Any], str, float]:
  args = ["replicate", "resync", "status", f"{server_name}/{bucket_name}"]
  if remote_arn:
    args.extend(["--remote-bucket", remote_arn])
  success, items, error, elapsed_ms = await run_mc_json(
    args,
    timeout=timeout,
    record=False,
  )
  return success, (items[-1] if items else {}), error, elapsed_ms

def get_minio_client(host: str, port: int, access_key: str, secret_key: str) -> Minio:
  """
  获取 Minio 客户端
  
  Args:
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    Minio: Minio 客户端
  """
  endpoiont = f"{host}:{port}"
  try:
    minio_client = Minio(
      endpoint=endpoiont,
      access_key=access_key,
      secret_key=secret_key,
      secure=False,
    )
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_CONN_FAILED, str(e))
  return minio_client

def test_minio_server(host: str, port: int, access_key: str, secret_key: str):
  """
  连接 Minio 服务器
  
  Args:
    host: 服务器主机
    port: 服务器端口
    access_key: 访问密钥
    secret_key: 密钥

  Returns:
    None: 无返回值
  """
  minio_client = get_minio_client(host, port, access_key, secret_key)
  # 列出所有的存储桶
  bucket_list = []  
  try:
    buckets = minio_client.list_buckets()
    for bucket in buckets:
      bucket_list.append(bucket.name)
  except Exception as e:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))

async def check_server_bucket_existed(server_name: str, bucket_name: str) -> bool:
  """
  检查存储桶是否存在
  """
  cmd = shlex.join(["mc", "ls", f"{server_name}/{bucket_name}", "--json"])
  success, output = await _run_cmd(cmd)
  if not success:
    return False
  return True

async def create_bucket(server_name: str, bucket_name: str):
  """
  创建存储桶
  
  Args:
    minio_client: Minio 客户端
    bucket_name: 存储桶名称

  Returns:
    None: 无返回值
  """
  # try:
  #   minio_client.make_bucket(bucket_name)
  # except Exception as e:
  #   raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, str(e))
  success, err = await _run_cmd(f"mc mb {server_name}/{bucket_name}")
  return success, err
  if not success:
    raise CustomException(ErrorDesc.MINIO_CREATE_BUCKET_FAILED, str(err))

def _get_buckets_info_sync(client: Minio) -> list[dict[str, Any]]:
  """
  获取存储桶文件列表
  
  Args:
    client: Minio 客户端

  Returns:
    List[dict]: 存储桶文件列表
  """
  # 获取所有桶列表
  buckets = client.list_buckets()
  results = []
  for bucket in buckets:
    # 获取桶内对象（递归列出所有对象以获取最新状态）
    objects = client.list_objects(bucket.name, recursive=True)
    total_size = 0
    objects_list = []
    for obj in objects:
      total_size += obj.size
      objects_list.append({
        "name": obj.object_name,
        "size": obj.size,
        "last_modified": str(obj.last_modified)
      })
    file_tree = build_file_tree(objects_list)
    results.append({
      "name": "Bucket: " + bucket.name,
      "total_size": total_size,
      "created_at": str(bucket.creation_date),
      "files": file_tree
    })
  return results


async def get_buckets_info(client: Minio) -> list[dict[str, Any]]:
  """Collect a recursive inventory without blocking the FastAPI event loop."""
  return await asyncio.to_thread(_get_buckets_info_sync, client)

async def get_site_alias():
  """
  获取站点别名
  
  Args:
    site_name: 站点名称
  """
  data = {}
  alias_cmd = f"mc alias list --json"
  success, output = await _run_cmd(alias_cmd)
  if success:
    lines = [line.strip() for line in output.split('\n') if line.strip()]
    for line in lines:
      try:
        alias_item = json.loads(line)
        alias_name = alias_item["alias"]
        data[alias_name] = alias_item
      except json.JSONDecodeError:
        continue
  return data

async def set_site_alias(site_name, endpoint, admin_user, admin_password):
  """
  设置站点别名
  
  Args:
    site_name: 站点名称
    endpoint: 站点地址
    admin_user: 管理员用户名
    admin_password: 管理员密码

  Returns:
    Tuple[bool, str]: 设置结果
    True: 设置成功
    False: 设置失败
    str: 设置结果
  """
  alias_cmd = f"mc alias set {site_name} http://{endpoint} {admin_user} {admin_password}"
  success, _ = await _run_cmd(alias_cmd)
  if not success: return False, f"设置别名失败:{str(_)}"
  return True, "设置别名成功"

async def remove_site_alias(site_name):
  """
  删除站点别名
  
  Args:
    site_name: 站点名称

  Returns:
    Tuple[bool, str]: 删除结果
    True: 删除成功
    False: 删除失败
    str: 删除结果
  """
  alias_cmd = f"mc alias rm {site_name}"
  success, _ = await _run_cmd(alias_cmd)
  if not success: return False, f"删除别名失败:{str(_)}"
  return True, "删除别名成功"

async def add_new_site(master_name, site_name):
  """
  新增 MinIO 节点加入当前的复制集
  
  Args:
    master_name: 主站点名称
    site_name: 新站点名称

  Returns:
    Tuple[bool, str]: 加入结果
    True: 加入成功
    False: 加入失败
    str: 加入结果
  """
  # 将新站点加入复制集
  # 注意：Site Replication 要求所有站点在加入前必须是“空”的（或具有相同的初始状态）
  print(f"[*] 正在将 {site_name} 加入到 {master_name} 的复制集...")
  replicate_cmd = f"mc admin replicate add {master_name} {site_name}"
  success, output = await _run_cmd(replicate_cmd)
  return success, output

async def get_site_replication_status(master):
  """
  获取当前复制集状态
  
  Args:
    master: 主站点名称

  Returns:
    dict: 复制集状态
    None: 获取失败
  """
  # 使用 --json 参数便于 Python 解析
  cmd = f"mc admin replicate info {master} --json"
  success, output = await _run_cmd(cmd)
  if success:
      return json.loads(output)
  return None

async def list_server_buckets(
  server_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, list[str], str, float]:
  """List top-level buckets through mc and retain an actionable error."""
  success, items, error, elapsed_ms = await run_mc_json(
    ["ls", server_name],
    timeout=timeout,
    record=False,
    allow_empty=True,
  )
  if not success:
    return False, [], error, elapsed_ms
  buckets: set[str] = set()
  for item in items:
    key = str(item.get("key") or item.get("name") or "").strip()
    if not key:
      continue
    buckets.add(key.rstrip("/").rsplit("/", 1)[0])
  return True, sorted(buckets), "", elapsed_ms


async def get_bucket_object_summary(
  server_name: str,
  bucket_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, dict[str, int], str, float]:
  """Count all object versions before a guarded empty-bucket cleanup."""
  success, items, error, elapsed_ms = await run_mc_json(
    ["ls", "--recursive", "--versions", f"{server_name}/{bucket_name}"],
    timeout=timeout,
    record=False,
    allow_empty=True,
  )
  if not success:
    return False, {}, error, elapsed_ms
  object_count = 0
  version_count = 0
  total_bytes = 0
  for item in items:
    key = str(item.get("key") or item.get("name") or "").strip()
    if not key:
      continue
    object_count += 1
    if item.get("versionId") or item.get("versionID"):
      version_count += 1
    total_bytes += _parse_mc_size_bytes(item.get("size")) or 0
  return True, {
    "object_count": object_count,
    "version_count": version_count,
    "total_bytes": total_bytes,
  }, "", elapsed_ms


async def remove_empty_bucket(
  server_name: str,
  bucket_name: str,
  *,
  timeout: float = 20.0,
) -> tuple[bool, str, float]:
  """Remove a bucket only when MinIO confirms it is empty."""
  success, _items, error, elapsed_ms = await run_mc_json(
    ["rb", f"{server_name}/{bucket_name}"],
    timeout=timeout,
    allow_empty=True,
  )
  return success, error, elapsed_ms


async def get_server_buckets(server_name: str) -> List[str]:
  """Get the bucket names while preserving the legacy list-only API."""
  success, buckets, _error, _elapsed_ms = await list_server_buckets(server_name)
  return buckets if success else []

async def enable_bucket_versioning(server_name: str, bucket_name: str):
  """
  开启存储桶版本控制
  
  Args:
    server_name: 服务器名称
    bucket_name: 存储桶名称
  """
  cmd = f"mc version enable {server_name}/{bucket_name}"
  success, _ = await _run_cmd(cmd)
  if not success:
    return False, f"开启版本控制失败:{str(_)}"
  return True, "开启版本控制成功"

async def create_bucket_replicate(
  from_server: str,
  to_server: str,
  bucket_name: str,
  priority: int = 0,
  enabled: bool = True,
  replicate_options: list[str] | None = None,
):
  """
  创建存储桶复制
  
  Args:
    from_server: 源服务器名称
    to_server: 目标服务器名称
    bucket_name: 存储桶名称
  """
  options = replicate_options or ["delete", "delete-marker", "existing-objects"]
  args = [
    "mc", "replicate", "add", f"{from_server}/{bucket_name}",
    "--remote-bucket", f"{to_server}/{bucket_name}",
    "--replicate", ",".join(options),
    "--priority", str(priority),
  ]
  if not enabled:
    args.append("--disable")
  cmd = shlex.join(args)
  success, err = await _run_cmd(cmd)
  if not success:
    if "cluster replication setup" in str(err).lower():
      return False, (
        "源站点已启用 Site Replication，无法创建 Bucket Replication；"
        "请先将受管 MinIO 节点迁移为桶复制模式"
      )
    return False, f"创建复制失败:{str(err)}"
  return True, "创建复制成功"


async def delete_bucket_replicate(
  from_server: str,
  bucket_name: str,
  rule_id: str,
):
  """
  删除存储桶复制规则（mc replicate remove --id）。
  """
  args = [
    "mc", "replicate", "remove",
    "--id", rule_id,
    f"{from_server}/{bucket_name}",
  ]
  cmd = shlex.join(args)
  success, err = await _run_cmd(cmd)
  if not success:
    return False, f"删除复制失败:{str(err)}"
  return True, "删除复制成功"

async def get_bucket_replicate_status_result(
  server_name: str,
  bucket_name: str,
) -> tuple[bool, dict, str]:
  """
  获取存储桶复制状态
  
  Args:
    server_name: 服务器名称
    bucket_name: 存储桶名称
  """
  cmd = f"mc replicate ls {server_name}/{bucket_name} --json"
  success, output = await _run_cmd(cmd)
  data = {}
  if not success and _replication_config_not_set(output):
    return True, data, ""
  if success:
    lines = [line.strip() for line in output.split('\n') if line.strip()]
    for line in lines:
      try:
        status_item = json.loads(line)
        data[status_item["rule"]["ID"]] = status_item
      except (json.JSONDecodeError, KeyError, TypeError):
        continue
  return success, data, "" if success else str(output)


async def get_bucket_replicate_status(server_name: str, bucket_name: str):
  _, data, _ = await get_bucket_replicate_status_result(server_name, bucket_name)
  return data

async def get_bucket_replicate_entries_result(
  server: str,
  bucket: str,
) -> tuple[bool, list[dict[str, str]], str]:
  """Return every remote rule without collapsing duplicate destinations."""
  success, output = await _run_cmd(f"mc replicate ls {server}/{bucket}")
  if not success and _replication_config_not_set(output):
    return True, [], ""
  if not success:
    return False, [], str(output)

  endpoints: list[str] = []
  rule_ids: list[str] = []
  for line in output.splitlines():
    if "Remote Bucket:" in line:
      remote = line.split("Remote Bucket:")[-1].strip().removesuffix(f"/{bucket}")
      endpoint = remote.split("://", 1)[-1].rsplit("@", 1)[-1].rstrip("/")
      endpoints.append(endpoint)
    if "Rule ID:" in line:
      rule_ids.append(line.split("Rule ID:")[-1].strip())
  if len(endpoints) != len(rule_ids):
    return False, [], "mc replicate ls 返回的 Rule ID 与 Remote Bucket 数量不一致"
  entries = [
    {"endpoint": endpoint, "rule_id": rule_id}
    for endpoint, rule_id in zip(endpoints, rule_ids)
  ]
  return True, entries, ""


def _replication_config_not_set(output: object) -> bool:
  message = str(output or "").lower()
  return (
    "replication configuration" in message
    and ("not set" in message or "not found" in message)
  )


async def get_bucket_replicate_entries(server: str, bucket: str) -> list[dict[str, str]]:
  _, entries, _ = await get_bucket_replicate_entries_result(server, bucket)
  return entries

async def get_bucket_replicate_info(server: str, bucket: str):
  entries = await get_bucket_replicate_entries(server, bucket)
  return {entry["endpoint"]: entry["rule_id"] for entry in entries}
