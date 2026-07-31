import json
import asyncio
import shlex
import subprocess
from minio import Minio
from typing import Any, List

from src.core.exception import CustomException, ErrorDesc
from src.utils.helpers import build_file_tree
from src.modules.public.crud import create_shell_command_log

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
  try:
    result = await asyncio.to_thread(
      subprocess.run,
      cmd,
      shell=True,
      check=True,
      capture_output=True,
      text=True,
    )
    await create_shell_command_log(cmd, result.stdout, result.stderr)
    return True, result.stdout
  except subprocess.CalledProcessError as e:
    await create_shell_command_log(cmd, e.stdout or "", e.stderr or "")
    return False, e.stderr or e.stdout or str(e)

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

async def get_buckets_info(client: Minio):
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

async def get_server_buckets(server_name: str) -> List[str]:
  """
  获取存储桶列表
  
  Args:
    server_name: 服务器名称

  Returns:
    List[str]: 存储桶列表
  """
  data = []
  cmd = f"mc ls {server_name} --json"
  success, output = await _run_cmd(cmd)
  if success:
    lines = [line.strip() for line in output.split('\n') if line.strip()]
    for line in lines:
      try:
        bucket_item = json.loads(line)
        if "key" in bucket_item:
          data.append(bucket_item["key"].rsplit("/", 1)[0])
        else:
          continue
      except json.JSONDecodeError:
        continue
  return data

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
