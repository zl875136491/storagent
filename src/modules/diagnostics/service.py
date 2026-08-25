"""Self-diagnosis script rendering and authenticated report persistence."""
from __future__ import annotations

import asyncio
import json
import re
import secrets
from io import BytesIO

from src.core.exception import CustomException, ErrorDesc
from src.configs.configs import settings
from src.modules.public.model import DiagnosticRun
from src.utils.logger import logger


_MINIO_AUTH_ERROR_CODES = frozenset({
  "AccessDenied",
  "AuthorizationHeaderMalformed",
  "ExpiredToken",
  "InvalidAccessKeyId",
  "InvalidToken",
  "SignatureDoesNotMatch",
})
_MINIO_NETWORK_ERROR_CODES = frozenset({
  "InternalError",
  "RequestTimeout",
  "ServiceUnavailable",
  "SlowDown",
})
_NETWORK_EXCEPTION_NAMES = frozenset({
  "ConnectTimeoutError",
  "MaxRetryError",
  "NewConnectionError",
  "ProtocolError",
  "ReadTimeoutError",
  "SSLError",
})


def _exception_chain(error: BaseException):
  seen: set[int] = set()
  current: BaseException | None = error
  while current is not None and id(current) not in seen:
    seen.add(id(current))
    yield current
    current = current.__cause__ or current.__context__


def _storage_probe_error(error: BaseException, operation: str) -> CustomException:
  """Return a safe, actionable public error without exposing MinIO secrets."""
  chain = list(_exception_chain(error))
  for item in chain:
    source_code = str(getattr(item, "code", "") or "").strip()
    if source_code in _MINIO_AUTH_ERROR_CODES:
      return CustomException(
        ErrorDesc.MINIO_AUTH_FAILED,
        {
          "operation": operation,
          "category": "authentication",
          "source_code": source_code,
        },
      )
  for item in chain:
    source_code = str(getattr(item, "code", "") or "").strip()
    if (
      source_code in _MINIO_NETWORK_ERROR_CODES
      or isinstance(item, (TimeoutError, ConnectionError, OSError))
      or type(item).__name__ in _NETWORK_EXCEPTION_NAMES
    ):
      details = {"operation": operation, "category": "network"}
      if source_code:
        details["source_code"] = source_code
      return CustomException(ErrorDesc.MINIO_NETWORK_UNAVAILABLE, details)
  return CustomException(
    ErrorDesc.MINIO_ACCESS_FAILED,
    {"operation": operation, "category": "operation"},
  )


def _percent(used: int, total: int) -> float:
  if total <= 0:
    return 0.0
  return round(max(used, 0) / total * 100, 2)


def _format_bytes(value: int) -> str:
  size = float(max(value, 0))
  units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
  for index, unit in enumerate(units):
    if size < 1024 or index == len(units) - 1:
      return f"{int(size)} {unit}" if index == 0 else f"{size:.1f} {unit}"
    size /= 1024
  return "0 B"


def validate_version(version: str) -> str:
  if version not in ("v1", "v2"):
    raise CustomException(ErrorDesc.INVALID_PARAMS, "仅支持 v1 或 v2 自诊断脚本")
  return version


def render_script(version: str) -> str:
  """Render a portable interactive checker for one versioned API contract."""
  version = validate_version(version)
  template = r'''#!/usr/bin/env sh
# Storagent caller-side self-diagnosis.
#
# This script is intended to run on the caller's backend host. It never writes
# the APIKey to the diagnostic report, terminal output, or local files.

set -u

API_VERSION="__VERSION__"
API_PREFIX="__API_PREFIX__"
DEFAULT_BASE=__DEFAULT_BASE__
GATEWAY_ORIGIN=__GATEWAY_ORIGIN__
RUN_ID="diag-$(date +%s)-$$"

TMP_DIR=""
RESPONSE_FILE=""
ERROR_FILE=""
CHECKS_JSON=""
CHECK_SEPARATOR=""
RUN_LOG=""
OVERALL_STATUS="passed"
AUTHENTICATED=false

HTTP_STATUS="000"
HTTP_LATENCY_MS=0
CURL_EXIT=0
CURL_ERROR=""

cleanup() {
  [ -n "$TMP_DIR" ] && rm -rf "$TMP_DIR"
}

fail() {
  printf '%s\n' "错误: $1" >&2
  exit 2
}

ask() {
  # When invoked as `curl ... | sh`, stdin is the downloaded script itself.
  # Read interactive answers from the caller's terminal so the first prompt
  # does not consume the script stream and turn an empty answer into EOF.
  answer=""
  printf '%s' "$1" >&2
  if [ ! -r /dev/tty ]; then
    printf '%s\n' "错误: 自诊断脚本需要交互式终端，请先下载脚本后直接运行，或使用带 TTY 的 curl | sh。" >&2
    return 2
  fi
  IFS= read -r answer </dev/tty || return 2
  printf '%s' "$answer"
}

ask_secret() {
  # APIKey is only needed for the current process and must not be echoed.
  answer=""
  printf '%s' "$1" >&2
  if [ ! -r /dev/tty ]; then
    printf '%s\n' "错误: 自诊断脚本需要交互式终端，请先下载脚本后直接运行，或使用带 TTY 的 curl | sh。" >&2
    return 2
  fi
  if command -v stty >/dev/null 2>&1; then
    stty -echo </dev/tty 2>/dev/null || true
    IFS= read -r answer </dev/tty
    read_status=$?
    stty echo </dev/tty 2>/dev/null || true
    printf '\n' >&2
    [ "$read_status" -eq 0 ] || return 2
  else
    IFS= read -r answer </dev/tty || return 2
  fi
  printf '%s' "$answer"
}

trim() {
  printf '%s' "$1" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//'
}

json_escape() {
  # Details are generated locally and never include the APIKey.
  printf '%s' "$1" | tr '\r\n\t' '   ' | sed 's/\\/\\\\/g; s/"/\\"/g'
}

normalize_base_url() {
  value="$(trim "$1")"
  value="${value%/}"
  case "$value" in
    ""|local) printf '%s' "$DEFAULT_BASE" ;;
    bj|tj|ks|sz|hz) printf '%s' "${GATEWAY_ORIGIN}/server/${value}" ;;
    http://*|https://*) printf '%s' "$value" ;;
    *) return 1 ;;
  esac
}
validate_base_url() {
  printf '%s' "$1" | grep -Eq '^https?://[^/?#[:space:]]+(/[^?#[:space:]]*)?$'
}

url_host() {
  authority="${1#*://}"
  authority="${authority%%/*}"
  printf '%s' "${authority%%:*}"
}

add_check() {
  check_name=$1
  check_status=$2
  check_detail=$3
  check_latency=$4
  display_detail=$check_detail
  if [ "$#" -ge 5 ]; then
    display_detail=$5
  fi
  escaped_detail="$(json_escape "$check_detail")"

  CHECKS_JSON="${CHECKS_JSON}${CHECK_SEPARATOR}{\"name\":\"${check_name}\",\"status\":\"${check_status}\",\"detail\":\"${escaped_detail}\",\"latency_ms\":${check_latency}}"
  CHECK_SEPARATOR=","
  RUN_LOG="${RUN_LOG}${check_name}:${check_status}:${check_detail};"

  case "$check_status" in
    passed) marker="PASS" ;;
    skipped) marker="SKIP" ;;
    *) marker="FAIL" ;;
  esac
  printf '[%s] %-16s %s\n' "$marker" "$check_name" "$display_detail"
}

update_overall_status() {
  case "$1" in
    failed) OVERALL_STATUS="failed" ;;
    skipped) [ "$OVERALL_STATUS" = "passed" ] && OVERALL_STATUS="partial" ;;
  esac
}

request() {
  method=$1
  url=$2
  payload=$3
  timeout=$4
  include_api_key=$5

  : >"$RESPONSE_FILE"
  : >"$ERROR_FILE"

  if [ "$include_api_key" = "true" ]; then
    if [ "$method" = "POST" ]; then
      HTTP_META="$(curl -sS --connect-timeout 5 --max-time "$timeout" -X POST -H "x-api-key: $API_KEY" -H "Content-Type: application/json" --data "$payload" -o "$RESPONSE_FILE" -w '%{http_code} %{time_total}' "$url" 2>"$ERROR_FILE")"
    else
      HTTP_META="$(curl -sS --connect-timeout 5 --max-time "$timeout" -X "$method" -H "x-api-key: $API_KEY" -o "$RESPONSE_FILE" -w '%{http_code} %{time_total}' "$url" 2>"$ERROR_FILE")"
    fi
  elif [ "$method" = "POST" ]; then
    HTTP_META="$(curl -sS --connect-timeout 5 --max-time "$timeout" -X POST -H "Content-Type: application/json" --data "$payload" -o "$RESPONSE_FILE" -w '%{http_code} %{time_total}' "$url" 2>"$ERROR_FILE")"
  else
    HTTP_META="$(curl -sS --connect-timeout 5 --max-time "$timeout" -X "$method" -o "$RESPONSE_FILE" -w '%{http_code} %{time_total}' "$url" 2>"$ERROR_FILE")"
  fi
  CURL_EXIT=$?

  HTTP_STATUS="${HTTP_META%% *}"
  [ -n "$HTTP_STATUS" ] || HTTP_STATUS="000"
  HTTP_SECONDS="${HTTP_META#* }"
  [ "$HTTP_SECONDS" = "$HTTP_META" ] && HTTP_SECONDS=0
  HTTP_LATENCY_MS="$(awk -v seconds="$HTTP_SECONDS" 'BEGIN { printf "%.0f", seconds * 1000 }')"
  CURL_ERROR="$(tr '\r\n' ' ' <"$ERROR_FILE" | sed 's/[[:space:]][[:space:]]*/ /g; s/[[:space:]]*$//')"
}

request_failure_detail() {
  if [ "$CURL_EXIT" -ne 0 ]; then
    printf 'curl 退出码 %s%s' "$CURL_EXIT" "${CURL_ERROR:+: $CURL_ERROR}"
  else
    body="$(tr '\r\n' ' ' <"$RESPONSE_FILE" | cut -c 1-160)"
    printf 'HTTP %s%s' "$HTTP_STATUS" "${body:+: $body}"
  fi
}

response_summary() {
  sed -n 's/.*"summary"[[:space:]]*:[[:space:]]*"\([^"\\]*\)".*/\1/p' "$RESPONSE_FILE" | head -n 1
}

response_compact_json() {
  tr '\r\n' ' ' <"$RESPONSE_FILE" | sed 's/[[:space:]][[:space:]]*/ /g; s/^[[:space:]]*//; s/[[:space:]]*$//'
}

run_dns_check() {
  # Test endpoints use a direct NUC IP. DNS is not applicable to an IP literal.
  if printf '%s' "$HOST" | grep -Eq '^[0-9]{1,3}(\.[0-9]{1,3}){3}$' || printf '%s' "$HOST" | grep -q ':'; then
    add_check dns skipped "基础地址使用 IP ${HOST}，跳过 DNS 解析检查" 0
    return
  fi

  if command -v getent >/dev/null 2>&1; then
    if getent ahosts "$HOST" >/dev/null 2>&1; then
      add_check dns passed "DNS 已解析 $HOST" 0
      return
    fi
    add_check dns failed "DNS 无法解析 $HOST" 0
    update_overall_status failed
    return
  fi

  if command -v dig >/dev/null 2>&1; then
    if dig +short "$HOST" 2>/dev/null | grep -q '.'; then
      add_check dns passed "DNS 已解析 $HOST" 0
      return
    fi
    add_check dns failed "DNS 无法解析 $HOST" 0
    update_overall_status failed
    return
  fi

  add_check dns skipped "未找到 getent 或 dig，跳过独立 DNS 检测" 0
  update_overall_status skipped
}

run_gateway_check() {
  request GET "${BASE_URL}/health" "" 12 false
  if [ "$HTTP_STATUS" = "200" ]; then
    add_check gateway passed "网关健康检查 HTTP 200" "$HTTP_LATENCY_MS"
  else
    add_check gateway failed "网关健康检查失败: $(request_failure_detail)" "$HTTP_LATENCY_MS"
    update_overall_status failed
  fi
}

run_authentication_check() {
  request GET "${BASE_URL}${API_PREFIX}/diagnostics/${API_VERSION}/probe" "" 15 true
  if [ "$HTTP_STATUS" = "200" ] && grep -Eq '"authenticated"[[:space:]]*:[[:space:]]*true' "$RESPONSE_FILE"; then
    AUTHENTICATED=true
    add_check authentication passed "APIKey 与 ${API_VERSION} 契约验证通过" "$HTTP_LATENCY_MS"
  else
    add_check authentication failed "认证或版本契约失败: $(request_failure_detail)" "$HTTP_LATENCY_MS"
    update_overall_status failed
  fi
}

run_quota_capacity_check() {
  if [ "$MODE" = "1" ]; then
    add_check quota_capacity skipped "已选择网络与认证检测，跳过配额与容量预检" 0
    update_overall_status skipped
    return
  fi
  if [ "$AUTHENTICATED" != "true" ]; then
    add_check quota_capacity skipped "认证未通过，跳过配额与容量预检" 0
    return
  fi

  request GET "${BASE_URL}${API_PREFIX}/diagnostics/${API_VERSION}/quota-capacity-probe" "" 30 true
  summary="$(response_summary)"
  [ -n "$summary" ] || summary="配额与容量预检未返回摘要"
  response_json="$(response_compact_json)"
  if [ "$HTTP_STATUS" = "200" ] && grep -Eq '"ready"[[:space:]]*:[[:space:]]*true' "$RESPONSE_FILE"; then
    add_check quota_capacity passed "${summary}: ${response_json}" "$HTTP_LATENCY_MS" "$summary"
  elif [ "$HTTP_STATUS" = "200" ]; then
    add_check quota_capacity failed "${summary}: ${response_json}" "$HTTP_LATENCY_MS" "$summary"
    update_overall_status failed
  else
    add_check quota_capacity failed "配额与容量预检失败: $(request_failure_detail)" "$HTTP_LATENCY_MS"
    update_overall_status failed
  fi
}

run_storage_check() {
  if [ "$MODE" = "1" ]; then
    add_check storage skipped "已选择网络与认证检测，跳过临时对象读写" 0
    update_overall_status skipped
    return
  fi
  if [ "$AUTHENTICATED" != "true" ]; then
    add_check storage skipped "认证未通过，跳过临时对象读写" 0
    return
  fi

  payload="{\"run_id\":\"${RUN_ID}\"}"
  request POST "${BASE_URL}${API_PREFIX}/diagnostics/${API_VERSION}/storage-probe" "$payload" 30 true
  if [ "$HTTP_STATUS" = "200" ] && grep -Eq '"storage"[[:space:]]*:[[:space:]]*"passed"' "$RESPONSE_FILE"; then
    add_check storage passed "临时对象上传、读取和清理完成" "$HTTP_LATENCY_MS"
  else
    add_check storage failed "存储读写失败: $(request_failure_detail)" "$HTTP_LATENCY_MS"
    update_overall_status failed
  fi
}

report_run() {
  escaped_log="$(json_escape "$RUN_LOG")"
  report="{\"run_id\":\"${RUN_ID}\",\"network_only\":${NETWORK_ONLY},\"overall_status\":\"${OVERALL_STATUS}\",\"checks\":[${CHECKS_JSON}],\"raw_log\":\"${escaped_log}\"}"
  request POST "${BASE_URL}${API_PREFIX}/diagnostics/${API_VERSION}/report" "$report" 15 true
  if [ "$HTTP_STATUS" = "200" ] || [ "$HTTP_STATUS" = "201" ]; then
    printf '[PASS] %-16s 诊断日志已回传（HTTP %s）\n' report "$HTTP_STATUS"
  else
    printf '[FAIL] %-16s 诊断日志回传失败: %s\n' report "$(request_failure_detail)" >&2
  fi
}

read_mode() {
  while :; do
    mode="$(ask "检测范围 [1] 网络与认证 [2] 完整读写（默认 2）: ")" || return 2
    case "$mode" in
      ""|2) printf '%s' 2; return ;;
      1) printf '%s' 1; return ;;
      *) printf '%s\n' "请输入 1 或 2。" >&2 ;;
    esac
  done
}

command -v curl >/dev/null 2>&1 || fail "未找到 curl，请先安装 curl。"
command -v mktemp >/dev/null 2>&1 || fail "未找到 mktemp，无法安全创建临时文件。"

BASE_INPUT="$(ask "Storagent 基础地址（回车默认 local；也可输入 local/bj/tj/ks/sz/hz 或完整地址）[${DEFAULT_BASE}]: ")" || fail "无法读取基础地址。请在交互式终端中运行此脚本。"
BASE_URL="$(normalize_base_url "$BASE_INPUT")" || fail "基础地址无效。请输入区域代号或 http(s) 完整地址。"
validate_base_url "$BASE_URL" || fail "基础地址格式无效: $BASE_URL"

API_KEY="$(ask_secret "APIKey（输入时不显示）: ")" || fail "无法读取 APIKey。请在交互式终端中运行此脚本。"
API_KEY="$(trim "$API_KEY")"
[ -n "$API_KEY" ] || fail "APIKey 不能为空。"

MODE="$(read_mode)" || fail "无法读取检测范围。请在交互式终端中运行此脚本。"
NETWORK_ONLY=false
[ "$MODE" = "1" ] && NETWORK_ONLY=true
HOST="$(url_host "$BASE_URL")"
[ -n "$HOST" ] || fail "无法从基础地址识别主机名。"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/storagent-diagnosis.XXXXXX")" || fail "无法创建临时目录。"
RESPONSE_FILE="${TMP_DIR}/response.json"
ERROR_FILE="${TMP_DIR}/curl-error.log"
trap cleanup EXIT HUP INT TERM

printf '开始 Storagent %s 自诊断: %s\n' "$API_VERSION" "$BASE_URL"
run_dns_check
run_gateway_check
run_authentication_check
run_quota_capacity_check
run_storage_check
report_run
printf '诊断完成: %s\n' "$OVERALL_STATUS"
'''
  return (template
    .replace("__VERSION__", version)
    .replace("__API_PREFIX__", "/api/" + version)
    .replace("__DEFAULT_BASE__", json.dumps(settings.DIAGNOSTIC_SCRIPT_DEFAULT_BASE.rstrip("/")))
    .replace("__GATEWAY_ORIGIN__", json.dumps(settings.DIAGNOSTIC_SCRIPT_GATEWAY_ORIGIN.rstrip("/"))))

def row_response(row: DiagnosticRun) -> dict:
  return {
    "id": str(row.id), "run_id": row.run_id, "api_version": row.api_version,
    "app_name": row.app_name,
    "source_host": row.source_host, "network_only": row.network_only,
    "overall_status": row.overall_status, "checks": row.checks,
    "raw_log": row.raw_log, "created_at": row.created_at,
  }


async def save_report(version: str, payload, app_context: dict, source_host: str) -> dict:
  version = validate_version(version)
  app_name = str(app_context["app_name"])
  existing = await DiagnosticRun.find_one(DiagnosticRun.run_id == payload.run_id)
  if existing:
    if existing.app_name != app_name:
      raise CustomException(ErrorDesc.INSUFFICIENT_PERMISSIONS, "诊断运行标识不属于当前应用")
    return row_response(existing)
  row = DiagnosticRun(
    run_id=payload.run_id, api_version=version, app_name=app_name,
    source_host=source_host[:256],
    network_only=payload.network_only, overall_status=payload.overall_status,
    checks=[item.model_dump() for item in payload.checks], raw_log=payload.raw_log,
  )
  await row.insert()
  return row_response(row)


async def list_runs(app_context: dict | None = None) -> dict:
  query = DiagnosticRun.find_all() if app_context is None else DiagnosticRun.find(
    DiagnosticRun.app_name == app_context["app_name"],
  )
  rows = await query.sort("-created_at").limit(200).to_list()
  return {"data": [row_response(row) for row in rows]}


async def quota_capacity_probe(app_context: dict) -> dict:
  """Return an APIKey-scoped quota and aggregate physical-capacity preflight."""
  from src.modules.public import service as public_service
  from src.modules.storage import operations

  app_name = str(app_context["app_name"])
  (quota_bytes, quota_usage_bytes), overview = await asyncio.gather(
    public_service.get_application_quota_usage(
      app_name,
      force=False,
      require_all=True,
    ),
    operations.get_cluster_health_overview(),
  )
  quota_bytes = max(int(quota_bytes or 0), 0)
  quota_usage_bytes = max(int(quota_usage_bytes or 0), 0)
  quota_available_bytes = max(quota_bytes - quota_usage_bytes, 0)

  cluster_summary = overview.get("summary") if isinstance(overview, dict) else {}
  cluster_summary = cluster_summary if isinstance(cluster_summary, dict) else {}
  raw_capacity_bytes = max(int(cluster_summary.get("raw_capacity_bytes") or 0), 0)
  raw_used_bytes = max(int(cluster_summary.get("raw_used_bytes") or 0), 0)
  raw_available_bytes = max(raw_capacity_bytes - raw_used_bytes, 0)
  cluster_count = max(int(cluster_summary.get("cluster_count") or 0), 0)
  online_cluster_count = max(int(cluster_summary.get("online_clusters") or 0), 0)
  cluster_status = str(cluster_summary.get("status") or "offline")

  blocking_reasons: list[str] = []
  if quota_bytes <= 0:
    blocking_reasons.append("应用配额未配置")
  elif quota_available_bytes <= 0:
    blocking_reasons.append("应用可用配额不足")
  if cluster_count <= 0 or online_cluster_count <= 0 or cluster_status == "offline":
    blocking_reasons.append("没有可用的存储集群")
  elif raw_capacity_bytes <= 0 or raw_available_bytes <= 0:
    blocking_reasons.append("集群物理可用容量不足")

  warnings: list[str] = []
  quota_usage_percent = _percent(quota_usage_bytes, quota_bytes)
  raw_usage_percent = _percent(raw_used_bytes, raw_capacity_bytes)
  if quota_usage_percent >= 85:
    warnings.append("应用配额使用率已达到 85%")
  if raw_usage_percent >= 85:
    warnings.append("集群物理容量使用率已达到 85%")
  if cluster_status not in ("online", "offline"):
    warnings.append("集群容量状态不是完全在线")

  return {
    "ready": not blocking_reasons,
    "summary": (
      f"应用配额可用 {_format_bytes(quota_available_bytes)}（{quota_usage_percent:.1f}% 已用）；"
      f"集群物理可用 {_format_bytes(raw_available_bytes)}（{raw_usage_percent:.1f}% 已用）"
    ),
    "quota": {
      "limit_bytes": quota_bytes,
      "used_bytes": quota_usage_bytes,
      "available_bytes": quota_available_bytes,
      "usage_percent": quota_usage_percent,
    },
    "cluster": {
      "status": cluster_status,
      "cluster_count": cluster_count,
      "online_cluster_count": online_cluster_count,
      "raw_capacity_bytes": raw_capacity_bytes,
      "raw_used_bytes": raw_used_bytes,
      "raw_available_bytes": raw_available_bytes,
      "raw_usage_percent": raw_usage_percent,
    },
    "warnings": warnings,
    "blocking_reasons": blocking_reasons,
  }


async def storage_probe(app_context: dict, run_id: str) -> dict:
  """Write, read and always remove a small server-side temporary object."""
  from src.core.minio_op import get_minio_client
  from src.modules.storage import crud as storage_crud
  servers = await storage_crud.read_minio_server_list()
  if not servers:
    raise CustomException(ErrorDesc.MINIO_ACCESS_FAILED, "未找到可用 MinIO 服务")
  server = next((item for item in servers if item.master), servers[0])
  key = ".storagent-diagnostics/" + re.sub(r"[^A-Za-z0-9._-]", "-", run_id)[:96] + ".txt"
  payload = secrets.token_bytes(128)
  try:
    access_key, secret_key = storage_crud.plain_minio_credentials(server)
  except Exception as error:
    raise CustomException(
      ErrorDesc.MINIO_AUTH_FAILED,
      {"operation": "credentials", "category": "authentication"},
    ) from error
  client = get_minio_client(server.host, server.minio_port, access_key, secret_key)
  uploaded = False
  primary_error: BaseException | None = None
  try:
    await asyncio.to_thread(client.put_object, app_context["app_name"], key, BytesIO(payload), len(payload))
    uploaded = True
    response = await asyncio.to_thread(client.get_object, app_context["app_name"], key)
    try:
      received = response.read()
    finally:
      response.close()
      response.release_conn()
    if received != payload:
      raise RuntimeError("读取内容与写入内容不一致")
  except Exception as error:
    primary_error = error
    raise _storage_probe_error(error, "read_write") from error
  finally:
    if uploaded:
      try:
        await asyncio.to_thread(client.remove_object, app_context["app_name"], key)
      except Exception as error:
        if primary_error is None:
          raise _storage_probe_error(error, "cleanup") from error
        logger.warning(
          "诊断临时对象清理失败，但保留原始诊断错误: primary={} cleanup={}",
          type(primary_error).__name__,
          type(error).__name__,
        )
  return {"storage": "passed", "object_prefix": ".storagent-diagnostics/"}
