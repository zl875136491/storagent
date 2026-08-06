"""
数据面能力令牌 (Capability Token)。

背景：v1 之前的文件接口要求前端持有并直接发送 `x-api-key`，一旦请求经过浏览器网络
面板即可被复制冒用，从而伪造任意上传 / 下载动作。v1 起彻底移除这种用法：`x-api-key`
只允许出现在 App 后端与 Storagent 之间的服务端请求中；浏览器前端要直连 Storagent 的
分片上传 / 下载接口，必须改为携带本模块签发的“能力令牌”。

设计（详见文档中心 v1 · 功能接口引导 的“控制面 / 数据面”一节）：
- Token 由 App 后端在自己的进程内签发，不需要额外请求 Storagent：
  使用双方共享的 x-api-key 明文作为 HMAC-SHA256 密钥，对一份只读的能力描述签名。
- Token 结构固定为 `Base64Url(Payload JSON).Base64Url(HMAC-SHA256 签名)`。
- Payload 字段：
  - `ref`：该 x-api-key 的 SHA256 摘要，用于 Storagent 反查是哪一枚 Key（与数据库
    查询哈希完全一致的单向摘要，不会泄露明文 Key）。
  - `act`：允许的动作，只能是 `upload_part` 或 `download`。
  - `key`：绑定的 object_key，必须与请求参数逐字节一致。
  - `exp`：Unix 秒级过期时间，App 后端应对上传令牌给 2 小时量级、下载令牌给
    5-15 分钟量级的极短有效期。
  - `uid`：仅分片上传场景使用，绑定 upload_id。
- Storagent 收到请求后按 `ref` 反查该 APIKey 记录并解密得到明文 Key，重新计算签名，
  并核对 `act` / `key`（及 `uid`）与请求参数完全一致，全部通过才放行；因此前端即使
  截获了 Token，也只能在有效期内对指定文件完成指定的单一动作，无法执行任何其他调用。
"""
import base64
import hashlib
import hmac
import json
import time
from typing import Optional

from src.core.exception import CustomException, ErrorDesc


def _b64url_encode(data: bytes) -> str:
  return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(data: str) -> bytes:
  padding = "=" * (-len(data) % 4)
  return base64.urlsafe_b64decode(data + padding)


def api_key_ref(plain_api_key: str) -> str:
  """能力令牌中标识 APIKey 的引用，与数据库查询哈希算法一致（单向、可安全暴露）。"""
  return hashlib.sha256(plain_api_key.encode("utf-8")).hexdigest()


def issue_capability_token(
  plain_api_key: str,
  *,
  action: str,
  object_key: str,
  expires_in_seconds: int,
  upload_id: Optional[str] = None,
) -> str:
  """
  App 后端调用：用共享的 x-api-key 明文本地签发短期能力令牌，无需请求 Storagent。
  """
  if expires_in_seconds <= 0:
    raise ValueError("expires_in_seconds 必须大于 0")
  payload: dict = {
    "ref": api_key_ref(plain_api_key),
    "act": action,
    "key": object_key,
    "exp": int(time.time()) + int(expires_in_seconds),
  }
  if upload_id:
    payload["uid"] = upload_id
  payload_bytes = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
  signature = hmac.new(plain_api_key.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
  return f"{_b64url_encode(payload_bytes)}.{_b64url_encode(signature)}"


def peek_capability_token_ref(token: str) -> str:
  """
  在校验签名前先取出 `ref` 以便反查签发所用的 APIKey。
  这一步本身不构成信任依据，最终必须以 verify_capability_token 的签名校验为准。
  """
  try:
    payload_part, _sig_part = token.split(".", 1)
    payload = json.loads(_b64url_decode(payload_part))
    ref = payload.get("ref")
  except Exception as error:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_INVALID, "token 格式不合法") from error
  if not isinstance(ref, str) or not ref:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_INVALID, "token 格式不合法")
  return ref


def verify_capability_token(
  token: str,
  plain_api_key: str,
  *,
  action: str,
  object_key: str,
  upload_id: Optional[str] = None,
) -> dict:
  """使用反查得到的明文 APIKey 校验签名、有效期与作用域；全部通过后返回 payload。"""
  try:
    payload_part, sig_part = token.split(".", 1)
    payload_bytes = _b64url_decode(payload_part)
    signature = _b64url_decode(sig_part)
    payload = json.loads(payload_bytes)
  except Exception as error:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_INVALID, "token 格式不合法") from error

  expected_signature = hmac.new(plain_api_key.encode("utf-8"), payload_bytes, hashlib.sha256).digest()
  if not hmac.compare_digest(expected_signature, signature):
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_INVALID, "token 签名不匹配")

  try:
    expires_at = int(payload.get("exp", 0))
  except (TypeError, ValueError) as error:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_INVALID, "token 过期时间不合法") from error
  if expires_at < int(time.time()):
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_EXPIRED, "token 已过期")

  if payload.get("act") != action:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH, "token 允许的动作与当前请求不匹配")
  if payload.get("key") != object_key:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH, "token 绑定的 object_key 与请求不匹配")
  bound_upload_id = payload.get("uid")
  if bound_upload_id and bound_upload_id != upload_id:
    raise CustomException(ErrorDesc.CAPABILITY_TOKEN_SCOPE_MISMATCH, "token 绑定的 upload_id 与请求不匹配")

  return payload
