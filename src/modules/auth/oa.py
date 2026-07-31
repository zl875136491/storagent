"""OA IM delivery for short-lived, one-time authentication links."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from urllib.parse import urlencode

import requests

from src.configs.configs import settings
from src.utils.logger import logger


@dataclass(frozen=True)
class OADeliveryResult:
  status: str
  detail: str = ""

  @property
  def accepted(self) -> bool:
    return self.status in ("sent", "unknown")


def build_login_link(username: str, code: str) -> str:
  query = urlencode({"username": username, "code": code})
  return f"{settings.FRONT_URL.rstrip('/')}/login_by_code?{query}"


def _send_once(username: str, title: str, content: str, code: str) -> requests.Response:
  url = (
    f"{settings.SPRINGBOARD_URL.rstrip('/')}"
    f"/send_gquan_msg/{settings.SPRINGBOARD_APP.strip()}"
  )
  return requests.post(
    url=url,
    data={
      "msg_type": "AGENDA",
      "to_itcode": username,
      "title": title,
      "desc": content,
      "content_or_url": build_login_link(username, code),
    },
    timeout=10,
  )


async def send_oa_auth_message(
  username: str,
  title: str,
  content: str,
  code: str,
) -> OADeliveryResult:
  """
  Retry the same link so a delayed response cannot invalidate a delivered code.

  A transport exception is ambiguous: the OA service may have accepted the
  message before the response was lost. The caller therefore keeps the local
  challenge valid and asks the user to check OA instead of issuing a new code.
  """
  retries = max(int(settings.OA_AUTH_SEND_RETRIES), 1)
  saw_transport_error = False
  last_detail = ""
  for attempt in range(retries):
    if attempt:
      await asyncio.sleep(attempt)
    try:
      response = await asyncio.to_thread(
        _send_once,
        username,
        title,
        content,
        code,
      )
    except requests.RequestException as exc:
      saw_transport_error = True
      last_detail = str(exc)
      logger.warning(
        f"OA 认证消息响应状态未知 username={username} attempt={attempt + 1}: {exc}"
      )
      continue

    if response.status_code == 200:
      return OADeliveryResult("sent")
    last_detail = f"HTTP {response.status_code}"
    logger.warning(
      f"OA 认证消息发送失败 username={username} attempt={attempt + 1} "
      f"status={response.status_code}"
    )

  if saw_transport_error:
    return OADeliveryResult("unknown", last_detail)
  return OADeliveryResult("failed", last_detail or "OA 服务拒绝了消息")
