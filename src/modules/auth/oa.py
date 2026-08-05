"""OA Agenda delivery, including short-lived authentication links."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
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


def _send_agenda_once(
  user_id: str,
  title: str,
  description: str,
  content_or_url: str,
) -> requests.Response:
  url = (
    f"{settings.SPRINGBOARD_URL.rstrip('/')}"
    f"/send_gquan_msg/{settings.SPRINGBOARD_APP.strip()}"
  )
  return requests.post(
    url=url,
    data={
      "msg_type": "AGENDA",
      "to_itcode": user_id,
      "title": title,
      "desc": description,
      "content_or_url": content_or_url,
    },
    timeout=10,
  )


def _send_once(username: str, title: str, content: str, code: str) -> requests.Response:
  return _send_agenda_once(
    username,
    title,
    content,
    build_login_link(username, code),
  )


async def _send_with_retries(
  send_once: Callable[[], requests.Response],
  user_id: str,
  *,
  retries: int | None,
  log_label: str,
  recipient_label: str,
) -> OADeliveryResult:
  retry_count = max(
    int(settings.OA_AUTH_SEND_RETRIES if retries is None else retries),
    1,
  )
  saw_transport_error = False
  last_detail = ""
  for attempt in range(retry_count):
    if attempt:
      await asyncio.sleep(attempt)
    try:
      response = await asyncio.to_thread(send_once)
    except requests.RequestException as exc:
      saw_transport_error = True
      last_detail = str(exc)
      logger.warning(
        f"{log_label}响应状态未知 {recipient_label}={user_id} "
        f"attempt={attempt + 1}: {exc}"
      )
      continue

    if response.status_code == 200:
      return OADeliveryResult("sent")
    last_detail = f"HTTP {response.status_code}"
    logger.warning(
      f"{log_label}发送失败 {recipient_label}={user_id} attempt={attempt + 1} "
      f"status={response.status_code}"
    )

  if saw_transport_error:
    return OADeliveryResult("unknown", last_detail)
  return OADeliveryResult("failed", last_detail or "OA 服务拒绝了消息")


async def send_agenda_message(
  user_id: str,
  title: str,
  description: str,
  content_or_url: str,
  *,
  retries: int | None = None,
) -> OADeliveryResult:
  """Send a generic OA Agenda message to one user ID."""
  return await _send_with_retries(
    partial(
      _send_agenda_once,
      user_id,
      title,
      description,
      content_or_url,
    ),
    user_id,
    retries=retries,
    log_label="OA Agenda 消息",
    recipient_label="user_id",
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
  return await _send_with_retries(
    partial(_send_once, username, title, content, code),
    username,
    retries=None,
    log_label="OA 认证消息",
    recipient_label="username",
  )
