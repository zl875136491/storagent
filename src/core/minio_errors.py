"""Classify MinIO failures into stable, safe API errors."""
from __future__ import annotations

from collections.abc import Iterator

from src.core.exception import CustomException, ErrorDesc


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


def exception_chain(error: BaseException) -> Iterator[BaseException]:
  """Iterate an exception and its causal chain without looping."""
  seen: set[int] = set()
  current: BaseException | None = error
  while current is not None and id(current) not in seen:
    seen.add(id(current))
    yield current
    current = current.__cause__ or current.__context__


def is_bucket_quota_exceeded(error: BaseException) -> bool:
  values = [
    str(error),
    str(getattr(error, "code", "")),
    str(getattr(error, "message", "")),
  ]
  normalized = " ".join(values).lower()
  return (
    "xminioadminbucketquotaexceeded" in normalized
    or "bucket quota exceeded" in normalized
  )


def classify_minio_error(
  error: BaseException,
  operation: str,
  *,
  quota_exceeded: bool = False,
) -> CustomException:
  """Map MinIO SDK/transport failures without returning implementation detail."""
  if quota_exceeded and is_bucket_quota_exceeded(error):
    return CustomException(
      ErrorDesc.APP_STORAGE_QUOTA_EXCEEDED,
      "APP 存储超出限额，请联系管理员处理",
    )

  chain = list(exception_chain(error))
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
