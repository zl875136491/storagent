"""虚拟存储错误。配额与状态冲突使用 409，凭据失败使用 401。"""
from __future__ import annotations

from fastapi import HTTPException


class VirtualStorageError(Exception):
  def __init__(self, message: str, error_code: str, status_code: int = 400):
    self.message = message
    self.error_code = error_code
    self.status_code = status_code
    super().__init__(message)


class AccountNotFoundError(VirtualStorageError):
  def __init__(self, app_name: str):
    super().__init__(f"虚拟账号 {app_name} 不存在", "ACCOUNT_NOT_FOUND", 404)


class AccountScopeError(VirtualStorageError):
  def __init__(self):
    super().__init__("token 不能访问该账号", "ACCOUNT_SCOPE_MISMATCH", 403)


class AccountDisabledError(VirtualStorageError):
  def __init__(self, app_name: str):
    super().__init__(f"虚拟账号 {app_name} 已停用", "ACCOUNT_DISABLED", 403)


class TenantNotFoundError(VirtualStorageError):
  def __init__(self, tenant_id: str):
    super().__init__(f"租户 {tenant_id} 不存在", "TENANT_NOT_FOUND", 404)


class TenantFrozenError(VirtualStorageError):
  def __init__(self, tenant_id: str):
    super().__init__(f"租户 {tenant_id} 已冻结", "TENANT_FROZEN", 403)


class TenantRetiredError(VirtualStorageError):
  def __init__(self, tenant_id: str):
    super().__init__(f"租户 {tenant_id} 已注销", "TENANT_RETIRED", 410)


class QuotaExceededError(VirtualStorageError):
  def __init__(self, message: str = "配额不足"):
    super().__init__(message, "QUOTA_EXCEEDED", 409)


class ConsoleForbiddenError(VirtualStorageError):
  def __init__(self, message: str):
    super().__init__(message, "CONSOLE_FORBIDDEN", 403)


class InvalidCredentialError(VirtualStorageError):
  def __init__(self, message: str = "凭据无效"):
    super().__init__(message, "INVALID_CREDENTIAL", 401)


class ConflictError(VirtualStorageError):
  def __init__(self, message: str, error_code: str = "CONFLICT"):
    super().__init__(message, error_code, 409)


class PreconditionError(VirtualStorageError):
  def __init__(self, message: str):
    super().__init__(message, "PRECONDITION_FAILED", 412)


class NotReadyError(VirtualStorageError):
  def __init__(self, message: str):
    super().__init__(message, "NOT_READY", 409)


class InfrastructureError(VirtualStorageError):
  def __init__(self, message: str):
    super().__init__(message, "INFRASTRUCTURE_UNAVAILABLE", 503)


class UnknownMinioResult(VirtualStorageError):
  """MinIO 超时或连接中断，调用结果无法判断。不得据此释放预留。"""

  def __init__(self, message: str = "对象存储结果不明"):
    super().__init__(message, "STORAGE_RESULT_UNKNOWN", 504)


def to_http_exception(error: VirtualStorageError) -> HTTPException:
  return HTTPException(
    status_code=error.status_code,
    detail={"error_code": error.error_code, "message": error.message},
  )
