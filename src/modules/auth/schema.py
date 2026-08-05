from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Literal
from beanie import PydanticObjectId


_ITCODE_PATTERN = r"^[A-Za-z0-9._-]+$"


def _normalize_itcode(value: str) -> str:
  return value.strip().lower()


def _validate_password_bytes(value: str) -> str:
  if len(value.encode("utf-8")) > 72:
    raise ValueError("密码不能超过 72 字节")
  return value

class RefreshTokenRequest(BaseModel):
  refresh_token: str

class LoginRequest(BaseModel):
  username: str = Field(..., min_length=2, max_length=64, pattern=_ITCODE_PATTERN)
  password: str = Field(..., min_length=1, max_length=128)

  @field_validator("username", mode="before")
  @classmethod
  def normalize_username(cls, value: str) -> str:
    return _normalize_itcode(value)

  @field_validator("password")
  @classmethod
  def validate_password_bytes(cls, value: str) -> str:
    return _validate_password_bytes(value)


class AuthLinkRequest(BaseModel):
  username: str = Field(..., min_length=2, max_length=64, pattern=_ITCODE_PATTERN)

  @field_validator("username", mode="before")
  @classmethod
  def normalize_username(cls, value: str) -> str:
    return _normalize_itcode(value)


class PasswordPairRequest(AuthLinkRequest):
  password: str = Field(..., min_length=8, max_length=128)
  confirm_password: str = Field(..., min_length=8, max_length=128)

  @field_validator("password", "confirm_password")
  @classmethod
  def validate_password_bytes(cls, value: str) -> str:
    return _validate_password_bytes(value)

  @model_validator(mode="after")
  def passwords_match(self):
    if self.password != self.confirm_password:
      raise ValueError("两次输入的密码不一致")
    return self


class CodeLoginRequest(AuthLinkRequest):
  code: str = Field(..., min_length=20, max_length=256)

  @field_validator("code")
  @classmethod
  def normalize_code(cls, value: str) -> str:
    return value.strip()


class AuthRequestResponse(BaseModel):
  message: str
  expires_in_seconds: int = Field(..., ge=1)
  delivery_status: Literal["sent", "unknown"]

class TokenResponse(BaseModel):
  access_token: str
  refresh_token: str
  token_type: str

class SimpleRole(BaseModel):
  id: PydanticObjectId
  name: str

class UserProfileResponse(BaseModel):
  id: str
  username: str
  name: str
  is_admin: bool
  roles: List[SimpleRole]
  permissions: List[str]
  created_at: str
  updated_at: str
  system_time: str

class AdminUserItem(BaseModel):
  id: str
  username: str
  name: str
  is_admin: bool
  role_name: str
  roles: List[SimpleRole]
  permissions: List[str]
  created_at: str
  updated_at: str

class AdminUserListResponse(BaseModel):
  data: List[AdminUserItem]

class UpdateUserRoleRequest(BaseModel):
  roles: List[str] | None = Field(default=None, min_length=1, max_length=5)
  role: str | None = None

  @model_validator(mode="after")
  def validate_role_payload(self):
    if self.roles is not None and self.role is not None:
      raise ValueError("roles 与 role 只能提交一个")
    values = self.roles if self.roles is not None else [self.role]
    normalized = []
    for value in values:
      role_name = (value or "").strip()
      if not role_name:
        raise ValueError("角色不能为空")
      if role_name not in normalized:
        normalized.append(role_name)
    self.roles = normalized
    return self

  @property
  def role_names(self) -> List[str]:
    return list(self.roles or [])

class UpdateUserRoleResponse(BaseModel):
  id: str
  username: str
  name: str
  is_admin: bool
  role_name: str
  roles: List[SimpleRole]
  permissions: List[str]
  created_at: str
  updated_at: str
