from pydantic import BaseModel
from typing import List
from beanie import PydanticObjectId

class RefreshTokenRequest(BaseModel):
  refresh_token: str

class LoginRequest(BaseModel):
  username: str
  password: str

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
  created_at: str
  updated_at: str
  system_time: str

class AdminUserItem(BaseModel):
  id: str
  username: str
  name: str
  is_admin: bool
  role_name: str
  created_at: str
  updated_at: str

class AdminUserListResponse(BaseModel):
  data: List[AdminUserItem]

class UpdateUserRoleRequest(BaseModel):
  role: str  # "用户" | "管理员"

class UpdateUserRoleResponse(BaseModel):
  id: str
  username: str
  name: str
  is_admin: bool
  role_name: str
