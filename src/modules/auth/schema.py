from pydantic import BaseModel
from typing import List
from beanie import PydanticObjectId

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