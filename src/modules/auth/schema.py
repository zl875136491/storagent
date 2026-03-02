from pydantic import BaseModel
from typing import List

class LoginRequest(BaseModel):
  username: str
  password: str

class TokenResponse(BaseModel):
  access_token: str
  refresh_token: str
  token_type: str

class UserProfileResponse(BaseModel):
  id: str
  username: str
  name: str
  roles: List[str]
  created_at: str
  updated_at: str
  system_time: str