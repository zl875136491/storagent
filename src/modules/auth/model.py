from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
from typing import List
from pydantic import Field
from src.utils.helpers import utc_now
  
class Role(Document):
  name: str
  is_admin: bool = False
  permissions: List[str] = Field(default=[])
  
  class Settings:
    name = "role"
    indexes = [
      IndexModel(["name"], unique=True),
    ]
  
class User(Document):
  username: str
  name: str
  hashed_password: str
  roles: List[Link[Role]] = Field(default=[])
  permissions: List[str] = Field(default=[])
  """跨节点同步创建的占位用户，禁止登录"""
  is_sync: bool = Field(default=False)
  """密码重置时递增；JWT 中版本不匹配的旧会话立即失效"""
  auth_version: int = Field(default=0, ge=0)
  """角色集合变更时递增；用于跨区域字段级合并，避免覆盖密码字段"""
  roles_version: int = Field(default=0, ge=0)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  
  class Settings:
    name = "user"
    indexes = [
      IndexModel(["username"], unique=True),
    ]
  
class TempCode(Document):
  """仅保存在发起节点的 OA 一次性认证挑战，不参与 Etcd 同步。"""
  username: str
  code: str = Field(default="")  # 兼容旧数据；新流程只写 code_hash
  code_hash: str = Field(default="")
  purpose: str = Field(default="login")
  password_hash: str = Field(default="")
  display_name: str = Field(default="")
  delivery_status: str = Field(default="pending")
  created_at: datetime = Field(default_factory=utc_now)
  expired_at: datetime
  consumed_at: datetime | None = Field(default=None)
  
  class Settings:
    name = "temp_code"
    indexes = [
      IndexModel(["expired_at"]),
      IndexModel(["code_hash"]),
      IndexModel(["username", "purpose", "created_at"]),
    ]
  
class DestoryedToken(Document):
  """吊销的 JWT。优先用 token_hash 跨区同步；token 字段兼容旧数据。"""
  token: str = Field(default="")
  token_hash: str = Field(default="")
  expired_at: datetime
  
  class Settings:
    name = "destoryed_token"
    indexes = [
      IndexModel(["token"]),
      IndexModel(["token_hash"]),
      IndexModel(["expired_at"]),
    ]
