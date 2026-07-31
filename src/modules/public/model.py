from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
from pydantic import Field, model_validator
from src.utils.helpers import utc_now
from src.modules.auth.model import User
from typing import List, Literal
from pydantic import BaseModel

class Region(Document):
  name: str
  shown_name: str
  
  class Settings:
    name = "region"
    indexes = [
      IndexModel(["name"], unique=True),
      IndexModel(["shown_name"], unique=True),
    ]

class Application(Document):
  name: str
  shown_name: str = Field(default="")
  description: str = Field(default="")
  enabled: bool = Field(default=False)
  created_at: datetime = Field(default_factory=utc_now)
  updated_at: datetime = Field(default_factory=utc_now)
  enabled_at: datetime | None = Field(default=None)
  provisioning_status: Literal["pending", "provisioning", "ready", "failed", "degraded"] | None = Field(default=None)
  provisioning_error: str = Field(default="")
  provisioning_updated_at: datetime | None = Field(default=None)
  # regions: List[Link[Region]] = Field(default=[])
  author: Link[User]
  approver: Link[User] | None = Field(default=None)

  @model_validator(mode="after")
  def normalize_legacy_provisioning_status(self):
    # Existing enabled applications predate provisioning state and already
    # passed the legacy authorization flow.
    if self.provisioning_status is None:
      self.provisioning_status = "ready" if self.enabled else "pending"
    return self
  
  class Settings:
    name = "application"
    indexes = [
      IndexModel(["name"], unique=True),
      IndexModel(["shown_name"], unique=True),
    ]

class APIKey(Document):
  application: Link[Application]
  """SHA256(明文 Key)，用于查询；历史数据可能仍为明文直至访问时迁移"""
  key: str
  """展示用脱敏片段，如 sk_xxxx************abcd"""
  key_hint: str = Field(default="")
  """Fernet 加密的明文 Key，供 Etcd 同步/再发布"""
  key_enc: str = Field(default="")
  expired_at: datetime # 过期时间
  deleted: bool = Field(default=False)
  deleted_at: datetime | None = Field(default=None)
  """由管理员吊销时为 True；所有者可见「被管理人员注销」"""
  destory_by_admin: bool = Field(default=False)
  
  class Settings:
    name = "api_key"
    indexes = [
      IndexModel(["key"], unique=True),
      IndexModel(["application"]),
      IndexModel(["expired_at"]),
    ]

class APIKeyUsageData(BaseModel):
  date: datetime
  server: str
  type: str
  size: int

class APIKeyUsage(Document):
  api_key: Link[APIKey]
  is_full: bool = Field(default=False)
  data: List[APIKeyUsageData] = Field(default=[]) # 最大3000条数据
  full_at: datetime | None = Field(default=None) # 满3000条数据的时间

class SystemConfig(Document):
  key: str
  value: str
  name: str
  description: str
  value_type: str
  
  class Settings:
    name = "system_config"
    indexes = [
      IndexModel(["key"], unique=True),
    ]

class ShellCommandLog(Document):
  command: str
  date: datetime = Field(default_factory=utc_now)
  stdout: str = Field(default="")
  stderr: str = Field(default="")
  
  class Settings:
    name = "shell_command"
    indexes = [
      IndexModel(["date"]),
    ]

class AuditEvent(Document):
  """关键操作审计落库（与日志 [AUDIT] 互补，便于检索）"""
  action: str
  actor: str = Field(default="-")
  resource: str = Field(default="-")
  success: bool = Field(default=True)
  detail: str = Field(default="")
  region: str = Field(default="")
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "audit_event"
    indexes = [
      IndexModel([("created_at", -1)]),
      IndexModel(["action"]),
      IndexModel(["actor"]),
    ]
