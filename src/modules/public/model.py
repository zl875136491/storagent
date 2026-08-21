from beanie import Document
from beanie.odm.fields import Link
from datetime import datetime
from pymongo import IndexModel
from pydantic import Field, model_validator
from src.utils.helpers import utc_now
from src.modules.auth.model import User
from typing import List, Literal
from pydantic import BaseModel


DEFAULT_APPLICATION_QUOTA_BYTES = 100 * 1024 ** 3

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
  quota_bytes: int = Field(default=DEFAULT_APPLICATION_QUOTA_BYTES, gt=0)
  # Usage is a node-local cache. The quota itself is synchronized through Etcd.
  quota_usage_bytes: int = Field(default=0, ge=0)
  quota_usage_updated_at: datetime | None = Field(default=None)
  # Browser Origins allowed to call the data plane directly from this app.
  domains: List[str] = Field(default_factory=list)
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


class APIUsageEvent(Document):
  """一次成功上传或下载请求产生的持久化用量事件。"""
  api_key_id: str
  api_key_hint: str = Field(default="")
  app_name: str
  app_shown_name: str = Field(default="")
  operation: Literal["upload", "download"]
  bytes_transferred: int = Field(default=0, ge=0)
  region: str
  occurred_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "api_usage_event"
    indexes = [
      IndexModel([("occurred_at", -1)]),
      IndexModel([("api_key_id", 1), ("occurred_at", -1)]),
      IndexModel([("app_name", 1), ("occurred_at", -1)]),
      IndexModel([("region", 1), ("occurred_at", -1)]),
    ]

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
      IndexModel(["region"]),
      IndexModel([("action", 1), ("created_at", -1)]),
      IndexModel([("actor", 1), ("created_at", -1)]),
      IndexModel([("region", 1), ("created_at", -1)]),
    ]


class QuotaAlertRule(Document):
  """Single system-wide quota alert policy managed by application admins."""
  low_percent: int = Field(default=70, ge=1, le=100)
  medium_percent: int = Field(default=85, ge=1, le=100)
  high_percent: int = Field(default=90, ge=1, le=100)
  block_percent: int = Field(default=100, ge=1, le=100)
  message_template: str = Field(
    default="应用 {app_name} 当前配额使用率为 {usage_percent}%，请关注容量并按需提交扩容申请。",
    min_length=1,
    max_length=1000,
  )
  updated_at: datetime = Field(default_factory=utc_now)
  updated_by: str = Field(default="")

  class Settings:
    name = "quota_alert_rule"


class QuotaAlertEvent(Document):
  """Deduplicated application quota alert delivery record."""
  application_name: str
  owner_username: str
  level: Literal["low", "medium", "high", "blocked"]
  usage_bytes: int = Field(ge=0)
  projected_usage_bytes: int = Field(ge=0)
  quota_bytes: int = Field(gt=0)
  usage_percent: float = Field(ge=0)
  message: str
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "quota_alert_event"
    indexes = [
      IndexModel([("application_name", 1), ("level", 1), ("created_at", -1)]),
      IndexModel([("created_at", -1)]),
    ]


class ApplicationExpansionRequest(Document):
  """An application owner requests a quota increase from application admins."""
  application_name: str
  application_shown_name: str
  applicant_username: str
  reason: str = Field(min_length=1, max_length=2000)
  add_size_bytes: int = Field(gt=0)
  status: Literal["pending", "approved", "rejected"] = "pending"
  reviewer_username: str = Field(default="")
  review_note: str = Field(default="", max_length=1000)
  created_at: datetime = Field(default_factory=utc_now)
  reviewed_at: datetime | None = None

  class Settings:
    name = "application_expansion_request"
    indexes = [
      IndexModel([("application_name", 1), ("status", 1), ("created_at", -1)]),
      IndexModel([("applicant_username", 1), ("created_at", -1)]),
    ]


class DiagnosticRun(Document):
  """A self-diagnosis execution submitted by a caller-owned backend host."""
  run_id: str
  api_version: Literal["v1", "v2"]
  app_name: str
  source_host: str = Field(default="")
  network_only: bool = False
  overall_status: Literal["passed", "failed", "partial"]
  checks: list[dict] = Field(default_factory=list)
  raw_log: str = Field(default="", max_length=200_000)
  created_at: datetime = Field(default_factory=utc_now)

  class Settings:
    name = "diagnostic_run"
    indexes = [
      IndexModel([("app_name", 1), ("created_at", -1)]),
      IndexModel([("created_at", -1)]),
      IndexModel([("run_id", 1)], unique=True),
    ]
