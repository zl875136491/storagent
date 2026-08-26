from src import APP_ROOT
import re
from typing import Optional
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


# The Etcd operations view checks the complete control-plane cluster by
# default. Deployments can still replace this with ETCD_ENDPOINTS when their
# topology differs, but an explicitly blank environment variable must not
# silently reduce the view to one local endpoint.
DEFAULT_ETCD_ENDPOINTS = (
  "http://10.41.102.223:2379",
  "http://10.32.129.241:2379",
  "http://10.17.158.115:2379",
  "http://10.8.136.107:2379",
  "http://10.31.133.207:2379",
)
_MINIO_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$")

def create_file_path(file_path: str) -> bool:
  """
  创建路径
  """
  from os.path import exists as os_exists
  from os import makedirs as os_makedirs
  if not os_exists(file_path):
    os_makedirs(file_path, exist_ok=True)
  return True

# 加载环境变量
load_dotenv(APP_ROOT + "/.env")

class Settings(BaseSettings):
  """
  应用配置类
  
  将所有配置项定义在此类中, 并使用 pydantic_settings 库进行加载
  
  请引入该库下的 settings 实例, 该类无需重复实例化
  """
  # Info
  APP_NAME: str = "Storagent"
  # 业务接口版本随 API_V1_PREFIX（/api/v1）走：v1 完全取代此前未带版本号的接口。
  APP_VERSION: str = "1.0.0"
  DEBUG: bool = False
  BACKEND_CORS_ORIGINS: list[str] = ["*"]
  # 预检（OPTIONS）响应缓存时长（秒）。浏览器会在这段时间内对相同
  # origin+method+header 组合复用已缓存的预检结果，不再重复发起 OPTIONS
  # 请求。各浏览器自身也有上限（Chromium 最长 2 小时、Firefox 最长 24
  # 小时），这里设置的值会被浏览器自动截断到其上限，无需按浏览器区分。
  BACKEND_CORS_MAX_AGE_SECONDS: int = 86400
  TIMEZONE: str = "Asia/Shanghai"
  INIT_SERVICE: bool = True
  RELOAD: bool = False
  # 生产默认关闭 OpenAPI；DEBUG=true 时仍开启
  ENABLE_DOCS: bool = False
  
  # 唯一ID
  REGION: str = "undefined"
  REGION_NAME: str = "未定义"
  
  # 服务器名称
  SERVER_HOST: str = "localhost"
  SERVER_PORT: int = 9000
  # 对外暴露的 API URL 协议（endpoints / locate 指引）
  PUBLIC_SCHEME: str = "http"
  # 对外 Nginx 网关域名，不带协议、端口或路径，例如 stor.1oa.com.cn。
  # 为空时，历史 MinIO 服务仍回退到 host:server_port。
  PUBLIC_DOMAIN: str = ""
  # Self-diagnosis scripts may be downloaded from one deployment and run on a
  # different host. Keep their suggested gateway explicit per environment.
  DIAGNOSTIC_SCRIPT_DEFAULT_BASE: str = "http://stor.1oa.com.cn/server/local"
  DIAGNOSTIC_SCRIPT_GATEWAY_ORIGIN: str = "http://stor.1oa.com.cn"
  # 跨节点 locate 单节点 stat 超时（秒）
  OBJECT_LOCATE_TIMEOUT: float = 5.0

  # MongoDB
  MONGO_DB_HOST: str = "localhost"
  MONGO_DB_PORT: int = 27017
  MONGO_DB_USER: str = "user"
  MONGO_DB_PASSWD: str = "passwd"
  MONGO_DB_NAME: str = "storagent"
  MONGO_DB_AUTH_SOURCE: str = "admin"

  # Celery uses MongoDB for both broker transport and result metadata.
  CELERY_ENABLED: bool = False
  CELERY_BROKER_URL: str = ""
  CELERY_RESULT_BACKEND: str = ""
  CELERY_MONGODB_DATABASE: str = "storagent_celery"
  CELERY_MONGODB_MESSAGES_COLLECTION: str = "celery.messages"
  CELERY_MONGODB_ROUTING_COLLECTION: str = "celery.routing"
  CELERY_MONGODB_QUEUES_COLLECTION: str = "celery.queues"
  CELERY_MONGODB_RESULT_COLLECTION: str = "celery_taskmeta"
  # A producer only publishes to the queue belonging to its own Region and
  # protocol.  A protocol bump intentionally creates a new queue so an old
  # worker cannot consume an incompatible maintenance task during rollout.
  CELERY_TASK_QUEUE_PREFIX: str = "storagent"
  CELERY_TASK_PROTOCOL_VERSION: int = 2
  CELERY_BEAT_LOCK_COLLECTION: str = "celery_beat_locks"
  CELERY_BEAT_LOCK_TTL_SECONDS: int = 45
  CELERY_BEAT_FOLLOWER_POLL_SECONDS: float = 5.0
  CELERY_OPERATION_START_TIMEOUT_SECONDS: int = 180
  CELERY_OPERATION_RUNNING_TIMEOUT_SECONDS: int = 7200
  CELERY_OPERATION_WATCHDOG_INTERVAL_SECONDS: float = 60.0
  # Worker-side observability records are written alongside Celery results.
  # Keeping the database explicit supports deployments whose result backend is
  # separated from the broker database.
  CELERY_MONGODB_RESULT_DATABASE: str = ""
  CELERY_TASK_HISTORY_COLLECTION: str = "celery_task_history"
  CELERY_WORKER_HEARTBEAT_COLLECTION: str = "celery_worker_heartbeats"
  CELERY_TASK_HISTORY_RETENTION_DAYS: int = 30
  CELERY_WORKER_HEARTBEAT_RETENTION_DAYS: int = 7
  CELERY_RUNTIME_TIMEOUT_SECONDS: float = 1.5
  CELERY_WORKER_STALE_AFTER_SECONDS: int = 90
  CELERY_OVERVIEW_CACHE_SECONDS: float = 8.0
  CELERY_OVERVIEW_RATE_LIMIT_PER_MINUTE: int = 20
  CELERY_HISTORY_RATE_LIMIT_PER_MINUTE: int = 40
  AUTH_CLEANUP_INTERVAL_SECONDS: int = 3600

  # Minio
  MINIO_HOST: str = "localhost"
  MINIO_PORT: int = 9000
  MINIO_ACCESS_KEY: str = "admin"
  MINIO_SECRET_KEY: str = "passwd"
  MINIO_REPLICATE_WEIGHT: int = 0
  
  # # Redis 
  # REDIS_HOST: str = "localhost"
  # REDIS_PORT: int = 6379
  # REDIS_DB: int = 0
  # REDIS_PASSWORD: str = "passwd"
  # # 频道命名基本不会变
  # REDIS_API_KEY_CHANNEL: str = "api_key_patch"
  # REDIS_FILE_CHANNEL: str = "file_patch"
  
  # Etcd
  ETCD_HOST: str = "localhost"
  ETCD_PORT: int = 2379
  # Read-only operations checks the complete five-member control-plane cluster.
  # Deployments may override this with a comma-separated endpoint list; the
  # default keeps production and fresh installs from silently showing 1 / 1.
  ETCD_ENDPOINTS: str = ",".join(DEFAULT_ETCD_ENDPOINTS)
  ETCD_USERNAME: str = "admin"
  ETCD_PASSWORD: str = "passwd"
  ETCD_HEALTH_TIMEOUT_SECONDS: float = 3.0
  ETCD_HEALTH_CACHE_TTL_SECONDS: float = 30.0
  ETCD_RAFT_LAG_WARNING: int = 100
  ETCD_RAFT_LAG_CRITICAL: int = 1000
  ETCD_SNAPSHOT_DIR: str = "/tmp/storagent-etcd-snapshots"
  ETCD_SNAPSHOT_MAX_BYTES: int = 1024 ** 3

  # Control-plane synchronization
  # The authority is used only for the first topology-layout snapshot. After
  # initialization, every region may update the shared layout through Etcd CAS.
  SYNC_AUTHORITY_REGION: str = "beijing"
  SYNC_RECONCILE_INTERVAL_SECONDS: float = 30.0
  REPLICATION_RECONCILE_INTERVAL_SECONDS: float = 300.0
  REPLICATION_LOCK_TTL_SECONDS: int = 120
  REPLICATION_LOCK_TIMEOUT_SECONDS: int = 10

  # Storage operations and cluster self-healing
  SERVER_DETAILS_CACHE_TTL_SECONDS: int = 600
  # 管理员应急下载链接；运行时还会限制在 30-900 秒内。
  ONE_TIME_DOWNLOAD_TTL_SECONDS: int = 300
  MINIO_OPERATION_TIMEOUT_SECONDS: float = 20.0
  # Soft-deleted objects remain recoverable until restore_until. Once that
  # deadline is reached they are copied to this internal bucket, then removed
  # from the application bucket by the local archive worker.
  # The default is deliberately off: enabling a source-version deletion path
  # requires an explicit post-rollout archive validation in each environment.
  OBJECT_ARCHIVE_ENABLED: bool = False
  # Existing archive bucket policy is never changed implicitly. This explicit
  # bootstrap switch is intended for a freshly provisioned test environment.
  OBJECT_ARCHIVE_AUTOCONFIGURE: bool = False
  OBJECT_RECOVERY_PERIOD_DAYS: int = 30
  OBJECT_ARCHIVE_BUCKET: str = "storagent-expired-archive"
  # Archive objects are retained independently from the application recovery
  # window. The archive bucket lifecycle removes both current and non-current
  # versions after this period.
  OBJECT_ARCHIVE_RETENTION_DAYS: int = 365
  OBJECT_ARCHIVE_INTERVAL_SECONDS: float = 300.0
  OBJECT_ARCHIVE_BATCH_SIZE: int = 50
  OBJECT_ARCHIVE_RETRY_SECONDS: float = 300.0
  OBJECT_ARCHIVE_POLICY_CHECK_SECONDS: int = 300
  APPLICATION_QUOTA_USAGE_CACHE_SECONDS: float = 60.0
  APPLICATION_QUOTA_USAGE_MAX_CONCURRENCY: int = 4
  # The Celery authority periodically seeds/reconciles the Etcd logical quota
  # aggregate for applications created before event-based quota accounting.
  APPLICATION_QUOTA_AGGREGATE_INTERVAL_SECONDS: int = 3600
  APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE: int = 50
  APPLICATION_QUOTA_RESERVATION_TTL_SECONDS: int = 86400
  APPLICATION_QUOTA_MAX_ACTIVE_RESERVATIONS: int = 1000
  APPLICATION_UPLOAD_MAX_PART_BYTES: int = 64 * 1024 ** 2
  APPLICATION_UPLOAD_MAX_IN_MEMORY_PARTS: int = 2
  # Quota warnings are evaluated at upload admission; duplicate OA messages
  # for an unchanged threshold are suppressed for this interval.
  QUOTA_ALERT_COOLDOWN_SECONDS: int = 86400
  CAPACITY_SNAPSHOT_INTERVAL_SECONDS: int = 3600
  CAPACITY_SNAPSHOT_MAX_CONCURRENCY: int = 3
  # Caller diagnostics must consume persisted aggregates, never trigger a
  # foreground MinIO scan. A stale aggregate is reported as not ready.
  CAPACITY_SNAPSHOT_MAX_AGE_SECONDS: int = 10800
  DIAGNOSTIC_QUOTA_AGGREGATE_MAX_AGE_SECONDS: int = 7200
  MINIO_HEAL_TIMEOUT_SECONDS: float = 3600.0
  CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS: float = 120.0
  # Infrastructure thresholds are independent from application quota rules.
  MINIO_DRIVE_WARNING_PERCENT: float = 85.0
  MINIO_DRIVE_CRITICAL_PERCENT: float = 95.0
  MINIO_DRIVE_INODE_WARNING_PERCENT: float = 85.0
  MINIO_DRIVE_INODE_CRITICAL_PERCENT: float = 95.0
  MINIO_DRIVE_CAPACITY_SKEW_PERCENT: float = 20.0
  AUTO_HEAL_ENABLED: bool = True
  AUTO_HEAL_COOLDOWN_SECONDS: float = 21600.0

  LOG_PATH: str = "logs/"
  LOG_STD_LEVEL: str = "INFO"
  LOG_STORAGE_LEVEL: str = "INFO"
  LOG_STORAGE_DAYS: int = 30
  
  # JWT 认证配置
  SECRET_KEY: str = "XXXXXXX"
  BCRYPT_SALT: str = "XXXXXXXX"
  ALGORITHM: str = "HS256"
  ACCESS_TOKEN_EXPIRE_MINUTES: int = 480 # 8 hours
  REFRESH_TOKEN_EXPIRE_DAYS: int = 30  # 30 days
  
  # 从已有系统中获取注册用户
  IGNORE_AUTH: bool = False
  USER_INFO_URL: str = "http://1.1.1.1:3000/api/v1/"

  # OA IM 身份验证
  SPRINGBOARD_URL: str = "http://tl.cooacloud.com/springboard_v3/"
  SPRINGBOARD_APP: str = "storagent"
  FRONT_URL: str = "http://stor.1oa.com.cn"
  OA_AUTH_CODE_EXPIRE_MINUTES: int = 15
  OA_AUTH_SEND_RETRIES: int = 3

  @property
  def mongo_db_url(self) -> str:
    """
    MongoDB 数据库连接 URL
    """
    return f"mongodb://{self.MONGO_DB_USER}:{self.MONGO_DB_PASSWD}@{self.MONGO_DB_HOST}:{self.MONGO_DB_PORT}/{self.MONGO_DB_NAME}?authSource={self.MONGO_DB_AUTH_SOURCE}"

  model_config = SettingsConfigDict(
    env_file=APP_ROOT + "/.env",
    env_file_encoding="utf-8",
    extra="ignore"
  )
  
  def create_path(self):
    """
    创建一些预设的文件系统路径
    """
    create_file_path(self.LOG_PATH)

  def validate_runtime(self) -> None:
    """
    启动前校验：拒绝明显不安全的生产配置。
    pytest 导入时跳过，避免本地 .env 未改完即打断单测。
    """
    import sys
    if "pytest" in sys.modules:
      return

    errors: list[str] = []
    if self.OBJECT_RECOVERY_PERIOD_DAYS < 1:
      errors.append("OBJECT_RECOVERY_PERIOD_DAYS 必须大于 0")
    if self.OBJECT_ARCHIVE_ENABLED:
      if not self.OBJECT_ARCHIVE_BUCKET.strip():
        errors.append("OBJECT_ARCHIVE_BUCKET 不能为空")
      archive_bucket = self.OBJECT_ARCHIVE_BUCKET.strip()
      if archive_bucket != archive_bucket.lower():
        errors.append("OBJECT_ARCHIVE_BUCKET 必须使用小写的 MinIO 存储桶名称")
      if not _MINIO_BUCKET_RE.fullmatch(archive_bucket) or ".." in archive_bucket:
        errors.append("OBJECT_ARCHIVE_BUCKET 必须是合法的 MinIO 存储桶名称")
      if self.OBJECT_ARCHIVE_RETENTION_DAYS < self.OBJECT_RECOVERY_PERIOD_DAYS:
        errors.append("OBJECT_ARCHIVE_RETENTION_DAYS 不能小于 OBJECT_RECOVERY_PERIOD_DAYS")
      if self.OBJECT_ARCHIVE_BATCH_SIZE < 1 or self.OBJECT_ARCHIVE_BATCH_SIZE > 1000:
        errors.append("OBJECT_ARCHIVE_BATCH_SIZE 必须在 1 到 1000 之间")
    if self.REGION.strip().lower() in ("", "undefined"):
      errors.append("REGION 未设置（不能为 undefined）")
    try:
      from src.core.celery_routing import (
        normalize_protocol_version,
        normalize_queue_prefix,
        normalize_region,
      )

      normalize_region(self.REGION)
      normalize_queue_prefix(self.CELERY_TASK_QUEUE_PREFIX)
      normalize_protocol_version(self.CELERY_TASK_PROTOCOL_VERSION)
    except ValueError as error:
      errors.append(f"Celery 区域路由配置无效: {error}")
    if self.CELERY_BEAT_LOCK_TTL_SECONDS < 15:
      errors.append("CELERY_BEAT_LOCK_TTL_SECONDS 不能小于 15 秒")
    if self.CELERY_OPERATION_START_TIMEOUT_SECONDS < 30:
      errors.append("CELERY_OPERATION_START_TIMEOUT_SECONDS 不能小于 30 秒")
    if self.CELERY_OPERATION_RUNNING_TIMEOUT_SECONDS < self.CELERY_OPERATION_START_TIMEOUT_SECONDS:
      errors.append("CELERY_OPERATION_RUNNING_TIMEOUT_SECONDS 不能小于启动超时")
    if self.CELERY_OPERATION_WATCHDOG_INTERVAL_SECONDS < 15:
      errors.append("CELERY_OPERATION_WATCHDOG_INTERVAL_SECONDS 不能小于 15 秒")
    if self.CELERY_TASK_HISTORY_RETENTION_DAYS < 1:
      errors.append("CELERY_TASK_HISTORY_RETENTION_DAYS 必须大于 0")
    if self.CELERY_WORKER_HEARTBEAT_RETENTION_DAYS < 1:
      errors.append("CELERY_WORKER_HEARTBEAT_RETENTION_DAYS 必须大于 0")
    if self.APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE < 1 or self.APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE > 1000:
      errors.append("APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE 必须在 1 到 1000 之间")
    if self.CAPACITY_SNAPSHOT_MAX_CONCURRENCY < 1 or self.CAPACITY_SNAPSHOT_MAX_CONCURRENCY > 20:
      errors.append("CAPACITY_SNAPSHOT_MAX_CONCURRENCY 必须在 1 到 20 之间")

    weak_secrets = {
      "",
      "XXXXXXX",
      "your-secret-key-here",
      "secret",
      "changeme",
    }
    if not self.DEBUG:
      if self.SECRET_KEY.strip() in weak_secrets or len(self.SECRET_KEY.strip()) < 16:
        errors.append("SECRET_KEY 过弱或为占位值（生产环境至少 16 字符）")
      origins = [str(o).strip() for o in self.BACKEND_CORS_ORIGINS]
      if not origins or origins == ["*"]:
        errors.append("生产环境禁止 BACKEND_CORS_ORIGINS=[\"*\"]，请配置明确前端域名")

    if errors:
      raise RuntimeError("启动配置校验失败:\n- " + "\n- ".join(errors))

settings = Settings()
settings.create_path()
