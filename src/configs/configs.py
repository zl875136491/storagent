from src import APP_ROOT
from typing import Optional
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

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
  APP_VERSION: str = "0.1.0"
  DEBUG: bool = False
  BACKEND_CORS_ORIGINS: list[str] = ["*"]
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
  # 跨节点 locate 单节点 stat 超时（秒）
  OBJECT_LOCATE_TIMEOUT: float = 5.0

  # MongoDB
  MONGO_DB_HOST: str = "localhost"
  MONGO_DB_PORT: int = 27017
  MONGO_DB_USER: str = "user"
  MONGO_DB_PASSWD: str = "passwd"
  MONGO_DB_NAME: str = "storagent"
  MONGO_DB_AUTH_SOURCE: str = "admin"

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
  ETCD_USERNAME: str = "admin"
  ETCD_PASSWORD: str = "passwd"

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
  APPLICATION_QUOTA_USAGE_CACHE_SECONDS: float = 60.0
  APPLICATION_QUOTA_USAGE_MAX_CONCURRENCY: int = 4
  APPLICATION_QUOTA_RESERVATION_TTL_SECONDS: int = 86400
  APPLICATION_QUOTA_MAX_ACTIVE_RESERVATIONS: int = 1000
  APPLICATION_UPLOAD_MAX_PART_BYTES: int = 64 * 1024 ** 2
  APPLICATION_UPLOAD_MAX_IN_MEMORY_PARTS: int = 2
  MINIO_HEAL_TIMEOUT_SECONDS: float = 3600.0
  CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS: float = 120.0
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
    if self.REGION.strip().lower() in ("", "undefined"):
      errors.append("REGION 未设置（不能为 undefined）")

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
