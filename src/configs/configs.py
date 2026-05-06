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
  DEBUG: bool = True
  BACKEND_CORS_ORIGINS: list[str] = ["*"]
  TIMEZONE: str = "Asia/Shanghai"
  
  # 唯一ID
  REGION: str = "undefined"
  REGION_NAME: str = "未定义"
  
  # 服务器名称
  SERVER_HOST: str = "localhost"
  SERVER_PORT: int = 9000

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

settings = Settings()
settings.create_path()