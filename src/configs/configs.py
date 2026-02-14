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
  APP_NAME: str = "Cross Storage"
  APP_VERSION: str = "0.1.0"
  DEBUG: bool = True
  BACKEND_CORS_ORIGINS: list[str] = ["*"]
  TIMEZONE: str = "Asia/Shanghai"
  
  # MongoDB
  MONGO_DB_HOST: str = "localhost"
  MONGO_DB_PORT: int = 27017
  MONGO_DB_USER: str = "user"
  MONGO_DB_PASSWD: str = "passwd"
  MONGO_DB_NAME: str = "manufacture-mgmt"
  MONGO_DB_AUTH_SOURCE: str = "admin"

  LOG_PATH: str = "/var/log/cross_storage"
  LOG_STD_LEVEL: str = "INFO"
  LOG_STORAGE_LEVEL: str = "INFO"
  LOG_STORAGE_DAYS: int = 30
  
  # JWT 认证配置
  SECRET_KEY: str = "XXXXXXX"
  # 密码哈希轮数, 用于 bcrypt 加密, 数值应该介于 4 到 31 之间
  BCRYPT_ROUNDS: int = 4
  ALGORITHM: str = "HS256"
  ACCESS_TOKEN_EXPIRE_MINUTES: int = 15  # 15 minutes
  REFRESH_TOKEN_EXPIRE_DAYS: int = 30  # 30 days

  @property
  def mongo_db_url(self) -> str:
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