from typing import Optional
from beanie import init_beanie
from src.utils.logger import logger
from src.configs.configs import settings
from motor.motor_asyncio import AsyncIOMotorClient

# 全局变量，保存数据库客户端引用，用于测试时清理连接
_motor_client: Optional[AsyncIOMotorClient] = None

# Models from each module
from ..modules.auth.model import (
  User,
  Role,
  TempCode,
  DestoryedToken,
)

from ..modules.public.model import (
  Region,
  APIKey,
  Application,
  SystemConfig,
  ShellCommandLog,
)

from ..modules.storage.model import (
  MinioEvent,
  MinioServer,
  MinioBucket,
)

document_models = [
  Region,
  APIKey,
  Application,
  SystemConfig,
  User,
  Role,
  TempCode,
  DestoryedToken,
  MinioEvent,
  MinioServer,
  MinioBucket,
]


async def init_db():
  """
  初始化 MongoDB 数据库连接和 Beanie ODM。
  """
  global _motor_client

  # 1. 创建 Motor 客户端
  _motor_client = AsyncIOMotorClient(
    settings.mongo_db_url,
    serverSelectionTimeoutMS=5000 # 设置超时，防止无限等待
  )
  logger.info("[Production Mode] MongoDB 客户端创建完成。")
  
  # 2. 选择数据库
  database = _motor_client[settings.MONGO_DB_NAME]
  
  # 3. 初始化 Beanie
  # document_models 参数需要传入所有 Beanie Document 类的列表
  document_models = [
    Region,
    APIKey,
    Application,
    SystemConfig,
    User,
    Role,
    TempCode,
    DestoryedToken,
    ShellCommandLog,
    MinioEvent,
    MinioServer,
    MinioBucket,
  ]
  await init_beanie(
    database=database,
    document_models=document_models
  )
  logger.info(f"MongoDB '{settings.MONGO_DB_NAME}' 连接和 Beanie 初始化完成。")

async def close_db():
  """
  关闭 MongoDB 数据库连接。
  主要用于测试时清理资源。
  """
  global _motor_client
  if _motor_client:
    _motor_client.close()
    _motor_client = None
    logger.info("MongoDB 连接已关闭。")

def get_motor_client() -> Optional[AsyncIOMotorClient]:
  """
  获取当前的 Motor 客户端实例。
  主要用于测试时访问客户端进行清理。
  """
  return _motor_client