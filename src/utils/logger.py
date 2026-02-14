import sys
from loguru import logger
from src.configs.configs import settings

def setup_logging():
  """
  配置 Loguru 日志记录器。
  该函数应在应用启动时调用一次。
  """
  # 移除 Loguru 的默认处理器，以便进行完全自定义
  logger.remove()

  # 定义日志格式
  log_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <4}</level> | "
    "<level>{message}</level> | "
    "<cyan>{process.name}</cyan>:<cyan>{thread.name}</cyan> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
  )

  # 通用配置
  common_sink_config = {"format": log_format, "enqueue": True, "backtrace": True, "diagnose": True}

  # 添加控制台输出 sink
  logger.add(sys.stderr, level=settings.LOG_STD_LEVEL, colorize=True, **common_sink_config)

  # 添加文件输出 sink
  log_file_path = settings.LOG_PATH + "/app_{time:YYYY-MM-DD}.log"
  logger.add(
    log_file_path,
    level=settings.LOG_STORAGE_LEVEL,
    rotation="00:00",
    retention=f"{settings.LOG_STORAGE_DAYS} days",
    compression="zip",
    encoding="utf-8",
    catch=True,
    **common_sink_config,
  )
