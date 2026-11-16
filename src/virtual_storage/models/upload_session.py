"""兼容导出。模型定义在 documents.py。"""
from .documents import UploadSession

class UploadPart:  # 分片确认存放在 UploadSession.confirmed_parts，不再单独建集合。
  pass

__all__ = ["UploadPart", "UploadSession"]
