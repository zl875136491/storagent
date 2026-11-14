"""兼容导出。模型定义在 documents.py。"""
from .documents import VirtualObject

VirtualObjectCatalog = VirtualObject
__all__ = ["VirtualObject", "VirtualObjectCatalog"]
