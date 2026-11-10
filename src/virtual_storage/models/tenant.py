"""兼容导出。模型定义在 documents.py。"""
from .documents import KeyDelivery, Tenant, TenantAPIKey

KeyDeliveryRecord = KeyDelivery
__all__ = ["KeyDelivery", "KeyDeliveryRecord", "Tenant", "TenantAPIKey"]
