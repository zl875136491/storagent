"""兼容导出。模型定义在 documents.py。"""
from .documents import QuotaLedgerEntry

QuotaLedger = QuotaLedgerEntry
__all__ = ["QuotaLedger", "QuotaLedgerEntry"]
