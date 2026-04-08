"""
文件模块 MongoDB 文档占位。

当前分片上传会话由 MinIO 管理（upload_id），无需在本地落库。
若需将对象与业务实体（用户、应用等）关联，可在此定义 Beanie Document 并在 database.init_db 中注册。
"""
