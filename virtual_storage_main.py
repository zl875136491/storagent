"""虚拟存储独立入口。旧 main.py 不加载这个应用。"""
from src.virtual_storage.app import create_app

app = create_app()
