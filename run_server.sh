#! /bin/bash

# venv
# source ../venv/bin/activate

# # 获取 .env 中的 SERVER_PORT
# SERVER_PORT=$(grep 'SERVER_PORT' .env | awk -F '[ =]+' '{print $2}')
# echo "SERVER_PORT: $SERVER_PORT"

# 启动项目
uvicorn main:app --host 0.0.0.0 --port=$SERVER_PORT --timeout-graceful-shutdown 1 --reload --reload-exclude '*/tests/*'
