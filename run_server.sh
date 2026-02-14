#! /bin/bash

# venv
source ../venv/bin/activate

# 启动项目
uvicorn main:app --host 0.0.0.0 --port=6783 --timeout-graceful-shutdown 1 --reload --reload-exclude '*/tests/*'
