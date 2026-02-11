# Crosstorage
Crosstorage is a compound word which is composed of Cross and storage.

## 项目简介

## 项目依赖
- FastAPI
- MongoDB
- Beanie
- JWT
- Bcrypt

## 项目结构
```shell
# 目录结构
.
├── main.py
├── requirements.txt
├── run_server.sh
├── src
│   ├── __init__.py
│   ├── api
│   ├── configs
│   ├── utils
│   ├── core
│   │   ├── auth.py
│   │   ├── database.py
│   │   └── exception.py
│   └── modules
│       ├── public
│       │   ├── crud.py
│       │   ├── model.py
│       │   ├── route.py
│       │   ├── schema.py
│       │   └── service.py
│       ├── auth
│       │   ├── crud.py
│       │   ├── model.py
│       │   ├── route.py
│       │   ├── schema.py
│       │   └── service.py
│       └── ....
└── tests
│   ├── test_main.py
```

## 安装 / 运行

### 环境准备

- Python3.10.X with SSL support

- MongoDB

### 环境变量
```shell
# JWT 的密钥, 用于加密和解密 JWT 令牌
SECRET_KEY = "XXXXXX"

# MongoDB 的配置, 用于连接 MongoDB 数据库
# MONGO_DB_AUTH_SOURCE 默认为 admin
MONGO_DB_HOST = "10.32.12.110"
MONGO_DB_PORT = 27017
MONGO_DB_USER = "user"
MONGO_DB_PASSWD = "passwd"
MONGO_DB_NAME = "crosstorage"
MONGO_DB_AUTH_SOURCE = "admin"
```

