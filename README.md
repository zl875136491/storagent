# Storagent
Storagent is a compound word which is composed of storage and agent.

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
创建 .env 文件
```shell
# JWT 的密钥, 用于加密和解密 JWT 令牌
# 需要全局保持一致， 进行全局身份互认
SECRET_KEY = "XXXXXX"
BCRYPT_SALT = "XXXXXXXX"

# 服务器配置， 需要将网络的情况暴露到接口，用于节点间标记对方身份
SERVER_HOST = "10.32.12.0"
SERVER_PORT = 6783

# 唯一ID
# 需要每个地区保证不同
REGION = "beijing"

# 配置 MongoDB， Redis， Minio 的访问参数
# MongoDB, Redis, Minio 都只在本地访问，不暴露到网络
.....
# 用于企业身份认证的接口
USER_INFO_URL = "http://"
# 如果跳过企业认证，测试时可以使用 IGNORE_AUTH = True 自动注册登录的用户
IGNORE_AUTH = False
```

