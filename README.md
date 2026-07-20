# Storagent

Storagent（Storage + Agent）是一个多区域 MinIO 对象存储管理后端 API 服务。它在各 Region 部署 MinIO 集群，并通过统一的 API 提供认证、应用管理、API Key 鉴权上传下载、存储拓扑可视化等能力。

## 核心功能

| 模块 | 能力 |
|------|------|
| 认证 | JWT 登录/登出/刷新、企业 SSO 集成、RBAC 权限 |
| 公共 | Region 管理、Application 创建与 SSE 授权、API Key 管理 |
| 存储 | MinIO 服务器管理、Bucket 列表、复制拓扑查询 |
| 文件 | S3 Multipart 分片上传/下载/断点续传（API Key 鉴权） |
| 拓扑 | Bucket 复制关系图节点/边位置持久化 |

## 技术栈

- **Web**: FastAPI + Uvicorn
- **数据库**: MongoDB + Beanie ODM
- **对象存储**: MinIO SDK + mc CLI
- **协调**: Etcd（跨节点 Region/Server 同步）

## 快速开始

### 1. 环境准备

- Python 3.12+
- MongoDB
- MinIO
- Etcd（多节点部署时需要）
- mc（MinIO Client，项目自带 `runtimes/mc`）

### 2. 安装依赖

```bash
cd storagent
pip install -r requirements.txt
```

### 3. 配置环境变量

```bash
cp .env.example .env
# 编辑 .env，填写 SECRET_KEY、MongoDB、MinIO、Etcd 等配置
```

生成密钥：

```bash
./storagent.sh scripts gen_secret_key
./storagent.sh scripts gen_salt
```

### 4. 启动服务

```bash
# 开发模式（热重载）
RELOAD=true ./storagent.sh run

# 生产模式
RELOAD=false DEBUG=false ./storagent.sh run
```

服务启动后访问：
- API 文档: http://localhost:9000/docs
- 健康检查: http://localhost:9000/health
- 就绪检查: http://localhost:9000/ready

### 5. Docker 部署

```bash
docker build -t storagent .
docker run -d --env-file .env -p 9000:9000 storagent
```

## 环境变量说明

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `SECRET_KEY` | JWT 签名密钥（多节点需一致） | — |
| `BCRYPT_SALT` | 密码哈希盐（多节点需一致） | — |
| `REGION` | 当前节点区域标识（唯一） | `undefined` |
| `REGION_NAME` | 区域显示名称 | `未定义` |
| `SERVER_HOST` | 对外暴露的主机地址 | `localhost` |
| `SERVER_PORT` | API 服务端口 | `9000` |
| `INIT_SERVICE` | 启动时自动注册 Region/MinIO 到 Etcd | `true` |
| `DEBUG` | 调试模式 | `false` |
| `RELOAD` | 热重载（开发用） | `true` |
| `IGNORE_AUTH` | 跳过企业 SSO（测试用） | `false` |
| `MONGO_DB_*` | MongoDB 连接参数 | 见 `.env.example` |
| `MINIO_*` | 本地 MinIO 连接参数 | 见 `.env.example` |
| `ETCD_*` | Etcd 连接参数 | 见 `.env.example` |

完整列表见 [`.env.example`](.env.example)。

## API 概览

所有 API 前缀为 `/api`，认证方式为 Bearer Token 或 `x-api-key` 请求头。

### 认证 `/api/auth`

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/login` | 用户登录 |
| POST | `/refresh` | 刷新 Token |
| GET | `/profile` | 获取用户信息 |
| GET | `/logout` | 登出 |

### 公共 `/api/public`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/endpoints` | 获取各区域端点 |
| POST/GET | `/region` | 创建/列表区域 |
| POST/GET | `/application` | 创建/列表应用 |
| POST | `/application/{id}/approval` | SSE 授权应用 |
| POST/GET/DELETE | `/api-key` | 创建/列表/吊销 API Key |

### 存储 `/api/storage`

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/minio-server` | 创建 MinIO 服务器 |
| PUT | `/minio-server/{id}` | 更新复制权重 |
| GET | `/minio-server` | 服务器列表 |
| GET | `/buckets` | 跨节点 Bucket 汇总 |
| GET | `/buckets/{name}/replicates` | 复制拓扑 |

### 文件 `/api/files`（需 `x-api-key`）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/multipart/init` | 初始化分片上传 |
| POST | `/multipart/part` | 上传分片 |
| POST | `/multipart/complete` | 完成上传 |
| POST | `/multipart/abort` | 中止上传 |
| GET | `/multipart/parts` | 断点续传列表 |
| GET | `/object/stat` | 对象元信息（本节点不存在时返回其他节点指引） |
| GET | `/object/locate` | 主动定位对象所在服务点 |
| GET | `/object/download` | 流式/Range 下载（本节点不存在时返回其他节点指引） |

### 拓扑 `/api/graph`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET/POST | `/bucket-node-position` | 查询/更新节点位置 |
| GET/POST | `/bucket-edge-position` | 查询/更新边位置 |

## 项目结构

```
storagent/
├── main.py                 # FastAPI 入口
├── storagent.sh            # 启动脚本
├── requirements.txt
├── Dockerfile
├── .env.example
└── src/
    ├── api/                # 路由注册
    ├── configs/            # 配置与权限常量
    ├── core/               # DB、Auth、MinIO、Etcd、异常处理
    ├── modules/
    │   ├── auth/           # 用户认证
    │   ├── public/         # Region、Application、API Key
    │   ├── storage/        # MinIO 服务器与 Bucket
    │   ├── files/          # 分片上传下载
    │   ├── graph/          # 拓扑图位置
    │   └── health/         # 健康检查
    └── utils/
```

## 典型工作流

1. **部署节点**: 配置 `.env` 中的 `REGION` 和 MinIO 参数，启动服务（`INIT_SERVICE=true` 自动注册到 Etcd）
2. **创建应用**: 用户登录 → POST `/api/public/application` 创建应用
3. **授权应用**: 管理员 POST `/api/public/application/{id}/approval`（SSE 流式进度，自动在各 MinIO 节点创建 Bucket）
4. **获取 API Key**: POST `/api/public/api-key` 创建密钥
5. **上传文件**: 使用 `x-api-key` 调用 `/api/files/multipart/*` 完成分片上传
6. **跨节点下载**: 若本节点不存在对象，`/object/stat` 或 `/object/download` 返回 `404032`，`data.available_at` 含各可用节点的 `download_url`；也可主动调用 `/object/locate` 查询

## 多节点部署与数据互通

每个 Region 独立部署 Storagent 实例，通过 **Etcd** 实现跨节点数据同步：

| Etcd Key | 同步内容 | 触发时机 |
|----------|---------|---------|
| `region` | 区域名称映射 | 启动 init / 创建 Region / Watch |
| `servers` | MinIO 节点配置 | 启动 init / 创建或更新 Server / Watch |
| `applications` | 应用元数据（含 enabled 状态） | 创建/授权应用 / Watch |
| `api_keys` | API 密钥（含吊销状态） | 创建/吊销 Key / Watch |

### 同步链路

1. **拓扑同步**：各节点启动时将 Region/Server 注册到 Etcd，Watch 自动同步到其他节点 MongoDB，并更新 mc alias
2. **Site Replication**：Watch 发现新 Server 时自动尝试加入 MinIO Site Replication
3. **应用同步**：应用授权（enabled）后发布到 Etcd，远端节点自动建桶、开版本控制、配置 Bucket Replication
4. **API Key 同步**：任一节点创建的 Key 通过 Etcd 广播，所有节点均可鉴权
5. **客户端发现**：`GET /api/public/endpoints` 返回各 Region 的 API 地址和 MinIO 地址

### 部署要求

1. 各节点设置不同的 `REGION` 值
2. 共享同一个 Etcd 集群
3. `SECRET_KEY` 和 `BCRYPT_SALT` 全局一致（JWT 互认）
4. 各节点 MinIO 通过 Site Replication 互联

## License

见 [LICENSE](LICENSE) 文件。
