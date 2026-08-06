# Storagent

Storagent（Storage + Agent）是一个多区域 MinIO 对象存储管理后端 API 服务。它在各 Region 部署 MinIO 集群，并通过统一的 API 提供认证、应用管理、API Key 鉴权上传下载、存储拓扑可视化等能力。

## 核心功能

| 模块 | 能力 |
|------|------|
| 认证 | JWT 登录/登出/刷新、OA 一次性链接、注册/密码重置、RBAC 权限 |
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
| `ONE_TIME_DOWNLOAD_TTL_SECONDS` | 管理员一次性应急下载链接有效期（30-900 秒） | `300` |
| `INIT_SERVICE` | 启动时自动注册 Region/MinIO 到 Etcd | `true` |
| `DEBUG` | 调试模式 | `false` |
| `RELOAD` | 热重载（开发用） | `true` |
| `IGNORE_AUTH` | 跳过企业 SSO（测试用） | `false` |
| `SPRINGBOARD_URL` | OA 消息网关地址 | `http://tl.cooacloud.com/springboard_v3/` |
| `SPRINGBOARD_APP` | OA 消息应用标识 | `storagent` |
| `FRONT_URL` | OA 认证链接的前端地址 | `http://stor.1oa.com.cn` |
| `OA_AUTH_CODE_EXPIRE_MINUTES` | OA 一次性链接有效期（分钟） | `15` |
| `MONGO_DB_*` | MongoDB 连接参数 | 见 `.env.example` |
| `MINIO_*` | 本地 MinIO 连接参数 | 见 `.env.example` |
| `ETCD_*` | Etcd 连接参数 | 见 `.env.example` |

完整列表见 [`.env.example`](.env.example)。

## API 概览

所有业务 API 统一挂载在版本前缀 `/api/v1` 下，认证方式为 Bearer Token 或
`x-api-key` 请求头。**历史未带版本号的 `/api/*` 路径已完全下线，不再兼容**：
其鉴权模型允许（甚至默认）前端直接持有并发送 `x-api-key`，一旦经浏览器网络
面板泄露即可被冒用发起任意上传/下载，v1 起视为不安全设计，由“能力令牌”机制
完全取代。文档中心的「功能接口引导」同步引入版本切换，只维护 v1 一份文档。

### 控制面 / 数据面与能力令牌（Capability Token）

`x-api-key` 现在只允许出现在 **App 后端 → Storagent** 的服务端请求中（控制面：
`multipart/init`、`multipart/complete`、`multipart/abort`、`multipart/parts`、
`object/stat`、`object/locate`），前端浏览器不得持有或发送它。

前端如需直连 Storagent 完成实际的数据传输（数据面：`multipart/part` 分片上传、
`object/download` 下载），必须改为携带 App 后端签发的**能力令牌**（`token` 查询
参数），二者选其一即可：

1. App 后端使用共享的 `x-api-key` 明文作为 HMAC-SHA256 密钥，在本地对
   `{ref: sha256(x-api-key), act: "upload_part"|"download", key: object_key, exp: 过期时间戳, uid?: upload_id}`
   签名，得到 `Base64Url(Payload).Base64Url(签名)` 形式的 Token，无需请求
   Storagent（类似 S3 预签名 URL）。上传令牌建议 2 小时量级有效期，下载令牌
   建议 5-15 分钟量级有效期。
2. App 后端把 Token（连同 `upload_id`/`object_key` 或最终下载 URL）交给前端，
   前端直接携带 Token 调用 Storagent 的 `multipart/part` 或 `object/download`。
3. Storagent 按 Token 中的 `ref` 反查对应的 APIKey、解密出明文重新计算签名，
   并核对 `act`/`key`（及 `uid`）与请求参数完全一致、未过期才放行；因此前端
   即使截获 Token，也只能在有效期内对指定文件完成指定的单一动作。

详见 [`src/core/capability_token.py`](src/core/capability_token.py) 与文档中心
「功能接口引导」v1。

### 认证 `/api/v1/auth`

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/login` | 用户登录 |
| POST | `/register/request` | 设置密码并向 OA 发送注册确认链接 |
| POST | `/password-reset/request` | 设置新密码并向 OA 发送重置确认链接 |
| POST | `/login-link/request` | 向 OA 发送快捷登录链接 |
| POST | `/login-by-code` | 消费一次性链接并签发 Token |
| POST | `/refresh` | 刷新 Token |
| GET | `/profile` | 获取用户信息 |
| GET | `/logout` | 登出 |

### 公共 `/api/v1/public`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/endpoints` | 获取各区域端点 |
| POST/GET | `/region` | 创建/列表区域 |
| POST/GET | `/application` | 创建/列表应用 |
| POST | `/application/{id}/approval` | SSE 授权应用 |
| POST/GET/DELETE | `/api-key` | 创建/列表/吊销 API Key |

### 存储 `/api/v1/storage`

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/minio-server` | 创建 MinIO 服务器 |
| PUT | `/minio-server/{id}` | 更新复制权重 |
| GET | `/minio-server` | 服务器列表 |
| GET | `/buckets` | 跨节点 Bucket 汇总 |
| GET | `/buckets/{name}/replicates` | 复制拓扑 |
| POST | `/buckets/{name}/replicates` | 创建单向复制连接 |

存储服务统一使用 Bucket Replication 管理单向复制连接。不要同时为受管
MinIO 节点启用 Site Replication；MinIO 不允许两种复制模式混用。

### 文件 `/api/v1/files`（控制面需 `x-api-key`；数据面 `x-api-key` 或能力令牌 `token` 二选一）

| 方法 | 路径 | 面 | 说明 |
|------|------|------|------|
| POST | `/multipart/init` | 控制面 | 初始化分片上传 |
| POST | `/multipart/part` | 数据面 | 上传分片，可用能力令牌代替 `x-api-key` |
| POST | `/multipart/complete` | 控制面 | 完成上传 |
| POST | `/multipart/abort` | 控制面 | 中止上传 |
| GET | `/multipart/parts` | 控制面 | 断点续传列表 |
| POST | `/object/stat` | 控制面 | 对象元信息，`object_key` 放在 JSON 请求体中（本节点不存在时返回其他节点指引） |
| GET | `/object/locate` | 控制面 | 主动定位对象所在服务点 |
| GET | `/object/download` | 数据面 | 流式/Range 下载，可用能力令牌代替 `x-api-key`（本节点不存在时返回其他节点指引） |

### 拓扑 `/api/v1/graph`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET/POST | `/bucket-node-position` | 查询/更新节点位置 |
| GET/POST | `/bucket-edge-position` | 查询/更新边位置 |

### AI 助手 `/api/v1/ai`

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/config` | 获取当前用户可用的助手运行配置 |
| GET/PUT | `/admin/config` | 管理员读取/更新模型提供商配置 |
| POST | `/admin/test` | 管理员测试上游模型连接 |
| POST | `/openai/v1/chat/completions` | 已登录用户的 PageAgent 专用代理 |

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
    │   ├── ai/             # AI 提供商配置与 PageAgent 代理
    │   └── health/         # 健康检查
    └── utils/
```

## 典型工作流

1. **部署节点**: 配置 `.env` 中的 `REGION` 和 MinIO 参数，启动服务（`INIT_SERVICE=true` 自动注册到 Etcd）
2. **创建应用**: 用户登录 → POST `/api/v1/public/application` 创建应用
3. **授权应用**: 管理员 POST `/api/v1/public/application/{id}/approval`（SSE 流式进度，自动在各 MinIO 节点创建 Bucket）
4. **获取 API Key**: POST `/api/v1/public/api-key` 创建密钥
5. **上传文件**: 使用 `x-api-key` 调用 `/api/v1/files/multipart/*` 完成分片上传
6. **跨节点下载**: 若本节点不存在对象，`/object/stat` 或 `/object/download` 返回 `404032`，`data.available_at` 含各可用节点的 `download_url`；也可主动调用 `/object/locate` 查询

## 多节点部署与数据互通

每个 Region 独立部署 Storagent 实例，通过 **Etcd** 实现跨节点数据同步：

| Etcd Key | 同步内容 | 触发时机 |
|----------|---------|---------|
| `roles` | 角色定义（按角色名关联） | 启动合并 / 周期校准 / Watch |
| `users` | 可登录用户、加密密码哈希、认证版本和角色（按用户名关联） | OA 注册/密码重置 / 角色变更 / 周期校准 / Watch |
| `region` | 区域名称映射 | 启动 init / 创建 Region / Watch |
| `servers` | MinIO 节点配置 | 启动 init / 创建或更新 Server / Watch |
| `topology_layout` | Bucket 拓扑节点坐标和连线端点 | 北京首次引导 / 拓扑编辑 / Watch |
| `applications` | 应用元数据（含 enabled 状态） | 创建/授权应用 / Watch |
| `api_keys` | API 密钥（含吊销状态） | 创建/吊销 Key / Watch |
| `revoked_tokens` | JWT 吊销哈希 | 登出 / Watch |
| `ai_config` | AI 提供商配置（API Key 加密） | 管理员更新配置 / Watch |

### 同步链路

1. **身份同步**：角色按名称、用户按用户名合并；密码哈希和认证版本写入 Etcd，密码哈希加密存储，远端使用本地 ObjectId 重建关联。OA 一次性挑战只保存在发起节点，不参与同步
2. **节点清单**：各节点启动时将 Region/Server 注册到 Etcd，Watch 同步到其他节点 MongoDB，并更新 mc alias
3. **布局同步**：`topology_layout` 首次仅由 `SYNC_AUTHORITY_REGION`（默认北京）写入完整快照；之后任一区域的布局编辑均通过 Etcd CAS 合并
4. **数据面隔离**：MinIO Bucket Replication 规则始终从 MinIO 实时读取；Mongo/Etcd 只同步图形布局，不会因布局同步创建或删除复制规则
5. **应用与密钥**：应用、API Key、吊销 token 和 AI 配置通过 Etcd 广播，所有节点均可使用
6. **漏事件修复**：Watch 提供实时同步，后台按 `SYNC_RECONCILE_INTERVAL_SECONDS`（默认 30 秒）执行全量校准

Mongo `_id`、审计事件、API Key 使用统计、Shell 命令日志及本节点 `master`
标记属于本地数据，不要求字节级一致。全局数据使用用户名、角色名、Region 名、
应用名以及 Bucket/Server/Edge 组合键判断一致性。

### 部署要求

1. 各节点设置不同的 `REGION` 值
2. 共享同一个 Etcd 集群
3. `SECRET_KEY` 和 `BCRYPT_SALT` 全局一致（JWT 互认）
4. 所有节点设置相同的 `SYNC_AUTHORITY_REGION`；首次上线需保证该区域 Mongo 拓扑布局正确
5. MinIO 统一使用 Bucket Replication；不要同时启用 Site Replication

故障处理、备份恢复与限流说明见 [docs/RUNBOOK.md](docs/RUNBOOK.md)。

## License

见 [LICENSE](LICENSE) 文件。
