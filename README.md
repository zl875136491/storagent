# Storagent Backend

Storagent Backend 是多区域对象存储管理系统的区域控制 API。它以 FastAPI 提供认证、应用与 API Key 管理、文件控制面、MinIO 运维、跨区域控制状态、调用方诊断与只读 Celery 运维接口；对象字节仍由各区域的 MinIO 保存和复制。

## 架构展示

<video controls muted loop playsinline preload="metadata" width="100%">
  <source src="docs/assets/storagent-diagram.webm" type="video/webm">
  当前阅读器不支持内嵌 WebM 播放。
</video>

[下载架构演示视频](docs/assets/storagent-diagram.webm)

## 运行时边界

Storagent 的数据面与控制面相互协作，但不是同一种存储：

| 组件 | 职责 |
| --- | --- |
| Nginx 网关 | 公网 HTTPS 入口与 `/server/{region}` 区域路由；浏览器不直接暴露 MinIO 内网地址。 |
| FastAPI | 认证、授权、业务编排、版本化 API、异步运维任务受理与状态查询。 |
| MinIO | 对象数据面：对象读写、Bucket Replication、对象补传与原生集群自愈。 |
| MongoDB | 本区域业务数据、管理侧查询数据、运维任务、审计与 Celery 结果/观测记录。 |
| Etcd | 跨区域控制与共享状态：区域/服务拓扑、应用、API Key、身份相关同步、逻辑配额聚合、分布式锁与监听收敛。它不复制 MinIO 对象字节。 |
| 独立 Celery Worker | 后台归档、配额聚合、容量快照、Etcd 校准、存储运维操作与审计持久化；与 API 使用同一 Region、队列前缀和协议。 |

Etcd 负责的是选定的跨区域控制状态和协调，不是“所有元数据的唯一数据库”。MongoDB 仍承担本地区域的业务与运维持久化，MinIO 的复制规则和对象状态始终以 MinIO 原生能力为准。

## 当前功能

| 模块 | 能力 |
| --- | --- |
| `auth` | JWT 登录、刷新、登出、OA/SSO 一次性认证链路、用户和角色权限控制。 |
| `public` | Region、应用、API Key、端点发现、应用授权与跨区域控制数据同步。 |
| `storage` | MinIO 服务、Bucket、复制拓扑、管理员一次性下载、集群健康与原生自愈巡检。 |
| `files` | Multipart 上传/完成/中止/续传、对象 stat/定位/下载、应用配额、恢复期与归档。 |
| `graph` | Bucket 复制拓扑的节点、边和布局持久化。 |
| `usage` | 区域与应用用量查询。 |
| `capacity` | 权威区域容量快照、趋势与容量规划数据。 |
| `diagnostics` | 可下载的调用方自诊断脚本、认证/配额/容量/临时对象读写探针与诊断记录。 |
| `etcd` | Etcd 端点状态、趋势、维护操作、任务和事件视图。 |
| `celery` | 只读 Worker、队列、Broker、Beat 租约、实时任务和脱敏历史查询。 |
| `audit` | 操作审计与异步、幂等持久化。 |
| `ai`、`demo` | 已有的助手配置/代理与控制台演示接口。 |

## 本工作区已实现的运维能力

### 存储运维

- **未纳管存储桶处置**：以“未被 Storagent 应用纳管”描述替代不精确的“孤儿桶”。支持盘点、登记保留、解除保留；清理前会进行全站空桶复核，清理作为异步运维任务执行。
- **复制校准与 resync**：可校准 Bucket Replication 规则，并向 MinIO 提交对象补传。接口返回 operation ID，最终结果必须轮询任务状态，不应以 HTTP 受理结果判断完成。
- **MinIO 集群健康**：统一显示磁盘可写能力、容量/INode 风险、复制健康与原生 healing 状态；自动巡检仅由权威区域执行。
- **运维任务保护**：复制、删除、Etcd 维护等任务先落库再投递；Worker 原子领取任务。超时任务会进入 `failed` 并标记 `recovery_required`，不会自动重放可能已产生外部副作用的操作。

### 恢复期到期归档

恢复期到期对象的归档默认关闭。启用后，Worker 只处理 `source_region == REGION` 的对象，并依次执行：

1. 认领可归档记录；
2. 校验权威区域发布的归档策略指纹；
3. 校验归档桶版本控制与受控生命周期规则；
4. 复制并校验归档副本；
5. 仅删除被目录记录指向的源版本；
6. 写回归档结果或可重试失败状态。

归档启用前必须在测试桶完成复制、校验、恢复、版本删除和生命周期演练。生产环境不要设置 `OBJECT_ARCHIVE_AUTOCONFIGURE=true`。

### 调用方自诊断与容量预检

自诊断脚本由 `GET /api/v2/diagnostics/v2/self-diagnosis` 提供。调用方只输入 Storagent 基础地址与 API Key，**不需要 APPID，也不保留“预期 APPID”字段**。

完整模式包含五个阶段：

1. `dns`：域名解析检查；IP 基址会明确跳过；
2. `gateway`：网关健康检查；
3. `authentication`：API Key 与 v2 契约探针；
4. `quota_capacity`：应用配额与区域容量预检；
5. `storage`：临时对象上传、读取和清理。

`quota_capacity` 只读取应用逻辑配额聚合与已发布的容量样本，不会在调用方请求路径直接扫描 MinIO。样本陈旧或冗余不足会返回降级/提示；只有明确的配额耗尽、物理容量耗尽或新鲜的 critical/unreachable 状态才阻断完整诊断。

### Celery 区域隔离与观测

Celery Worker 位于相邻的独立仓库，但 Backend、Worker 与 Frontend 共同构成一个兼容面：

```text
队列: <CELERY_TASK_QUEUE_PREFIX>.<REGION lowercase>.v<CELERY_TASK_PROTOCOL_VERSION>
示例: storagent.beijing.v2
```

- API Producer 只向本区域协议队列投递，并写入来源 Region、协议 header；
- Worker 只订阅自己的区域队列，缺失或不匹配 header 的任务会失败；
- 多个同区域 Worker 可启用 Beat，但只有 MongoDB 租约 `storagent-beat:<region>:v<protocol>` 的持有者投递周期任务；
- 权威任务（复制规则校准、配额聚合、容量快照、自动自愈）还会检查 `SYNC_AUTHORITY_REGION`；
- `celery_task_history` 与 `celery_worker_heartbeats` 由 TTL 管理，历史输出脱敏；管理接口只读。过期的 `STARTED`/`RETRY` 由看门狗收口，不会仅因 Worker 主机名仍在线而显示为执行中。

完整的任务触发、执行角色、失败语义和发布顺序见 [Celery 后台任务说明](docs/CELERY_OPERATIONS.md)。

## API 与兼容性

所有业务接口均使用版本化前缀：`/api/v1` 或 `/api/v2`。历史未带版本号的 `/api/*` 接口不再兼容。

| 接口范围 | 说明 |
| --- | --- |
| `/auth`、`/public` | 登录态、区域、应用、API Key 与端点发现。 |
| `/storage` | MinIO 服务、Bucket、复制关系、异步运维任务、未纳管桶处置与集群健康。 |
| `/files` | 应用侧文件控制面与数据面能力令牌校验。 |
| `/diagnostics` | 自诊断脚本、探针、报告和运维端诊断历史。 |
| `/capacity` | 容量快照与规划。 |
| `/celery` | Worker、队列、Beat、任务目录和历史的只读查询。 |
| `/audit`、`/usage`、`/graph`、`/etcd` | 审计、用量、拓扑与 Etcd 运维。 |

### v2 调用方注意事项

- MinIO 认证、网络和一般操作失败对外维持稳定错误码 `storage.unavailable`；应结合 `retryable` 和 `details.category` 判断重试或鉴权处理，不能解析中文错误消息。
- `reconcile` 和 `resync` 的请求可被成功受理，但最终状态必须读取 operation ID 对应任务。
- 新页面使用 `unmanaged-buckets`；`orphan-buckets` 仅保留兼容接口和旧字段语义。
- 调用方应按诊断阶段 key 处理结果，不能依赖固定阶段数量。

## 三仓库关系

Storagent 由三个独立仓库协同交付，完整的调用与任务闭环为：`Frontend -> Backend -> Celery Worker -> Backend -> Frontend`。

| 仓库 | 职责 | 与其他仓库的关系 |
| --- | --- | --- |
| Backend（本仓库） | 提供 API、认证与业务编排，并受理异步运维任务。 | 接收 Frontend 的管理和查询请求；向 Celery Worker 投递区域任务，并向 Frontend 提供任务状态和结果。 |
| Frontend | 提供浏览器中的管理控制台。 | 通过 Backend 的版本化 API 发起操作、查询数据并展示异步任务进度。 |
| Celery Worker | 执行归档、配额聚合、容量快照和存储运维等后台任务。 | 消费 Backend 投递的本区域任务，并将执行状态和结果持久化，供 Backend 与 Frontend 查询。 |

相关仓库：

- [Storagent Frontend](https://github.com/zl875136491/storagent-frontend)
- [Storagent Celery Worker](https://github.com/zl875136491/storagent-celery)

## 项目结构

```text
backend/
├── README.md
├── main.py                         # FastAPI 生命周期、Etcd 初始化与 watcher
├── storagent.sh                    # 开发/生产启动入口
├── Dockerfile
├── requirements.txt
├── .env.example
├── docs/
│   ├── assets/storagent-diagram.webm
│   ├── CELERY_OPERATIONS.md         # 任务分发、执行与发布说明
│   ├── RELEASE_COMPATIBILITY.md     # R-001 至 R-014 发布控制
│   └── RUNBOOK.md
├── src/
│   ├── api/                         # v1/v2 路由装配
│   ├── configs/                     # Settings、权限与常量
│   ├── core/                        # DB、Auth、Etcd、MinIO、Celery 路由与异常
│   ├── modules/
│   │   ├── auth/ public/ storage/ files/ graph/ usage/
│   │   ├── diagnostics/ capacity/ etcd/ celery/ audit/
│   │   └── ai/ demo/ health/
│   ├── scripts/                     # 运维脚本
│   └── utils/
└── tests/                           # 单元、契约与风险回归测试
```

## 快速开始

### 前置服务

- Python 3.12+
- MongoDB
- 本区域 MinIO
- Etcd 集群（多区域控制面）
- `mc`（项目随 `runtimes/mc` 提供）
- 独立 Celery Worker（需要异步任务时）

### 安装和运行

```bash
pip install -r requirements.txt
cp .env.example .env
```

配置 `.env` 中的 `SECRET_KEY`、`BCRYPT_SALT`、MongoDB、MinIO、Etcd 和区域参数后：

```bash
RELOAD=true ./storagent.sh run
```

生产进程通常使用：

```bash
RELOAD=false DEBUG=false ./storagent.sh run
```

可用健康检查：

```text
GET /health
GET /ready
```

当 `ENABLE_DOCS=true` 时，OpenAPI 文档位于 `/docs`。

## 配置重点

完整变量请以 [`.env.example`](.env.example) 为准。多区域部署需要重点核对以下组：

| 配置组 | 必须保持一致或正确区分的项 |
| --- | --- |
| 区域 | 每个节点 `REGION` 唯一；所有区域的 `SYNC_AUTHORITY_REGION` 一致。 |
| 安全 | `SECRET_KEY`、`BCRYPT_SALT` 多区域一致；生产环境使用明确的 `BACKEND_CORS_ORIGINS`。 |
| Etcd | `ETCD_ENDPOINTS`、凭据、健康阈值与快照目录；不要把 Etcd 暴露为公网入口。 |
| Celery | 同 Region API/Worker 的 `REGION`、Broker、Result Backend、queue prefix 和 protocol 必须一致。 |
| 归档 | `OBJECT_ARCHIVE_ENABLED=false` 为默认安全值；启用时所有区域必须具有相同恢复期、归档桶和策略指纹。 |
| 容量与配额 | 聚合批次、容量并发、样本最大年龄应按区域规模设定，避免周期扫描压垮 MinIO。 |

## 测试与发布前核对

```bash
pytest
```

部署前不要只替换其中一个仓库。区域队列协议、任务注册和运维任务状态机跨 Backend、Celery Worker、Frontend 共同生效。推荐的协调发布顺序、回滚限制和 `R-001` 至 `R-014` 风险控制见 [发布兼容性说明](docs/RELEASE_COMPATIBILITY.md)。

## 相关文档

- [Celery 后台任务说明](docs/CELERY_OPERATIONS.md)
- [发布兼容性与风险控制](docs/RELEASE_COMPATIBILITY.md)
- [运行手册](docs/RUNBOOK.md)
