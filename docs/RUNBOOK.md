# Storagent 多地区运维 Runbook

面向跨 Region 部署的故障处理与数据保护。日常功能说明见 `README.md`。

## 1. 拓扑与依赖

每个 Region 节点依赖：

| 组件 | 作用 | 就绪检查 |
|------|------|----------|
| Storagent API | 本区控制面 | `GET /health` 存活；`GET /ready` 含 Mongo + Etcd |
| MongoDB | 本区元数据 / 会话黑名单 / 审计 | `/ready` |
| Etcd | 跨区身份、节点清单、拓扑布局、应用、API Key、吊销 token | `/ready` |
| MinIO | 对象数据与 Bucket Replication 事实状态 | 业务侧探测 |

约定：**一 Region 一 Storagent 主实例**（同区水平扩展需另行设计实例 ID）。

## 2. 探针与摘流

- Liveness：`GET /health`（进程存活即可）
- Readiness：`GET /ready`（Mongo 或 Etcd 不可用 → **HTTP 503**）
- 指标：`GET /metrics`（Prometheus）或 `GET /metrics?format=json`
- Docker `HEALTHCHECK` 应对齐 `/ready`

编排侧：未 ready 的实例不得接新流量。

## 3. 常见故障

### 3.1 Etcd 不可达 / 分区

**现象**：`/ready` 503；写 Region/MinIO/Application/API Key 返回业务码 `503040`（跨节点同步失败）。

**处理**：

1. 确认各节点 `ETCD_*` 与 `SECRET_KEY` 一致。
2. 恢复 Etcd 多数派；查看 Storagent 日志中 `Etcd watch 异常` / `CAS conflict`。
3. Etcd 恢复后由 Watch 和默认 120 秒周期全量校准自动收敛；关注 `sync_reconcile_failures_total`。
4. 勿在 Etcd 未恢复时强行改拓扑（易造成本地孤儿数据）。

### 3.2 幽灵节点 / 拓扑不收敛

**现象**：locate 指向已下线 Region；列表仍有旧 MinIO。

**处理**：

1. 管理面调用区域下线 API（`DELETE /api/v1/public/region/{id}`，需 `region_manage`）。
2. 确认 Etcd `region` / `servers` map 中已移除该 key，`topology_layout` 中也没有该节点的布局记录。
3. 其他节点应通过 PUT 收敛删除远程条目；若整 key 被 DELETE，会按空 map 收敛远程拓扑。

### 3.3 登出后他区仍可用

**现象**：A 区 logout，B 区 access 仍短暂有效。

**处理**：吊销依赖 Etcd `revoked_tokens`。检查 B 区 Watch 与周期校准指标；确认 `SECRET_KEY` 一致以便校验 JWT。黑名单保留至 JWT `exp`。

### 3.4 用户、角色或拓扑布局不一致

**现象**：不同 Region 的用户列表、角色或 Bucket 图形位置不同。

**处理**：

1. 确认 Etcd 中存在 `roles`、`users` 和 `topology_layout`；后者 `_meta.authority_region` 应为北京。
2. 检查 `/metrics?format=json` 中 `sync_last_success_timestamp_seconds` 持续更新，且 `sync_reconcile_failures_total` 未增长。
3. 不要直接修改各区 Mongo；用户按用户名、布局按 Bucket/Server/Edge 组合键由 Etcd 收敛。
4. `master`、Mongo `_id`、审计和访问统计是本地数据，出现差异属于预期。

### 3.5 应用启用但他区未建桶

**现象**：授权 SSE 中 sync 失败；或对象只在单区。

**处理**：查看授权流 `sync` 步骤与 MinIO Bucket Replication 日志；修复后重新授权或手动 `ensure` 桶与复制规则。

### 3.6 SECRET_KEY / 加密凭证异常

**现象**：解密失败、同步用户/服务器密钥无法使用。

**处理**：全网 `SECRET_KEY` 必须一致；轮换需有计划（停写 → 换密钥 → 重加密 Etcd/Mongo 机密或重建受影响条目）。禁止使用占位密钥启动生产（`DEBUG=false` 时启动校验会拒绝）。

## 4. 备份与恢复（建议 RPO/RTO）

| 数据 | 建议备份 | 恢复要点 |
|------|----------|----------|
| Etcd | 定期 `etcdctl snapshot save` | 先恢复 Etcd，再启 Storagent；避免空集群覆盖 |
| Mongo（每区） |  mongodump / 云快照 | 按区恢复；注意 token 黑名单与审计集合 |
| MinIO | 版本控制 + 跨区 Replication；对象层备份按合规要求 | 先恢复复制拓扑，再校验桶与版本 |

建议目标（可按业务调整）：

- **RPO**：Etcd/Mongo ≤ 1h；对象依赖复制滞后窗口
- **RTO**：单区控制面 ≤ 30min；跨区切换依赖 DNS/前端候选列表

恢复后验证清单：

1. `/ready` 200
2. `/api/v1/public/endpoints` 拓扑正确
3. 管理登录 + 创建/吊销测试 Key
4. 跨区 locate / 下载指引

## 5. 发布与回滚

### 5.1 通用发布

1. 在测试环境完成单元、接口和真实依赖验证；本地 commit 不等于已完成环境验证。
2. 先以只读方式核对每个节点的 `REGION`、`SYNC_AUTHORITY_REGION`、Etcd、MinIO、Mongo 和 CORS/secret 配置。
3. 新 API 实例必须先通过 `/ready` 再接入流量；优雅关闭默认约 30 秒（`GRACEFUL_SHUTDOWN_TIMEOUT`）。
4. 不在同一发布中修改 `OBJECT_RECOVERY_PERIOD_DAYS`。该值直接影响 v2 删除对象的 `restore_until`。

### 5.2 Celery 协议发布

Celery 的 Region queue 和 task protocol 是 Backend、Worker、Frontend 的共同兼容面。详细任务清单、状态机和参数见 [`CELERY_OPERATIONS.md`](CELERY_OPERATIONS.md)。

1. 使旧 API producer 静默或摘流，排空旧共享 `celery` 队列中的手工任务。
2. 停止旧 Worker/Beat 后部署新 Worker；验证每个 Region 的 `storagent.<region>.v<protocol>` 队列、12 个已注册任务、Worker 心跳和每 Region 单 Beat 租约。
3. 部署 Backend，验证 `/ready`、v1/v2 存储运维兼容接口、诊断和一条可回收的手工任务。
4. 最后部署 Frontend。旧前端在过渡期仍依赖的 `orphan-buckets` 旧响应模型和复制运维 HTTP 200 受理语义必须保留。
5. 回滚前先停止新 producer，并排空或人工标记新协议队列中的手工任务；不能仅回滚 Worker 或 Backend。

### 5.3 归档首次开启

1. 首发保持 `OBJECT_ARCHIVE_ENABLED=false`。
2. 每个 Region 用可回收的真实版本化对象完成复制、校验、恢复和源版本删除演练。
3. 核对归档桶版本控制、`storagent-expired-archive-retention` 生命周期规则、当前/非当前版本保留期，以及权威 Region 发布的策略 fingerprint。
4. 先用很小的 `OBJECT_ARCHIVE_BATCH_SIZE` 灰度。生产环境不得设置 `OBJECT_ARCHIVE_AUTOCONFIGURE=true`。

## 6. 限流与审计

- 登录：同 IP 约 10 次/分钟；刷新 30 次/分钟；locate 60 次/分钟（超出 → `429041`）。
- 审计：日志 `[AUDIT]` + Mongo `audit_event` 集合；指标 `audit_events_total`。

## 6.1 Celery 背景任务

- Celery worker、Region queue、任务生命周期记录和跨 Region 保护见 [`CELERY_OPERATIONS.md`](CELERY_OPERATIONS.md)。
- 当前实现按 `storagent.<region>.v<protocol>` 路由。Worker 只消费本区协议队列，并拒绝没有有效 Region/protocol header 的任务。
- 同 Region 多 Worker 可同时运行，但只有持有 Mongo Beat lease 的一个实例会投递周期任务。Celery 运维页面可核对 expected queue、Worker 队列、任务来源 Region 和 Beat 租约。

## 7. 联系与升级

变更密钥、扩容同区多实例、或引入共享会话存储前，先更新本 Runbook 与 `README` 部署要求。
