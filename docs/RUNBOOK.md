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
3. Etcd 恢复后由 Watch 和默认 30 秒周期全量校准自动收敛；关注 `sync_reconcile_failures_total`。
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

1. 镜像构建前跑 `pytest`（Jenkins Test 阶段）。
2. 滚动时先等新实例 `/ready`，再摘旧实例。
3. 优雅关闭默认约 30s（`GRACEFUL_SHUTDOWN_TIMEOUT`）。
4. 回滚：部署上一镜像标签；确认 `.env` 未引入不兼容的密钥/REGION。

## 6. 限流与审计

- 登录：同 IP 约 10 次/分钟；刷新 30 次/分钟；locate 60 次/分钟（超出 → `429041`）。
- 审计：日志 `[AUDIT]` + Mongo `audit_event` 集合；指标 `audit_events_total`。

## 6.1 Celery 背景任务

- Celery worker、默认队列、任务生命周期记录与跨 Region 分发风险见 [`CELERY_OPERATIONS.md`](CELERY_OPERATIONS.md)。
- 当前实现没有 Region task routing；部署时必须确认 MongoDB broker 是否按 Region 隔离。若多个 Region 共享 broker，不能假定手工运维任务会由创建它的 Region 执行。

## 7. 联系与升级

变更密钥、扩容同区多实例、或引入共享会话存储前，先更新本 Runbook 与 `README` 部署要求。
