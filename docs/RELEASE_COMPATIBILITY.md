# Release Compatibility and Risk Controls

适用版本：2026-08-26 风险修复版本。本文是 `R-001` 至 `R-014` 的发布前核对表，不替代测试环境验证。

| 风险 | 已实施控制 | 发布时仍需执行的动作 |
|---|---|---|
| R-001 三仓库错峰发布 | Region/protocol 队列、Worker envelope 拒绝、兼容发布顺序 | API 静默，排空旧队列，再以 Worker -> Backend -> Frontend 完成同一兼容窗口 |
| R-002 跨区消费与重复 Beat | `storagent.<region>.v<protocol>` 队列和 Mongo Beat lease | 检查每 Region 的 Worker queue、header 和一条有效 Beat lease |
| R-003 受理后永久悬挂 | dispatch metadata、原子领取、超时看门狗 | 验证 queued/running 超时会变为 `failed + recovery_required` |
| R-004 归档删除源版本 | 默认关闭、源 Region 限制、策略 fingerprint、版本/生命周期门禁 | 仅在演练成功后小批量开启归档 |
| R-005 预检误判失败 | ready/degraded/blocked 和 confidence；stale 变 warning | 用可用但 stale 的聚合样本验证完整自诊断仍通过 |
| R-006 v2 MinIO 契约变化 | 顶层稳定为 `storage.unavailable`，details 分类，retryable 区分 | 回归上传、下载、诊断的认证/网络/操作错误 |
| R-007 恢复期语义变化 | 默认 30 天，发布清单要求全 Region 一致且首发不改 | 只读比对所有节点 `OBJECT_RECOVERY_PERIOD_DAYS` |
| R-008 新配置导致启动失败 | archive 关闭时不校验 archive bucket；Celery 参数和启用归档时的规则显式校验 | 候选容器先跑 `/ready`，不要通过关闭校验绕过错误 |
| R-009 权威任务单点 | Etcd 发布容量样本优先，local fallback，stale 降级而非阻断 | 确认权威 Worker 至少成功一轮并发布 Etcd 数据 |
| R-010 观测写压 | 进程内 MongoClient、TTL、结果白名单、敏感信息脱敏 | 核对 TTL index 和历史集合增长速率 |
| R-011 overview 控制面放大 | 服务端短缓存、用户/IP 限流、队列维度 Worker 数 | 多管理员刷新时观察 pidbox 与 Mongo 延迟 |
| R-012 旧管理端兼容 | 复制操作保持 HTTP 200 + accepted/operation 字段；旧 orphan 路径输出旧 vocabulary | 回归旧页面的 reconcile/resync/orphan-buckets 请求 |
| R-013 周期扫描压力 | archive/quota batch、quota 轮转、容量并发限制和有界任务创建 | 根据应用数和 Region 数设置批次/并发，确认一轮可在下轮前完成 |
| R-014 重复副作用 | 手工任务不自动重试、原子领取、看门狗人工复核；审计 UUID 幂等 | 注入 Worker kill / Mongo 落库失败，确认不会重复执行破坏性操作 |

## v2 Contract Notes

### Diagnostics

- `GET /api/v2/diagnostics/v2/probe` 不再接收或返回 APPID/app_name；身份由 APIKey 上下文决定。
- 完整自诊断为五个阶段：DNS、gateway、authentication、`quota_capacity`、storage。消费者必须按阶段 key 处理，而不是依赖固定数量。
- `quota_capacity` 的 `ready=true` 表示允许继续；`preflight_status=degraded` 和 `confidence=low` 表示样本不新鲜或冗余待关注，不等同 MinIO 不可用。

### File/MinIO Errors

- `error.code` 对认证、网络和一般 MinIO 操作错误都保持 `storage.unavailable`。
- 认证错误为 `retryable=false` 且 `details.category=authentication`；网络和一般操作错误为 `retryable=true`，category 分别为 `network` 和 `operation`。
- 调用方应以 `error.code + retryable + details.category` 做策略，不能解析 message。

### Storage Operations

- `POST /api/v2/storage/operations/replication/{bucket}/reconcile` 和 `resync` 保持 HTTP 200，响应包含 `accepted`、`operation_id`、`operation_status`；最终状态仍需按 operation ID 查询。
- `GET /api/v2/storage/operations/orphan-buckets` 保留旧 `orphan` vocabulary 和旧字段集合。新页面应使用 `unmanaged-buckets` 获得处置状态、覆盖范围和受控删除能力。
- 未纳管桶删除仍为异步 HTTP 202，必须按 operation ID 轮询，并保留确认文本和全站空桶复核。

## Configuration Preflight

对每个节点只读比对以下值。不要把测试环境值复制到生产环境，也不要在生产环境执行本次变更。

```text
REGION
SYNC_AUTHORITY_REGION
CELERY_BROKER_URL / CELERY_RESULT_BACKEND
CELERY_TASK_QUEUE_PREFIX / CELERY_TASK_PROTOCOL_VERSION
CELERY_BEAT_ENABLED / CELERY_BEAT_LOCK_*
CELERY_OPERATION_*
CELERY_TASK_HISTORY_RETENTION_DAYS / CELERY_WORKER_HEARTBEAT_RETENTION_DAYS
OBJECT_RECOVERY_PERIOD_DAYS
OBJECT_ARCHIVE_ENABLED / OBJECT_ARCHIVE_AUTOCONFIGURE / OBJECT_ARCHIVE_*
APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE
CAPACITY_SNAPSHOT_MAX_CONCURRENCY
```

所有同 Region API/Worker 必须得到同一 `REGION`、queue prefix 和 protocol。归档开启时，所有 Region 的恢复期、归档桶和保留期还必须与权威 Region 的 Etcd fingerprint 一致。

## Test Environment Acceptance

1. 两个不同 Region 的 Worker 同时启动：向 A 投递的任务不可由 B 消费，反向亦然。
2. 同 Region 两个带 Beat 的 Worker 同时启动：同一 `storagent-beat:<region>:v2` 仅有一个 active lease。
3. 手工操作在未被领取/执行超时时得到 `recovery_required=true`；重新投递前必须人工确认外部状态。
4. 配额与容量均有余量但样本 stale 时，诊断输出 `ready=true`、`preflight_status=degraded`。
5. 模拟 MinIO AccessDenied 时 v2 返回 `storage.unavailable`、`retryable=false`。
6. 归档默认关闭；仅在测试桶上以明确开关验证 copy, verify, restore, version delete 和 lifecycle。
7. v1/v2 存储运维兼容路由和前端 Celery 页面均可正常渲染。
