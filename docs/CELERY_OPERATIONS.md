# Celery Background Tasks

审阅日期：2026-08-26。本文件描述当前已实现的 Celery 分发、执行和故障处置机制。它适用于共享 MongoDB broker 的多 Region 部署。

## 1. Core Model

每个 Region 的 API 和 Worker 必须使用相同的以下配置：

```dotenv
REGION=beijing
CELERY_TASK_QUEUE_PREFIX=storagent
CELERY_TASK_PROTOCOL_VERSION=2
CELERY_BROKER_URL=mongodb://...
CELERY_RESULT_BACKEND=mongodb://...
```

任务队列的唯一名称为：

```text
<CELERY_TASK_QUEUE_PREFIX>.<REGION lowercase>.v<CELERY_TASK_PROTOCOL_VERSION>
```

例如北京 Region 的协议 2 队列是 `storagent.beijing.v2`。API producer 只向本 Region 队列投递任务，并附带以下 Celery header：

```text
storagent-origin-region: beijing
storagent-task-protocol: 2
```

Worker 只订阅自己的队列，且每个任务入口都验证 header。缺少 header、来源 Region 不一致或协议不一致都会失败，绝不会回退消费旧的共享 `celery` 队列。因此，即使多个 Region 共用同一个 MongoDB broker，也不能跨区取走任务。

## 2. Scheduler and Ownership

一个 Worker 可以启用 Beat，但同 Region、同协议的多个 Worker 会通过 Mongo 集合 `celery_beat_locks` 竞争短租约：

```text
storagent-beat:<region>:v<protocol>
```

只有租约持有者执行 `tick()` 并投递周期任务。无法访问 broker 时 Beat 失去领导权并停止投递，而不是在分区时继续重复调度。

| 任务 | 触发方式 | 调度/执行范围 | 重试策略 |
|---|---|---|---|
| `storagent.auth.cleanup_expired_tokens` | Beat | 本区认证数据 | 可重试 |
| `storagent.files.archive_expired_objects` | Beat，显式开启归档后 | 本区且 `source_region` 匹配的对象 | 可重试，复制/删除幂等 |
| `storagent.etcd.reconcile` | Beat | 本区 Mongo 与共享 Etcd | 可重试 |
| `storagent.storage.sync_file_inventory` | Beat（6 小时）/ Celery 运维手动发起 | 本区 MinIO listing 与 Mongo 对象索引 | 不自动重试；同区互斥 |
| `storagent.maintenance.recover_queued_tasks` | Beat | 本区存储/Etcd 手工任务与过期任务历史 | 可重试 |
| `storagent.replication.reconcile_policies` | Beat | 仅 `SYNC_AUTHORITY_REGION` | 可重试 |
| `storagent.public.refresh_quota_aggregates` | Beat | 仅权威 Region，按批次轮转 | 可重试 |
| `storagent.capacity.snapshot` | Beat | 仅权威 Region | 可重试 |
| `storagent.storage.monitor_cluster_health` | Beat | 仅权威 Region，且 `AUTO_HEAL_ENABLED=true` | 可重试 |
| `storagent.etcd.execute` | Etcd 运维操作 | 创建任务的本区 Mongo | 不自动重试 |
| `storagent.storage.execute_operation` | 存储运维操作 | 创建任务的本区 Mongo | 不自动重试 |
| `storagent.audit.persist` | 审计事件 | 产生事件的本区 Mongo | 可重试，`event_id` 幂等 |

权威任务不会由非权威 Region 的 Beat schedule 生成；入口仍保留 Region 判断，防止手工投递或错误配置绕过调度保护。

## 3. Manual Operation State Machine

存储运维和 Etcd 运维任务先落库，再派发 Celery。持久化记录包含：

```text
origin_region, celery_task_id, dispatch_attempts, dispatched_at
```

Worker 不使用“读取后直接保存”的方式进入执行态，而是原子地将本区 `queued` 记录领取为 `running`。这避免了延迟消息与看门狗并发时，Worker 覆盖 `failed/recovery_required` 状态并继续执行外部副作用。

```text
queued --(Worker atomic claim)--> running --(confirmed result)--> succeeded/failed
   |                                  |
   +--(start timeout)--> failed        +--(running timeout)--> failed
                         recovery_required=true
```

`storagent.maintenance.recover_queued_tasks` 会将以下任务标记为需要人工复核，不会盲目重新投递：

- 已派发但在 `CELERY_OPERATION_START_TIMEOUT_SECONDS` 内未进入 `running`；
- 已运行超过 `CELERY_OPERATION_RUNNING_TIMEOUT_SECONDS`。

这样处理是刻意的：复制 resync、MinIO 删除、Etcd compact/defrag 在“外部调用已成功、进程在落库前中断”的窗口中不一定可以安全重放。人工复核原生 MinIO/Etcd 状态后，再创建新的运维任务。

同一看门狗还会结束本区 `celery_task_history` 中超过运行超时仍停在 `STARTED`/`RETRY` 的记录。这只改观察数据，不重放任务、不触碰对象或 Etcd 键。Worker 启动时也会把本机 hostname 上一次进程留下的同类记录收成 `FAILURE`，避免容器重启后因 hostname 不变而一直显示为执行中。

审计任务例外：每个 producer 在投递时生成 UUID `event_id`，Mongo 对该字段有 sparse unique index。重复投递会作为成功处理，因此可以安全自动重试。

## 4. Archive Safety

对象恢复期归档默认关闭：

```dotenv
OBJECT_ARCHIVE_ENABLED=false
OBJECT_ARCHIVE_AUTOCONFIGURE=false
```

开启后，Worker 只扫描 `source_region == REGION` 的目录记录；缺失源 Region 的旧记录和其他 Region 的记录均不会被当前节点认领。归档前必须满足：

1. 权威 Region 已在 Etcd 发布相同的归档策略 fingerprint；
2. 归档桶已启用版本控制；
3. 存在名为 `storagent-expired-archive-retention` 的生命周期规则；
4. 当前版本和非当前版本的保留期均不小于 `OBJECT_ARCHIVE_RETENTION_DAYS`；
5. 已用真实的版本化对象完成复制、校验、恢复和源版本删除演练。

生产环境不得设置 `OBJECT_ARCHIVE_AUTOCONFIGURE=true`。该选项仅用于新建的测试桶；已有归档桶策略不会被服务静默覆盖。

## 5. Capacity and Quota Workload Bounds

- 归档每轮最多处理 `OBJECT_ARCHIVE_BATCH_SIZE` 条记录；
- 配额聚合每轮读取最多 `APPLICATION_QUOTA_AGGREGATE_BATCH_SIZE` 个启用应用；按 `quota_usage_attempted_at` 轮转，持续失败的应用不会饿死后续应用；
- 容量快照对 MinIO 集群/复制检查使用 `CAPACITY_SNAPSHOT_MAX_CONCURRENCY`，复制检查不会为全部桶-Region 组合预建 asyncio Task；
- 调用方诊断只读取持久化配额聚合和 Etcd 发布的容量规划，绝不在请求路径扫描 MinIO。

容量样本过期、复制冗余偏低或状态为 degraded 会返回降级预检和 warning；只有明确的配额耗尽、fresh offline/unreachable/critical 或已知物理容量耗尽才阻断完整自诊断。

## 6. Observability

Worker 将最小化的任务生命周期和心跳写入 result MongoDB：

- `celery_task_history`：默认保留 30 天；
- `celery_worker_heartbeats`：默认保留 7 天；
- 两个集合都使用 TTL index；
- result 只保留白名单中的运行计数，error 会脱敏 token、password、URL 凭据和 query secret。

Celery 运维页面是只读的。服务端对 overview 做 `CELERY_OVERVIEW_CACHE_SECONDS` 短缓存，并分别限制 overview/history 的用户和客户端 IP 请求频率。页面显示 expected queue、Worker 实际队列/协议、任务来源 Region 和 Beat 租约，便于定位不匹配配置。

执行中列表会把 Mongo broker 上可能漏检的 inspect 结果，与 `celery_task_history` 里仍在心跳窗口（`CELERY_WORKER_STALE_AFTER_SECONDS * 3`，至少 300 秒）内更新的 `STARTED`/`RETRY` 合并。Worker 主机名在线不足以让过期历史继续显示为执行中。

旧的 `celery_taskmeta` 没有经过新的脱敏协议；页面只显示“历史记录已隐藏未脱敏内容”，不会泄露旧 result/traceback。

## 7. Release and Rollback

区域队列协议与 Worker 任务注册是 Backend、Worker、Frontend 的共同兼容面，不能把三个仓库当成可任意错峰的独立发布单元。

### 发布前检查

1. 对每个测试/生产节点只读核对 `REGION`、权威 Region、Broker/Result URL、queue prefix、protocol、恢复期、归档开关和 TTL/批次配置。
2. 确认现有 `celery` 旧队列没有未完成的手工任务；必要时先由旧 Worker 排空。
3. 暂停新的手工运维任务，并在切换窗口中让旧 API producer 停止投递旧队列。
4. 确认新 Worker 的注册任务包含本表 11 项，且心跳显示预期 Region queue。

### 推荐顺序

1. 使 API producer 静默或摘流，排空旧 `celery` 队列；
2. 停止旧 Beat/Worker，部署同一兼容版本的新 Worker；
3. 验证新 Region queue、心跳、Beat 单租约和任务注册；
4. 部署 Backend 并验证 `/ready`、诊断和一条可回收的手工任务；
5. 恢复 API 流量，最后发布 Frontend。

新 Worker 故意不消费没有 envelope 的旧队列，因此“先替换 Worker、旧 Backend 仍持续处理流量”不是安全的发布方式。

回滚前先停止新 producer，并排空或人工标记新 `v<protocol>` 队列中的手工任务；不得仅回滚 Worker 而让新 Backend 继续向协议队列投递。归档已经删除的源对象不能依赖代码回滚恢复，必须按归档恢复流程处理。
