# Celery 背景任务运行说明

审阅基线：2026-08-25。本文件说明当前 Storagent 代码中 Celery 的触发、执行、分发与跨区域行为；不把设计预期当作现有保证。

## 1. 组件与数据流

每个部署了 worker 的 Region 运行一个独立的 Celery 进程：

```text
FastAPI / Celery Beat
        |
        | send_task(...)
        v
MongoDB broker: celery.messages / celery.routing / celery.queues
        |
        v
Celery worker
        |
        +--> 本区 Mongo / MinIO / Etcd
        +--> MongoDB result: celery_taskmeta
        +--> MongoDB observability: celery_task_history / celery_worker_heartbeats
```

当前启动命令位于 worker 的 `worker.sh`：一个 worker 同时启动 `worker` 和 `beat`，并启用 Celery events。worker 使用 MongoDB 作为 broker 和 result backend，启用 `task_acks_late`、`task_reject_on_worker_lost`、`worker_prefetch_multiplier=1`，任务异常会按各任务定义最多自动重试 5 次。

管理 API 不会返回 broker URL、账号密码、任务参数或 kwargs。运行态来自 Celery inspect；队列深度、任务历史和 worker 心跳来自 MongoDB。旧的 `celery_taskmeta` 只有状态、结果和完成时间，**没有任务名、执行 Region 或 worker**；新版本开始由 `celery_task_history` 记录这些字段。

## 2. 当前分发结论

### 2.1 代码层没有按 Region 路由

`src/core/celery_client.py` 的 `dispatch_task()` 调用 `send_task()` 时没有指定 `queue`；worker 也没有配置 `task_routes`。因此所有任务默认发往 `celery` 队列，代码本身没有携带或校验“来源 Region”。

### 2.2 是否跨区重复，取决于 broker 是否隔离

| 部署条件 | 实际行为 |
|---|---|
| 每个 Region 使用自己的 MongoDB broker/数据库 | 每个 Region 的 worker 只能消费自己 broker 中的 `celery` 消息。周期任务会在每个 Region 各调度一次，适合本区清理、归档和同步。 |
| 多个 Region 共用同一个 MongoDB broker/数据库 | 任一 worker 都可能消费任一消息；每个带 `--beat` 的 worker 还会独立投递一份周期任务。当前没有 leader election、分布式 Beat 锁或 Region queue，因此会出现重复调度和跨区误消费风险。 |

**结论**：当前实现不是“每个 Region 的 Celery 天然只做本区任务”。它只在“broker 按 Region 隔离”这一部署前提成立时表现为本区执行。若 broker 共享，调度和消费都不具备 Region 隔离保证。

### 2.3 权威区域任务的实际保护

下列任务在函数入口检查 `REGION == SYNC_AUTHORITY_REGION`，非权威区域会直接跳过业务动作：复制策略校准、应用配额聚合、容量快照、MinIO 自愈巡检。这个保护能避免非权威 Region 执行核心动作，但无法阻止多余消息被投递、消费和写入结果。

## 3. 已注册任务清单

| 任务 | 触发 | 执行角色 | 执行范围与保护 | 当前分发机制 |
|---|---|---|---|---|
| `storagent.auth.cleanup_expired_tokens` | Beat，`AUTH_CLEANUP_INTERVAL_SECONDS` | Celery worker | 清理本区 JWT 吊销记录和 OA 挑战；无权威区域限制 | 默认 `celery` 队列 |
| `storagent.files.archive_expired_objects` | Beat，`OBJECT_ARCHIVE_INTERVAL_SECONDS` | Celery worker | 扫描本区 `ObjectCatalog`，将恢复期超时对象复制到内部归档桶后删除源对象；无权威区域限制 | 默认 `celery` 队列 |
| `storagent.etcd.reconcile` | Beat，`SYNC_RECONCILE_INTERVAL_SECONDS` | Celery worker | 执行本区 Mongo 与共享 Etcd 的全量校准；无单一调度者保护 | 默认 `celery` 队列 |
| `storagent.replication.reconcile_policies` | Beat，`REPLICATION_RECONCILE_INTERVAL_SECONDS` | Celery worker | 仅权威区域补齐应用复制规则和桶配额；非权威区域返回 | 默认 `celery` 队列 |
| `storagent.public.refresh_quota_aggregates` | Beat，`APPLICATION_QUOTA_AGGREGATE_INTERVAL_SECONDS` | Celery worker | 仅权威区域采集各区应用用量并更新 Etcd 聚合；非权威区域返回 `skipped` | 默认 `celery` 队列 |
| `storagent.capacity.snapshot` | Beat，`CAPACITY_SNAPSHOT_INTERVAL_SECONDS` | Celery worker | 仅权威区域采集容量快照并发布容量规划；非权威区域返回 | 默认 `celery` 队列 |
| `storagent.storage.monitor_cluster_health` | Beat，`CLUSTER_HEALTH_CHECK_INTERVAL_SECONDS` | Celery worker | 仅权威区域，且 `AUTO_HEAL_ENABLED=true` 时执行；否则返回 | 默认 `celery` 队列 |
| `storagent.etcd.execute` | Etcd 运维页面创建任务 | Celery worker | 根据本区 Mongo 中的 `EtcdOperationTask` ID 执行检查、压缩、defrag 或解除告警 | 默认 `celery` 队列 |
| `storagent.storage.execute_operation` | 存储运维页面创建任务 | Celery worker | 根据本区 Mongo 中的 `StorageOperation` ID 执行复制校准、resync、自愈或未纳管桶删除 | 默认 `celery` 队列 |
| `storagent.audit.persist` | 任意调用 `audit.audit()` 的业务操作 | Celery worker | 将审计事件写入当前执行 worker 所连接的本区 Mongo；派发失败时 API 进程回退为本地异步落库 | 默认 `celery` 队列 |

## 4. 共享 broker 时的风险

| 场景 | 现有后果 | 风险级别 |
|---|---|---|
| 多 Region 都运行 `--beat` | 每个 Beat 都会投递相同周期任务 | 高：重复消息和多余重试；权威任务虽会跳过但仍消耗资源 |
| 手工 Etcd / 存储运维任务被异地 worker 取走 | 任务 ID 在异地 Mongo 通常不存在，worker 直接返回；若本地存在同 ID，可能执行错误的本地任务 | 高：执行结果与发起页面不一致，无法保证任务落在创建 Region |
| 审计任务被异地 worker 取走 | 审计落到异地 Mongo，Region 归属偏离原始操作 | 中：审计追溯不完整 |
| 文件归档被异地 worker 取走 | 扫描的是异地对象目录和归档桶，不是发起 Beat 的 Region | 中：调度节奏和归档责任失真 |
| Etcd 全量校准被任意 worker 消费 | 不能保证每个 Region 在自己的周期内完成校准 | 中：某些 Region 的收敛时效不可预测 |

## 5. 现有测试环境观察

截至 2026-08-25 的测试环境，backend A（`REGION=nuc-docker-a`）和 backend B（`REGION=nuc-docker-b`）都配置到同一个 `storagent_celery` broker 数据库；其中只运行了一个 `REGION=nuc-docker-a` 的 worker。broker 中只有一个业务队列 `celery`，另有 Celery control 的临时 `pidbox` 队列。

这意味着测试环境已经存在一条明确的跨 Region 风险路径：由 backend B 派发的 `storagent.etcd.execute`、`storagent.storage.execute_operation` 或 `storagent.audit.persist` 可以被 A worker 消费。前两个任务会按 A 的本区 Mongo 查询传入的任务 ID，通常找不到时直接返回；审计任务则可能写入 A 的本区审计库。当前测试环境只有一个 Beat，因此尚未发生“多个 Beat 重复投递周期任务”；一旦 B 也启动带 `--beat` 的 worker，就会出现第 4 节所述的重复调度风险。

已在 2026-08-25 做过一次实测：由 backend B（`REGION=nuc-docker-b`）投递的 `storagent.audit.persist`，任务 ID 为 `759bd5a1-8d94-48d9-8dff-12366d094c84`；`celery_task_history` 记录显示它由 `storagent-nuc-docker-a@49d48e41a49a` 执行，记录 Region 为 `nuc-docker-a`，状态为 `SUCCESS`。这证明共享 broker 下的跨 Region 消费已在测试环境实际发生，不是仅凭代码推断。

新建的 Celery 运维页面会显示当前 broker 中的 worker 名称、心跳上报 Region、默认队列深度和任务历史。若各 Region 使用隔离 broker，需要分别打开各 Region 的管理端查看本区 worker；如果同一页面出现多个 Region 的 worker，则说明这些 worker 可见于同一个 broker，必须按“共享 broker”风险处理。

## 6. 后续整改建议

以下是需要单独立项的分发改造，不是本次只读管理模块隐含完成的行为：

1. 周期任务只运行一个 Beat：可部署单独 scheduler，或引入带分布式锁的 Beat 实现；权威任务应只由权威 Region 调度。
2. 以 Region 建立任务队列，例如 `celery.<region>`，并在 producer 上为本区任务显式指定 queue。
3. 手工任务参数携带 `origin_region`，worker 在执行业务前校验它与自身 `REGION` 一致；不一致应拒绝并留下可诊断状态，而不是静默返回。
4. 对可能重复的周期任务补充显式幂等键或分布式锁；`task_acks_late` 只解决 worker 丢失后的重新投递，不解决多 Beat 产生的重复投递。
5. 在所有 Region 的页面核对 worker 心跳、broker 数据库与队列名称；部署配置变更必须把 broker 隔离策略写入运维清单。

## 7. 排障顺序

1. 在 Celery 运维页面确认 broker 可用、worker 为在线、队列积压是否增长。
2. 查看执行中、待取和定时任务，确认任务名、worker、Region 与预期一致。
3. 在历史记录中查看 `FAILURE` / `RETRY`，再转到对应的存储运维、Etcd 运维或审计页面确认业务状态。
4. 若历史记录显示“旧记录，任务名不可追溯”，这是旧版 `celery_taskmeta` 数据限制；等待本版本部署后的新任务写入 `celery_task_history`。
5. 若看到多个 Region worker 位于同一个 broker，暂停把手工本区运维任务投入该队列，先实施第 6 节的分区与调度改造。
