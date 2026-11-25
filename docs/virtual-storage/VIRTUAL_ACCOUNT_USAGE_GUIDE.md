# 虚拟存储账号使用注意事项

本文档说明虚拟存储账号和租户 API 的调用规范、参数要求、错误处理及密钥管理注意事项。

## 1. 账号标识符说明

虚拟存储账号有多个标识符，用于不同场景：

- **内部 account_id**：数据库内部标识（格式如 `acct_xxx`），不对外暴露
- **app_name**：业务名称，API 路径参数，示例：`/api/virtual-storage/v1/accounts/{app_name}/tenants`
- **public_account_id**：公开账号 ID，OAuth 2.0 的 `client_id`，可能与 `app_name` 相同或不同

**重要**：API 路径参数 `app_name` 与客户端请求头 `X-App-Id` 是不同层次的概念，不可混淆

## 2. OAuth 管理凭据与租户 API Key 的职责

### OAuth 管理凭据 (`client_id` / `client_secret`)

**用途**：用于账号级别的管理操作

**使用场景**：
- 获取 access token：`POST /oauth2/token`，`grant_type=client_credentials`，HTTP Basic 认证
- 刷新 token：`POST /oauth2/refresh`，`grant_type=refresh_token`，HTTP Basic 认证
- 创建租户：`POST /api/virtual-storage/v1/accounts/{app_name}/tenants`
- 查询租户列表：`GET /api/virtual-storage/v1/accounts/{app_name}/tenants`
- 管理租户配额：`PATCH /api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}`
- 颁发/撤销租户 API Key

**认证方式**：
```
Authorization: Bearer <access_token>
```

**Token 有效期**（来源：`src/virtual_storage/settings.py`）：
- Access token：默认 3600 秒（1 小时），可通过 `VS_ACCESS_TOKEN_SECONDS` 配置
- Refresh token：默认 604800 秒（7 天），可通过 `VS_REFRESH_TOKEN_SECONDS` 配置
- 每次刷新 refresh token 会更换

**密钥轮换**（来源：`docs/virtual-storage/OAUTH_APPS.md`）：
- 密钥可在 Stor 管理页面重新生成
- 重新生成后，旧 `client_secret`、旧 access token 和旧 refresh token **立即失效**
- 请妥善保管 `client_secret`，不得写入日志或提交到版本控制

### 租户 API Key

**用途**：租户级别的对象存储操作

**使用场景**：
- 上传对象：`POST /api/virtual-storage/v1/tenants/{tenant_id}/uploads`
- 查询对象：`GET /api/virtual-storage/v1/tenants/{tenant_id}/objects`
- 读取对象：`GET /api/virtual-storage/v1/tenants/{tenant_id}/objects/{object_key}`
- 删除对象：`DELETE /api/virtual-storage/v1/tenants/{tenant_id}/objects/{object_key}`

**认证方式**（来源：`src/virtual_storage/api/routes.py`）：
```
X-API-Key: <tenant_api_key>
```

**Key 特性**（来源：`src/virtual_storage/services/support.py`）：
- 租户 API Key 格式：`vst_` + 43 字符 URL-safe base64
- 创建租户时自动颁发，可通过管理接口额外颁发或撤销
- 撤销后该 Key 立即失效，不影响同一租户的其他 Key

## 3. 创建租户必填参数与单位

### API 端点

```http
POST /api/virtual-storage/v1/accounts/{account_id}/tenants
```

### 请求头

| 请求头 | 是否必填 | 说明 |
|-------|---------|------|
| `Authorization` | 是 | `Bearer <access_token>`，管理凭据的 access token |
| `Idempotency-Key` | 是 | 幂等键，建议使用 UUID 或业务唯一标识 |
| `X-Request-Id` | 否 | 请求追踪 ID，错误响应会回显此 ID，便于日志关联 |

### 请求体必填字段

| 字段 | 类型 | 是否必填 | 说明 |
|-----|------|---------|------|
| `external_tenant_id` | string | 是 | 外部租户 ID，用于业务系统关联，账号内唯一 |
| `logical_name` | string | 是 | 租户逻辑名称，账号内唯一 |
| `quota_bytes` | integer | 是 | 存储配额，单位：**字节 (Bytes)** |
| `quota_objects` | integer | 是 | 对象数量配额，单位：**个数** |
| `key_ttl_seconds` | integer | 否 | 租户 Key 的有效期（秒），不提供时使用系统默认值（`VS_KEY_TTL_SECONDS`，默认 30 天） |
| `tenant_credential` | string | 否 | 自定义租户凭据（高级功能，需业务合同支持） |

### 配额单位与示例

- **`quota_bytes`（字节）**：
  - 10 MiB = `10485760`
  - 100 MiB = `104857600`
  - 1 GiB = `1073741824`
  - 1 TiB = `1099511627776`

- **`quota_objects`（个数）**：
  - 直接填写对象数量上限
  - 示例：`1000`（一千个对象）

**示例请求体**（仅示例，不代表实际执行）：

```json
{
  "external_tenant_id": "APP-20261125-D6555CD6",
  "logical_name": "weekly-report-test",
  "quota_bytes": 104857600,
  "quota_objects": 1000
}
```

**注意**：
- **以上示例仅供参考格式，不保证可用配额**
- 实际分配需遵循账号剩余配额，超额会返回 `409 QUOTA_EXCEEDED`
- 系统对配额值本身不设置硬性上限（由账号可用配额和业务合同约束）
- 配额可以为 0（表示不限制或暂不分配）

## 4. 账号可分配配额与并发变化

### 配额分配机制

- 虚拟账号创建时分配总配额 `quota_bytes` 和 `quota_objects`
- 创建租户时从账号总配额中扣除请求的配额
- 租户配额总和不能超过账号总配额
- 配额一旦分配给租户，即使租户未使用，也从账号可用配额中扣除

### 配额变化与并发

- **创建租户**时会**原子性地**检查并扣除可用配额
- 如果可用配额不足，返回 `409 QUOTA_EXCEEDED`，不会创建租户
- 多个并发请求创建租户时，系统保证配额不会超额分配
- 账号配额可在 Stor 管理页面调整，调整后立即生效

**配额查询**（来源：`src/virtual_storage/services/accounts.py`）：

```http
GET /api/virtual-storage/v1/accounts/{app_name}
Authorization: Bearer <access_token>
```

响应字段（实际字段名）：

```json
{
  "account_id": "acct_xxx",
  "app_name": "app_xxx",
  "quota_bytes_total": 107374182400,
  "quota_bytes_allocated": 0,
  "quota_bytes_used": 1234567,
  "quota_bytes_reserved": 53687091200,
  "quota_bytes_trashed": 0,
  "quota_objects_total": 100000,
  "quota_objects_allocated": 0,
  "quota_objects_used": 10,
  "quota_objects_reserved": 50000,
  "quota_objects_trashed": 0
}
```

- `quota_bytes_total`：账号总配额
- `quota_bytes_reserved`：已预留（如正在上传）的字节数
- `quota_bytes_allocated`：已分配给租户的字节数
- 可用配额计算需结合 `total`, `allocated`, `used`, `reserved`, `trashed`

## 5. 唯一性约束

### `external_tenant_id` 唯一性

- **范围**：账号内唯一
- **用途**：业务系统用于关联租户的外部标识
- **冲突**：同一账号下重复的 `external_tenant_id` 会导致 `409 CONFLICT`

### `logical_name` 唯一性

- **范围**：账号内唯一
- **用途**：便于识别租户的逻辑名称
- **冲突**：同一账号下重复的 `logical_name` 会导致 `409 CONFLICT`

## 6. 幂等重试与参数一致性

### 幂等键 (`Idempotency-Key`)

- **必填**：创建租户时必须提供
- **用途**：防止网络重试导致重复创建
- **行为**：
  - 相同幂等键 + 相同请求体 → 返回首次创建的结果（200）
  - 相同幂等键 + 不同请求体 → 返回 `409 IDEMPOTENCY_MISMATCH`

### 重试建议

**网络超时或连接错误**：
- 可以使用相同幂等键和请求体重试
- 系统保证不会重复创建租户

**参数变化**：
- 如果业务参数发生变化，**必须使用新的幂等键**
- 否则会收到 `409 IDEMPOTENCY_MISMATCH` 错误

## 7. HTTP 状态码与错误处理

### 成功响应

| 状态码 | 说明 |
|-------|------|
| 200 OK | 租户创建成功或幂等重放成功 |
| 202 Accepted | 异步操作已接受 |

### 客户端错误 (4xx)

| 状态码 | error_code | 说明 |
|-------|-----------|------|
| 400 | `IDEMPOTENCY_REQUIRED` | 缺少 `Idempotency-Key` 请求头 |
| 401 | `INVALID_CREDENTIAL` | 认证失败，token 无效或已过期 |
| 403 | `ACCOUNT_SCOPE_MISMATCH` | token 无权访问该账号 |
| 403 | `ACCOUNT_DISABLED` | 账号已停用 |
| 404 | `ACCOUNT_NOT_FOUND` | 账号不存在 |
| 409 | `QUOTA_EXCEEDED` | 账号可用配额不足 |
| 409 | `CONFLICT` | `external_tenant_id` 或 `logical_name` 冲突 |
| 409 | `IDEMPOTENCY_MISMATCH` | 幂等键相同但请求体不同 |
| 422 | `MISSING_REQUIRED_FIELDS` | 缺少必填字段，`message` 中列出缺失的字段名 |
| 422 | `INVALID_QUOTA_BYTES` | `quota_bytes` 必须是整数（拒绝字符串、布尔值、浮点数等） |
| 422 | `INVALID_QUOTA_OBJECTS` | `quota_objects` 必须是整数（拒绝字符串、布尔值、浮点数等） |
| 422 | `INVALID_EXTERNAL_TENANT_ID` | `external_tenant_id` 必须是非空字符串（拒绝空字符串、空白字符串、非字符串类型） |
| 422 | `INVALID_LOGICAL_NAME` | `logical_name` 必须是非空字符串（拒绝空字符串、空白字符串、非字符串类型） |
| 422 | `INVALID_JSON` | 请求体不是有效的 JSON |
| 422 | `INVALID_REQUEST_BODY` | 请求体必须是 JSON 对象（拒绝 JSON 数组、null、非对象类型） |
| 422 | `INVALID_KEY_TTL` | `key_ttl_seconds` 必须是非负整数 |
| 422 | `INVALID_TENANT_CREDENTIAL` | `tenant_credential` 必须是字符串 |

### 服务器错误 (5xx)

| 状态码 | error_code | 说明 |
|-------|-----------|------|
| 500 | `INTERNAL_SERVER_ERROR` | 服务器内部错误（不应暴露敏感信息） |
| 503 | `INFRASTRUCTURE_UNAVAILABLE` | 基础设施不可用（如 MinIO、MongoDB） |
| 504 | `STORAGE_RESULT_UNKNOWN` | 对象存储操作结果不明确 |

### 请求 ID 关联

**适用范围**：本节描述的请求 ID 处理仅适用于 **虚拟存储租户创建接口** (`POST /api/virtual-storage/v1/accounts/{app_name}/tenants`) 及其错误处理。其他接口（如 OAuth、框架 404/405 等）的请求 ID 行为可能不同。

- 客户端**可选**在请求头中提供 `X-Request-Id`，建议使用业务唯一标识（如 `REQ-{timestamp}-{random}`）
- 如果未提供，服务器自动生成格式为 `REQ-{16位十六进制}` 的请求 ID
- **创建租户接口**的所有响应（成功和错误）都包含 `X-Request-Id` 响应头
- 服务器对恶意或超长请求 ID 进行安全过滤（移除控制字符，截断至 64 字符）
- 日志记录虚拟存储异常时包含请求 ID，但**不记录**请求正文、token 等敏感信息
- 用于在日志中关联请求与错误，便于问题排查

**虚拟存储错误响应格式**：

虚拟存储业务错误（`VirtualStorageError` 子类）的 JSON 体包含两个固定字段：

```json
{
  "error_code": "MISSING_REQUIRED_FIELDS",
  "message": "缺少必填字段: quota_objects"
}
```

响应头：

```
X-Request-Id: REQ-20261125-0F919C2E
Content-Type: application/json
```

## 8. 密钥日志脱敏与轮换影响

### 日志脱敏

**禁止记录的敏感信息**：
- `client_secret`（OAuth 管理凭据）
- 租户 API Key 的完整值（`vst_*`）
- access token 和 refresh token 的完整值

**允许记录的信息**：
- 账号 ID（`client_id`）
- 租户 ID
- Key ID（如 `key_abc123`）
- 请求 ID（`X-Request-Id`）
- 错误码（`error_code`）

**日志示例（正确）**：

```
[INFO] 创建租户成功: tenant_id=tn_xyz, account_id=acc_abc, key_id=key_123
[ERROR] 认证失败: account_id=acc_abc, error_code=INVALID_CREDENTIAL, request_id=REQ-123
```

**日志示例（错误）**：

```
[ERROR] 认证失败: client_secret=secret_abc123, token=Bearer eyJhbGc...
```

### 密钥轮换影响

#### OAuth 管理凭据轮换

**触发方式**：在 Stor 管理页面重新生成 `client_secret`

**影响**：
- 旧 `client_secret` **立即失效**
- 所有使用旧 `client_secret` 颁发的 access token **立即失效**
- 所有使用旧 `client_secret` 颁发的 refresh token **立即失效**

**应对措施**：
- 更新所有服务中存储的 `client_secret`
- 使用新 `client_secret` 重新获取 access token
- 建议在低峰期进行轮换，避免业务中断

#### 租户 API Key 轮换

**触发方式**：通过管理接口撤销旧 Key，颁发新 Key

```http
POST /api/virtual-storage/v1/accounts/{account_id}/tenants/{tenant_id}/keys
Authorization: Bearer <access_token>
Idempotency-Key: <unique_key>
```

**影响**：
- 撤销的 Key **立即失效**
- 同一租户的其他 Key 不受影响
- 租户可以同时拥有多个有效 Key

**应对措施**：
- 先颁发新 Key 并在服务中部署
- 确认新 Key 正常工作后，再撤销旧 Key
- 避免同时撤销所有 Key 导致服务中断

## 9. 常见问题排查

### Q1: 收到 500 错误但没有详细信息

**可能原因**：
- 旧版本 API 未捕获请求体解析异常
- 更新到最新版本后，会返回明确的 `422 VALIDATION_ERROR`

**排查步骤**：
1. 确认是否提供了 `X-Request-Id`，检查服务端日志
2. 验证请求体是否为有效 JSON
3. 验证所有必填字段是否存在且类型正确

### Q2: 创建租户返回 409 QUOTA_EXCEEDED

**可能原因**：
- 账号可用配额不足
- 其他并发请求已消耗配额

**排查步骤**：
1. 查询账号配额：`GET /api/virtual-storage/v1/accounts/{account_id}`
2. 检查 `quota_bytes_reserved` 和 `quota_objects_reserved`
3. 如需增加配额，联系管理员在 Stor 页面调整

### Q3: 相同幂等键重试返回 409 IDEMPOTENCY_MISMATCH

**可能原因**：
- 重试时修改了请求体参数

**解决方法**：
- 如果参数确实需要修改，使用新的幂等键
- 如果参数相同，检查 JSON 序列化是否一致（字段顺序可以不同）

### Q4: 创建租户后无法使用租户 API Key

**可能原因**：
- 使用了管理凭据的 access token 而非租户 Key
- 租户 ID 错误

**解决方法**：
- 确认使用 `X-API-Key` 请求头，而非 `Authorization`
- 确认租户 ID 拼写正确
- 确认 Key 未被撤销

### Q5: Token 过期或认证失败

**可能原因**：
- Access token 过期（默认 3600 秒后）
- Refresh token 过期（默认 7 天后）
- 账号已禁用或密钥已轮换

**解决方法**：
- **不要盲目重试 401 错误**：先区分是 token 过期还是账号禁用
- Token 过期时使用 refresh token 刷新，获取新的 access token
- Refresh token 也过期时，使用 `client_id` 和 `client_secret` 重新获取
- 如果密钥已轮换，所有旧 token 立即失效，需使用新密钥重新认证
- 账号禁用时不应重试，应通知管理员

## 10. API 调用最佳实践

1. **使用幂等键**：所有创建操作都应提供幂等键，避免重复创建
2. **提供请求 ID**：在请求头中提供 `X-Request-Id`，便于日志关联和问题排查
3. **错误重试**：
   - **写操作（如创建租户）**：
     - 超时或 5xx 错误时，**结果可能已生效**
     - 使用**相同幂等键 + 相同参数**重试，系统会返回已创建的结果
     - 如果参数需要变更，**必须使用新的幂等键**
     - 使用指数退避策略（如 1s, 2s, 4s, 8s），避免雷击效应
   - **读操作（如查询配额）**：超时或 5xx 可直接重试
   - **4xx 错误**：
     - `422` 验证失败：修正参数后使用**新的幂等键**重试
     - `409` 冲突（配额不足、名称冲突、幂等键冲突）：根据具体 `error_code` 处理，不应盲目重试
     - `401` 认证失败：先判断是 token 过期还是账号禁用，避免无限刷新
4. **密钥管理**：
   - 使用环境变量或密钥管理服务存储 `client_secret`
   - 定期轮换密钥，记录轮换时间
   - 在日志中脱敏所有敏感信息（token、密钥、API Key）
5. **配额监控**：
   - 定期查询账号配额使用情况
   - 在配额不足时提前告警
6. **Token 刷新**：
   - 在 access token 过期前主动刷新
   - 收到 `401` 错误时，先判断原因，不要盲目刷新
7. **并发控制**：
   - 使用幂等键保证并发安全
   - 避免同时发起大量创建租户请求

## 11. 相关文档

- [OAuth APPS 认证说明](./OAUTH_APPS.md)
- [虚拟存储 API 契约](./API_CONTRACT.md)
- [ADR-001: 虚拟存储设计决策](./ADR-001.md)
