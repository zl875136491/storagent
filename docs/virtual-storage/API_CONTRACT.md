# 虚拟存储 API 契约

测试入口：`https://<host>:8443`。进程只监听 TLS。

除 `/healthz` 外，明文 HTTP 返回 `HTTPS_REQUIRED`。旧 `/api/v1` 与 `/api/v2` 不在这个进程里。

## 管理面

`Authorization: Bearer <管理凭据>`

- `GET /api/virtual-storage/v1/accounts/{app_name}`
- `POST /api/virtual-storage/v1/accounts/{app_name}/tenants`
  - 头 `Idempotency-Key`
  - 体：`external_tenant_id`、`logical_name`、`quota_bytes`、`quota_objects`、可选 `key_ttl_seconds`
  - 窗口内重放返回 `api_key`；窗口外只有 `key_id` 和 `plaintext_available=false`
- `GET /api/virtual-storage/v1/accounts/{app_name}/tenants`
- `GET /api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}`
- `PATCH /api/virtual-storage/v1/accounts/{app_name}/tenants/{tenant_id}`，头 `If-Match: <revision>`
- `POST .../freeze`、`.../unfreeze`、`.../retire`、`.../cancel-retire`
- `GET /api/virtual-storage/v1/accounts/{app_name}/operations/{operation_id}`

## 数据面

`X-API-Key: <租户 Key>`。管理凭据不能当租户 Key。

- `POST /api/virtual-storage/v1/tenants/{tenant_id}/uploads`
- `PUT .../uploads/{upload_id}/parts/{part_number}`，正文是分片字节
- `GET .../uploads/{upload_id}`
- `POST .../uploads/{upload_id}/complete` 返回 202。完成分片以服务端清单为准
- `DELETE .../uploads/{upload_id}`
- `GET .../objects?prefix=`
- `GET .../objects/{object_id}/content`，支持单一 `Range: bytes=start-end`
- `DELETE .../objects/{object_id}` 软删除
- `POST .../objects/{object_id}/restore`

非归属区域把 `/api/virtual-storage/` 和 `/console/` 下的写请求转发到 `VS_HOME_BASE_URL`。`/ready` 仍表示本进程。

## 控制台

`GET /console`。`POST /console/session` 校验管理凭据。没有配置 OIDC。
