# 虚拟存储 API 契约

测试入口：`https://<host>:8443`。进程只监听 TLS。

本文描述仓库中的目标行为。测试环境在确认上线前仍使用固定管理凭据，以《APPS-测试环境接入指南》为准。

除 `/healthz` 外，明文 HTTP 返回 `HTTPS_REQUIRED`。旧 `/api/v1` 与 `/api/v2` 不在这个进程里。

机器管理接口只接受下面 `/oauth2/token` 或 `/oauth2/refresh` 签发的 access token。`client_secret` 不能放进表单，也不能再当作管理接口的 Bearer。

## 机器认证

实现在虚拟存储进程，不在人的 `/api/v1/auth`。

`POST /oauth2/token`

```http
Authorization: Basic base64(urlencode(client_id):urlencode(client_secret))
Content-Type: application/x-www-form-urlencoded

grant_type=client_credentials
```

成功时返回 `access_token`、`refresh_token`、`token_type`、`expires_in`（3600）、`scope`。不接受表单里的 `client_id` 或 `client_secret`。

`POST /oauth2/refresh`

```http
Authorization: Basic base64(urlencode(client_id):urlencode(client_secret))
Content-Type: application/x-www-form-urlencoded

grant_type=refresh_token&refresh_token=<refresh_token>
```

每次刷新都换发新的 refresh token，旧的立即失效。refresh token 有效期 7 天。密钥在 Stor 管理页面重新生成后，已发出的 access token 和 refresh token 立即失效。重新生成接口不提供给 APPS 调用。

非归属区域把这两个路径转发到归属区域，自己不签发。

## 管理面

`Authorization: Bearer <access_token>`。路径中的账号必须等于 token 的 `sub`。

- `GET /api/virtual-storage/v1/accounts/{account_id}`
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
