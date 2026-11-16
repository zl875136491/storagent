# APPS 机器认证说明

这份说明对应仓库里的实现。测试环境尚未切换，当前已部署的测试机仍使用固定管理凭据。

`client_id` 是虚拟账号的公开 account id，也是 `/api/virtual-storage/v1/accounts/{account_id}` 的路径参数。`client_secret` 只用于：

- `POST /oauth2/token`，`grant_type=client_credentials`，HTTP Basic
- `POST /oauth2/refresh`，`grant_type=refresh_token`，HTTP Basic，同时提交 refresh token

access token 1 小时。refresh token 7 天，每次刷新都会更换。管理接口的 `Authorization: Bearer` 只能放 access token。

密钥可以在 Stor 管理页面重新生成。APPS 没有重新生成接口。重新生成后，旧密钥、旧 access token 和旧 refresh token 立即失效。

虚拟账号不能登录 Stor，也没有普通用户权限。
