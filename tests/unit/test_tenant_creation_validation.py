"""
虚拟存储租户创建请求体校验的单元测试。
不依赖 MongoDB/MinIO，使用 mock 验证校验逻辑和错误响应。
"""
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.virtual_storage.api.routes import router, install_error_handler


@pytest.fixture
def app():
  """创建测试应用，不连接真实基础设施。"""
  from starlette.middleware.errors import ServerErrorMiddleware
  test_app = FastAPI()
  test_app.include_router(router)
  install_error_handler(test_app)
  test_app.add_middleware(ServerErrorMiddleware, debug=False)
  return test_app


@pytest.fixture
async def client(app):
  """创建测试客户端。"""
  transport = ASGITransport(app=app)
  async with AsyncClient(transport=transport, base_url="http://test") as ac:
    yield ac


def _mock_account():
  """创建 mock 账号对象。"""
  account = MagicMock()
  account.account_id = f"acc_{uuid.uuid4().hex[:8]}"
  account.app_name = f"app_{uuid.uuid4().hex[:8]}"
  account.quota_bytes = 100_000_000
  account.quota_objects = 1000
  account.quota_bytes_reserved = 0
  account.quota_objects_reserved = 0
  return account


@pytest.mark.asyncio
async def test_missing_quota_objects_returns_422(client):
  """缺少 quota_objects 字段应返回 422，不调用业务逻辑。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
      },
    )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "quota_objects" in body["message"]
  assert "X-Request-Id" in response.headers
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_missing_quota_bytes_returns_422(client):
  """缺少 quota_bytes 字段应返回 422，不调用业务逻辑。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "quota_bytes" in body["message"]
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_missing_external_tenant_id_returns_422(client):
  """缺少 external_tenant_id 字段应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "external_tenant_id" in body["message"]
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_missing_logical_name_returns_422(client):
  """缺少 logical_name 字段应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "logical_name" in body["message"]
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_quota_bytes_boolean_rejected(client):
  """quota_bytes 为布尔值应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": True,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_BYTES"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_quota_objects_boolean_rejected(client):
  """quota_objects 为布尔值应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": False,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_OBJECTS"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_quota_bytes_string_rejected(client):
  """quota_bytes 为字符串应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": "10000",
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_BYTES"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_empty_external_tenant_id_returns_422(client):
  """external_tenant_id 为空字符串应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_EXTERNAL_TENANT_ID"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_whitespace_external_tenant_id_returns_422(client):
  """external_tenant_id 仅空白应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "   ",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_EXTERNAL_TENANT_ID"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_empty_logical_name_returns_422(client):
  """logical_name 为空字符串应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_LOGICAL_NAME"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_invalid_json_returns_422(client):
  """无效的 JSON 应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"):
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={
        "Authorization": "Bearer mock",
        "Idempotency-Key": "test_idem",
        "Content-Type": "application/json",
      },
      content=b"not valid json{",
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_JSON"


@pytest.mark.asyncio
async def test_json_array_rejected(client):
  """JSON 数组应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json=["not", "an", "object"],
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] == "INVALID_REQUEST_BODY"
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_json_null_rejected(client):
  """JSON null 应返回 422。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={
        "Authorization": "Bearer mock",
        "Idempotency-Key": "test_idem",
        "Content-Type": "application/json",
      },
      content=b"null",
    )
  
  assert response.status_code == 422
  body = response.json()
  assert body["error_code"] in ["INVALID_JSON", "INVALID_REQUEST_BODY"]
  mock_create.assert_not_called()


@pytest.mark.asyncio
async def test_request_id_generated_when_missing(client):
  """缺少请求 ID 时应自动生成。"""
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
      },
    )
  
  assert response.status_code == 422
  assert "X-Request-Id" in response.headers
  request_id = response.headers["X-Request-Id"]
  assert request_id.startswith("REQ-")
  assert len(request_id) > 4


@pytest.mark.asyncio
async def test_request_id_echoed_on_error(client):
  """错误响应应回显请求 ID。"""
  custom_id = "REQ-TEST-12345678"
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={
        "Authorization": "Bearer mock",
        "Idempotency-Key": "test_idem",
        "X-Request-Id": custom_id,
      },
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
      },
    )
  
  assert response.status_code == 422
  assert response.headers["X-Request-Id"] == custom_id


@pytest.mark.asyncio
async def test_request_id_malicious_input_sanitized(client):
  """恶意请求 ID 应被过滤。"""
  malicious_id = "REQ\x00\x01\x02\n\r<script>alert('xss')</script>" + "A" * 200
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create:
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={
        "Authorization": "Bearer mock",
        "Idempotency-Key": "test_idem",
        "X-Request-Id": malicious_id,
      },
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
      },
    )
  
  assert response.status_code == 422
  request_id = response.headers["X-Request-Id"]
  assert "\x00" not in request_id
  assert "\n" not in request_id
  assert "\r" not in request_id
  assert len(request_id) <= 64


@pytest.mark.asyncio
async def test_valid_request_calls_business_logic(client):
  """合法请求应调用业务逻辑。"""
  mock_result = {
    "tenant_id": "tn_test123",
    "api_key": "vst_test_key",
    "plaintext_available": True,
  }
  
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock, return_value=mock_result) as mock_create, \
       patch("src.virtual_storage.workers.queue.enqueue_scan"):
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 200
  body = response.json()
  assert body["tenant_id"] == "tn_test123"
  assert body["api_key"] == "vst_test_key"
  assert "X-Request-Id" in response.headers
  
  mock_create.assert_called_once()
  call_kwargs = mock_create.call_args.kwargs
  assert call_kwargs["external_tenant_id"] == "ext_123"
  assert call_kwargs["logical_name"] == "test_name"
  assert call_kwargs["quota_bytes"] == 10000
  assert call_kwargs["quota_objects"] == 100


@pytest.mark.asyncio
async def test_request_id_on_success(client):
  """成功响应应包含请求 ID。"""
  mock_result = {"tenant_id": "tn_test", "api_key": "vst_key", "plaintext_available": True}
  custom_id = "REQ-SUCCESS-TEST"
  
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock, return_value=mock_result), \
       patch("src.virtual_storage.workers.queue.enqueue_scan"):
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={
        "Authorization": "Bearer mock",
        "Idempotency-Key": "test_idem",
        "X-Request-Id": custom_id,
      },
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 200
  assert response.headers["X-Request-Id"] == custom_id


@pytest.mark.asyncio
async def test_quota_exceeded_returns_409_not_422(client):
  """配额超限应返回 409 而非 422。"""
  from src.virtual_storage.core.exception import QuotaExceededError
  
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create, \
       patch("src.virtual_storage.workers.queue.enqueue_scan"):
    mock_create.side_effect = QuotaExceededError("配额不足")
    
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={"Authorization": "Bearer mock", "Idempotency-Key": "test_idem"},
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 409
  body = response.json()
  assert body["error_code"] == "QUOTA_EXCEEDED"
  assert "X-Request-Id" in response.headers


@pytest.mark.asyncio
async def test_conflict_error_returns_409_with_request_id(client):
  """冲突错误应返回 409 并包含请求 ID。"""
  from src.virtual_storage.core.exception import ConflictError
  
  with patch("src.virtual_storage.api.routes._bearer", return_value="mock_token"), \
       patch("src.virtual_storage.api.routes.create_tenant", new_callable=AsyncMock) as mock_create, \
       patch("src.virtual_storage.workers.queue.enqueue_scan"):
    mock_create.side_effect = ConflictError("租户已存在", "TENANT_EXISTS")
    
    response = await client.post(
      "/api/virtual-storage/v1/accounts/test_app/tenants",
      headers={
        "Authorization": "Bearer mock",
        "Idempotency-Key": "test_idem",
        "X-Request-Id": "REQ-CONFLICT-TEST",
      },
      json={
        "external_tenant_id": "ext_123",
        "logical_name": "test_name",
        "quota_bytes": 10000,
        "quota_objects": 100,
      },
    )
  
  assert response.status_code == 409
  body = response.json()
  assert body["error_code"] == "TENANT_EXISTS"
  assert "租户已存在" in body["message"]
  assert response.headers["X-Request-Id"] == "REQ-CONFLICT-TEST"
