"""测试虚拟存储租户创建请求体校验。"""
import uuid


def _name(prefix: str) -> str:
  return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _account(client, quota_bytes=100_000, quota_objects=200):
  app_name = _name("app")
  credential = "mgmt-" + uuid.uuid4().hex
  response = client.post("/internal/drive", json={
    "op": "account",
    "app_name": app_name,
    "credential": credential,
    "quota_bytes": quota_bytes,
    "quota_objects": quota_objects,
  })
  assert response.status_code == 200, response.text
  import base64
  basic = base64.b64encode(f"{app_name}:{credential}".encode()).decode()
  token_response = client.post(
    "/oauth2/token",
    headers={"Authorization": f"Basic {basic}"},
    data={"grant_type": "client_credentials"},
  )
  assert token_response.status_code == 200, token_response.text
  token = token_response.json()["access_token"]
  return app_name, token


def test_missing_quota_objects_returns_422(client):
  """缺少 quota_objects 字段应返回 422 而非 500。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "quota_objects" in body["message"]


def test_missing_quota_bytes_returns_422(client):
  """缺少 quota_bytes 字段应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "quota_bytes" in body["message"]


def test_missing_external_tenant_id_returns_422(client):
  """缺少 external_tenant_id 字段应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "external_tenant_id" in body["message"]


def test_missing_logical_name_returns_422(client):
  """缺少 logical_name 字段应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "quota_bytes": 10000,
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "MISSING_REQUIRED_FIELDS"
  assert "logical_name" in body["message"]


def test_invalid_quota_bytes_type_returns_422(client):
  """quota_bytes 类型错误应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": "not_a_number",
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_BYTES"


def test_invalid_quota_objects_type_returns_422(client):
  """quota_objects 类型错误应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": "not_a_number",
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_OBJECTS"


def test_negative_quota_bytes_returns_422(client):
  """quota_bytes 为负数应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": -1000,
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_BYTES"


def test_negative_quota_objects_returns_422(client):
  """quota_objects 为负数应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": -100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_QUOTA_OBJECTS"


def test_empty_external_tenant_id_returns_422(client):
  """external_tenant_id 为空字符串应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": "",
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_EXTERNAL_TENANT_ID"


def test_empty_logical_name_returns_422(client):
  """logical_name 为空字符串应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": "",
      "quota_bytes": 10000,
      "quota_objects": 100,
    },
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_LOGICAL_NAME"


def test_invalid_json_returns_422(client):
  """无效的 JSON 应返回 422。"""
  app_name, token = _account(client)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={
      "Authorization": f"Bearer {token}",
      "Idempotency-Key": _name("idem"),
      "Content-Type": "application/json",
    },
    content=b"not valid json{",
  )
  
  assert response.status_code == 422, f"Expected 422, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "INVALID_JSON"


def test_valid_request_succeeds(client):
  """合法请求应成功创建租户。"""
  app_name, token = _account(client, quota_bytes=100_000, quota_objects=200)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": 50,
    },
  )
  
  assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
  body = response.json()
  assert "tenant_id" in body
  assert "api_key" in body
  assert body["plaintext_available"] is True


def test_quota_exceeds_account_limit_returns_409(client):
  """超出账号配额应返回 409 而非创建租户。"""
  app_name, token = _account(client, quota_bytes=10_000, quota_objects=20)
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": _name("idem")},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 20_000,
      "quota_objects": 10,
    },
  )
  
  assert response.status_code == 409, f"Expected 409, got {response.status_code}: {response.text}"
  body = response.json()
  assert body["error_code"] == "QUOTA_EXCEEDED"


def test_idempotent_replay_succeeds(client):
  """相同幂等键和参数的重放应成功返回相同结果。"""
  app_name, token = _account(client, quota_bytes=100_000, quota_objects=200)
  idem_key = _name("idem")
  payload = {
    "external_tenant_id": _name("ext"),
    "logical_name": _name("name"),
    "quota_bytes": 10000,
    "quota_objects": 50,
  }
  
  response1 = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": idem_key},
    json=payload,
  )
  assert response1.status_code == 200, f"First request failed: {response1.text}"
  body1 = response1.json()
  
  response2 = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": idem_key},
    json=payload,
  )
  assert response2.status_code == 200, f"Idempotent replay failed: {response2.text}"
  body2 = response2.json()
  
  assert body1["tenant_id"] == body2["tenant_id"]
  assert body1["api_key"] == body2["api_key"]


def test_idempotent_key_different_body_returns_409(client):
  """相同幂等键但不同参数应返回 409。"""
  app_name, token = _account(client, quota_bytes=100_000, quota_objects=200)
  idem_key = _name("idem")
  
  response1 = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": idem_key},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": 50,
    },
  )
  assert response1.status_code == 200, f"First request failed: {response1.text}"
  
  response2 = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={"Authorization": f"Bearer {token}", "Idempotency-Key": idem_key},
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 20000,
      "quota_objects": 100,
    },
  )
  assert response2.status_code == 409, f"Expected 409, got {response2.status_code}: {response2.text}"
  body = response2.json()
  assert body["error_code"] == "IDEMPOTENCY_MISMATCH"


def test_request_id_in_response_header(client):
  """错误和成功响应应包含请求 ID。"""
  app_name, token = _account(client)
  request_id = f"REQ-TEST-{uuid.uuid4().hex[:8]}"
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={
      "Authorization": f"Bearer {token}",
      "Idempotency-Key": _name("idem"),
      "X-Request-Id": request_id,
    },
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
    },
  )
  
  assert response.status_code == 422
  assert response.headers.get("X-Request-Id") == request_id


def test_request_id_in_success_response(client):
  """成功响应应包含请求 ID。"""
  app_name, token = _account(client, quota_bytes=100_000, quota_objects=200)
  request_id = f"REQ-TEST-{uuid.uuid4().hex[:8]}"
  
  response = client.post(
    f"/api/virtual-storage/v1/accounts/{app_name}/tenants",
    headers={
      "Authorization": f"Bearer {token}",
      "Idempotency-Key": _name("idem"),
      "X-Request-Id": request_id,
    },
    json={
      "external_tenant_id": _name("ext"),
      "logical_name": _name("name"),
      "quota_bytes": 10000,
      "quota_objects": 50,
    },
  )
  
  assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
  assert response.headers.get("X-Request-Id") == request_id
