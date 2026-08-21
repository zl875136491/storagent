from pydantic import BaseModel
from typing import Any, List, Literal
from bson import ObjectId
from pydantic_core import CoreSchema, core_schema
from pydantic import GetCoreSchemaHandler, BaseModel, Field
from src.core.exception import CustomException, ErrorDesc
from datetime import datetime

class PydanticObjectId(ObjectId):
  """
  自定义 ObjectId 类型，继承自 bson.ObjectId。
  用于告诉 Pydantic 在校验时如何处理传入的数据，并配置序列化行为。
  """

  @classmethod
  def __get_pydantic_core_schema__(
      cls, source_type: Any, handler: GetCoreSchemaHandler
  ) -> CoreSchema:
    """
    Pydantic v2 的核心校验器方法。
    1. 尝试将输入转换为 ObjectId (如果输入是字符串或字节)。
    2. 确保输出的序列化类型是 'str'。
    """
    return core_schema.json_or_python_schema(
      # Python端校验：支持从 str, bytes, 或 ObjectId 本身进行校验
      python_schema=core_schema.with_info_plain_validator_function(cls.validate_object_id),
      # JSON端校验：JSON 始终是 string
      json_schema=core_schema.str_schema(),
      # 配置序列化：将 ObjectId 实例序列化为 str
      serialization=core_schema.plain_serializer_function_ser_schema(
        lambda instance: str(instance), 
        info_arg=False, 
        return_schema=core_schema.str_schema()
      ),
    )

  @classmethod
  def validate_object_id(cls, value: Any, handler: core_schema.ValidationInfo) -> ObjectId:
    if isinstance(value, ObjectId):
      return value
    if isinstance(value, str):
      try:
        return ObjectId(value)
      except Exception as e:
        raise CustomException(ErrorDesc.OBJECT_ID_NOT_VALID, f"表单传入对象ID: {value} 不合法")
    try:
      return ObjectId(value)
    except Exception:
      raise CustomException(ErrorDesc.OBJECT_ID_NOT_VALID, f"请求传入对象ID: {value} 不合法")

class SimpleMessageResponse(BaseModel):
  message: str


class RegionCreateRequest(BaseModel):
  name: str
  shown_name: str

class RegionResponse(BaseModel):
  id: PydanticObjectId
  name: str
  shown_name: str

class RegionListResponse(BaseModel):
  data: List[RegionResponse]

class ApplicationCreateRequest(BaseModel):
  name: str
  shown_name: str
  description: str
  domains: list[str] = Field(default_factory=list)

class SimpleUserResponse(BaseModel):
  id: PydanticObjectId
  username: str
  name: str

class ApplicationResponse(BaseModel):
  id: PydanticObjectId
  name: str
  shown_name: str
  created_at: datetime
  updated_at: datetime
  description: str
  enabled: bool
  enabled_at: datetime | None
  provisioning_status: Literal["pending", "provisioning", "ready", "failed", "degraded"]
  provisioning_error: str
  provisioning_updated_at: datetime | None
  quota_bytes: int = Field(gt=0)
  quota_usage_bytes: int = Field(default=0, ge=0)
  quota_usage_ratio: float = Field(default=0.0, ge=0)
  quota_usage_updated_at: datetime | None = None
  domains: list[str] = Field(default_factory=list)
  author: SimpleUserResponse


class ApplicationDomainRequest(BaseModel):
  domain: str = Field(..., min_length=1, max_length=256)

class ApplicationListResponse(BaseModel):
  data: List[ApplicationResponse]


class ApplicationQuotaUpdateRequest(BaseModel):
  quota_bytes: int = Field(..., gt=0)


class QuotaAlertRuleResponse(BaseModel):
  low_percent: int
  medium_percent: int
  high_percent: int
  block_percent: int
  message_template: str
  updated_at: datetime
  updated_by: str


class QuotaAlertRuleUpdateRequest(BaseModel):
  low_percent: int = Field(70, ge=1, le=100)
  medium_percent: int = Field(85, ge=1, le=100)
  high_percent: int = Field(90, ge=1, le=100)
  block_percent: int = Field(100, ge=1, le=100)
  message_template: str = Field(..., min_length=1, max_length=1000)


class ExpansionRequestCreate(BaseModel):
  reason: str = Field(..., min_length=1, max_length=2000)
  add_size_bytes: int = Field(..., gt=0)


class ExpansionRequestReview(BaseModel):
  approved: bool
  review_note: str = Field(default="", max_length=1000)


class ExpansionRequestResponse(BaseModel):
  id: str
  application_name: str
  application_shown_name: str
  applicant_username: str
  reason: str
  add_size_bytes: int
  status: Literal["pending", "approved", "rejected"]
  reviewer_username: str
  review_note: str
  created_at: datetime
  reviewed_at: datetime | None


class ExpansionRequestListResponse(BaseModel):
  data: List[ExpansionRequestResponse]

class SimpleApplicationResponse(BaseModel):
  id: PydanticObjectId
  name: str
  shown_name: str

class SimpleApplicationListResponse(BaseModel):
  data: List[SimpleApplicationResponse]

class APIKeyCreateRequest(BaseModel):
  application_id: PydanticObjectId
  expired_at: datetime | None = None

class APIKeyResponse(BaseModel):
  id: PydanticObjectId
  key: str
  expired_at: datetime

class APIKeyDetailResponse(BaseModel):
  id: PydanticObjectId
  key: str
  application: SimpleApplicationResponse
  expired_at: datetime
  deleted: bool = False
  destory_by_admin: bool = False

class APIKeyListResponse(BaseModel):
  data: List[APIKeyDetailResponse]

class Endpoint(BaseModel):
  region_id: PydanticObjectId
  server_id: PydanticObjectId
  name: str
  shown_name: str
  master: bool
  domain: str = Field(default="", description="对外 Nginx 网关域名")
  endpoint: str
  minio_endpoint: str

class EndpointsResponse(BaseModel):
  data: List[Endpoint]
