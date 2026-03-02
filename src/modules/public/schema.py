from pydantic import BaseModel
from typing import Any, List
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


class RegionCreateRequest(BaseModel):
  name: str
  nickname: str

class RegionResponse(BaseModel):
  id: PydanticObjectId
  name: str
  nickname: str

class RegionListResponse(BaseModel):
  data: List[RegionResponse]

class ApplicationCreateRequest(BaseModel):
  name: str
  description: str
  regions: List[PydanticObjectId]

class SimpleUserResponse(BaseModel):
  id: PydanticObjectId
  username: str
  name: str

class ApplicationResponse(BaseModel):
  id: PydanticObjectId
  name: str
  created_at: datetime
  updated_at: datetime
  description: str
  enabled: bool
  enabled_at: datetime | None
  author: SimpleUserResponse
  regions: List[RegionResponse]

class ApplicationListResponse(BaseModel):
  data: List[ApplicationResponse]