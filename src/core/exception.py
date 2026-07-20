import traceback
from enum import Enum
from loguru import logger
from typing import Generic, TypeVar
from pydantic import BaseModel, Field
from fastapi.responses import JSONResponse
from typing import Any, Optional, Callable
from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from starlette.exceptions import HTTPException
from fastapi.exceptions import RequestValidationError
from starlette.middleware.base import BaseHTTPMiddleware

from src.configs.configs import settings

class ErrorDesc(Enum):
  """
  错误详情枚举
  """
  # authentication
  LOGIN_ERR = ("登录失败", 401001)
  PASSWORD_UNSET = ("密码未设置", 401002)
  USER_NOT_FOUND = ("用户不存在", 400002)
  CREATE_USER_FAILED = ("创建用户失败", 400003)
  USER_PERMISSIONS_ERR = ("用户权限不存在", 403004)
  ADMIN_ROLE_EXCLUSIVE = ("管理员角色已存在", 400005)
  BASIC_ROLE_EXCLUSIVE = ("基础用户角色已存在", 400006)
  CREDENTIALS_NOT_VALID = ("无法验证凭据", 401007)
  REFRESH_TOKEN_NOT_VALID = ("无法验证刷新凭据", 402008)
  TOKEN_DESTROYED = ("凭据已被强制失效", 402009)
  INSUFFICIENT_PERMISSIONS = ("用户没有所需的权限", 403008)
  
  # page / path / args
  PAGE_NUM_NOT_VALID = ("页码不合法", 400009)
  PAGE_SIZE_NOT_VALID = ("页大小不合法", 400010)
  OBJECT_ID_NOT_VALID = ("错误的对象ID", 400011)
  
  # resource
  NAME_EXISTED = ("名称已存在", 400012)
  RES_ALREADY_EXISTS = ("资源已存在", 400013)
  RES_NOT_FOUND = ("数据不存在", 400013)
  RES_DATA_NOT_CHANGED = ("资源数据未改变", 400014)
  RES_NOT_BELONG_TO_USER = ("该资源不属于当前用户", 400015)
  DB_CONN_FAILED = ("数据库连接失败", 500016)
  DB_ERROR = ("数据库错误", 500017)
  DB_UPDATE_FAILED = ("数据库更新失败", 500018)
  
  # minio
  MINIO_CONN_FAILED = ("Minio 连接失败", 400018)
  MINIO_ACCESS_FAILED = ("Minio 访问失败", 400019)
  MINIO_ALIAS_FAILED = ("Minio 别名设置失败", 400020)
  MINIO_REPLICATE_FAILED = ("Minio 复制集设置失败", 400021)
  MINIO_CREATE_BUCKET_FAILED = ("Minio 创建存储桶失败", 400022)
  MINIO_ENABLE_VERSIONING_FAILED = ("Minio 开启版本控制失败", 400023)
  MINIO_CREATE_SERVER_FAILED = ("Minio 创建服务器失败", 400024)
  
  # special resource
  STATUS_ERR = ("状态错误", 400020)
  INVALID_RULE_PARAMS = ("规则参数不合法", 400021)
  INVALID_PARAMS = ("参数不合法", 400023)
  OPERATION_NOT_ALLOWED = ("操作不允许", 400024)
  FIELD_NOT_FOUND = ("字段不存在", 400025)
  SEND_CODE_FAILED = ("发送登录代码失败", 400026)
  REGION_NOT_DEF = ("地区信息未定义, 请通过环境变量设置", 400027)
  REGION_EXISTED = ("地区信息已存在", 400028)
  
  # API resource
  API_KEY_INVALID = ("API-KEY 无效", 400029)
  API_KEY_EXPIRED = ("API-KEY 已过期", 400030)
  APP_NOT_ENABLED = ("应用未启用", 400031)
  
  # file
  OBJECT_NOT_FOUND = ("对象不存在", 404033)
  OBJECT_NOT_FOUND_LOCAL = ("对象在本节点不存在", 404032)
  INTERNAL_SERVER_ERROR = ("服务器内部错误", 500001)
  

  
  @property
  def code(self) -> int:
    return self.value[1]
  
  @property
  def message(self) -> str:
    return self.value[0]

# 定义泛型类型
T = TypeVar("T")

class ApiResponse(BaseModel, Generic[T]):
  """
  统一 API 响应模型

  所有 API 接口都应返回此结构，以保证前端处理逻辑的一致性。
  """

  msg: str = Field(default="Success", description="响应消息")
  data: Optional[T] = Field(default=None, description="响应数据")
  code: Optional[int] = Field(default=None, description="业务错误码（成功时可为空）")
    
class CustomException(HTTPException):
  """
  自定义基础异常类
  """

  def __init__(self, msg: str | ErrorDesc, reason: Any = None):
    if isinstance(msg, ErrorDesc):
      self.message = msg.message
      status_code = msg.code // 1000 # 高3位为状态码, 低3位为错误识别码
      self.code = msg.code
    else:
      status_code = 400
      self.message = msg
      self.code = 400000
    if reason in ["", None]:
      self.reason = "无详细描述"
    else:
      self.reason = reason
    super().__init__(status_code=status_code, detail=self.reason)
      
  def __str__(self):
    return self.message + ": " + str(self.reason)

def error_response(msg: str, data: Optional[Any] = None, code: Optional[int] = None) -> dict:
  """
  生成一个失败的 API 响应（含稳定业务 code，便于前端契约解析）
  """
  return {"msg": msg, "data": data, "code": code}

async def custom_exception_handler(request: Request, exc: CustomException):
  """捕获自定义的业务异常"""
  log_message = (
    f"[异常]: Message: {exc.message} | "
    f"URL: ({request.method}) {request.url} | Client IP: {request.client.host} | "
    f"Code: {exc.code} | Detail: {exc.reason}"
  )
  logger.error(log_message)
  return JSONResponse(
    status_code=exc.status_code,
    content=jsonable_encoder(
      error_response(msg=exc.message, data=exc.reason, code=exc.code)
    ),
  )

async def validation_exception_handler(request: Request, exc: RequestValidationError):
  """
  捕获并格式化 Pydantic 的请求体验证异常
  """
  errors = []
  for error in exc.errors():
    field = ".".join(str(loc) for loc in error["loc"])
    errors.append({"field": field, "message": error["msg"]})

  log_message = (
    f"捕获到请求参数验证错误 | "
    f"Method: {request.method} | URL: {request.url} | Client IP: {request.client.host} | "
    f"Errors: {errors}"
  )
  logger.warning(log_message)

  return JSONResponse(
    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
    content=jsonable_encoder(
      error_response(msg="请求参数验证失败", data={"errors": errors})
    ),
  )

async def all_exception_handler(request: Request, exc: Exception):
  """
  捕获所有未被处理的异常（兜底异常处理器）
  
  根据 settings.DEBUG 标志决定是否在响应中包含堆栈信息：
  - DEBUG=True: 返回详细的错误信息和堆栈跟踪，便于调试
  - DEBUG=False: 只返回通用错误信息，不暴露内部实现细节
  """
  log_message = (
    f"捕获到未处理的全局异常: {exc} | "
    f"Method: {request.method} | URL: {request.url} | Client IP: {request.client.host}"
  )
  # 使用 logger.exception 可以自动记录完整的堆栈信息到日志
  logger.exception(log_message)
  
  # 根据 DEBUG 标志决定响应内容
  if settings.DEBUG:
    # Debug 模式：返回详细的错误信息和堆栈跟踪
    error_detail = {
      "type": type(exc).__name__,
      "reason": str(exc),
      "traceback": traceback.format_exc().split("\n")
    }
    return JSONResponse(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      content=jsonable_encoder(
        error_response(msg="服务器内部错误", data=error_detail, code=500001)
      ),
    )
  else:
    # 生产模式：只返回通用错误信息，不暴露堆栈信息
    error_detail = {
      "type": type(exc).__name__,
      "reason": str(exc),
      "traceback": None
    }
    return JSONResponse(
      status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
      content=jsonable_encoder(error_response(msg="服务器内部错误", data=error_detail, code=500001)),
    )

class ExceptionHandlerMiddleware(BaseHTTPMiddleware):
  """
  自定义异常处理中间件
  
  这个中间件会捕获所有未被处理的异常，确保即使 FastAPI 的 debug=True，
  也能使用我们的自定义异常处理器来格式化错误响应。
  """
  async def dispatch(self, request: Request, call_next: Callable):
    try:
      response = await call_next(request)
      return response
    except CustomException as exc:
      # 自定义业务异常
      return await custom_exception_handler(request, exc)
    except RequestValidationError as exc:
      # 请求验证异常
      return await validation_exception_handler(request, exc)
    except Exception as exc:
      # 所有其他异常（兜底）
      return await all_exception_handler(request, exc)

def register_exception(app: FastAPI):
  """
  为 FastAPI 应用注册全局异常处理器。
  
  使用两种方式确保异常被正确捕获：
  1. 注册 FastAPI 的异常处理器（标准方式）
  2. 添加自定义中间件作为兜底（确保即使在 debug 模式下也能工作）
  
  中间件会在异常处理器之前捕获异常，确保我们的格式化响应被返回。
  """
  # 首先注册 FastAPI 的异常处理器（标准方式）
  app.add_exception_handler(CustomException, custom_exception_handler)
  app.add_exception_handler(RequestValidationError, validation_exception_handler)
  app.add_exception_handler(Exception, all_exception_handler)
  
  # 添加自定义异常处理中间件作为兜底
  # 这个中间件会在其他中间件之后、路由之前执行，确保捕获所有异常
  # 即使在 debug 模式下，也能返回我们格式化的错误响应
  app.add_middleware(ExceptionHandlerMiddleware)
