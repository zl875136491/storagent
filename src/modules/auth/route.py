from fastapi import APIRouter, Depends, Request
from src.modules.auth import service as auth_service
from src.modules.auth import schema as auth_schema
from src.core.auth import get_current_user, oauth2_scheme, require_admin
from src.core.rate_limit import (
  rate_limit_login,
  rate_limit_oa_request,
  rate_limit_oa_verify,
  rate_limit_refresh,
)
from src.modules.auth.model import User

router = APIRouter()

@router.post(
  path="/login",
  response_model=auth_schema.TokenResponse,
  summary="用户登录")
async def login(
  payload: auth_schema.LoginRequest,
  request: Request,
) -> auth_schema.TokenResponse:
  """
  用户登录
  """
  rate_limit_login(request)
  username = payload.username
  password = payload.password
  return await auth_service.login_user(username, password)


@router.post(
  path="/register/request",
  response_model=auth_schema.AuthRequestResponse,
  summary="通过 OA 发起新用户注册",
)
async def request_registration(
  payload: auth_schema.PasswordPairRequest,
  request: Request,
) -> auth_schema.AuthRequestResponse:
  rate_limit_oa_request(request, payload.username)
  return await auth_service.request_registration(payload.username, payload.password)


@router.post(
  path="/password-reset/request",
  response_model=auth_schema.AuthRequestResponse,
  summary="通过 OA 发起密码重置",
)
async def request_password_reset(
  payload: auth_schema.PasswordPairRequest,
  request: Request,
) -> auth_schema.AuthRequestResponse:
  rate_limit_oa_request(request, payload.username)
  return await auth_service.request_password_reset(payload.username, payload.password)


@router.post(
  path="/login-link/request",
  response_model=auth_schema.AuthRequestResponse,
  summary="发送 OA 快捷登录链接",
)
async def request_login_link(
  payload: auth_schema.AuthLinkRequest,
  request: Request,
) -> auth_schema.AuthRequestResponse:
  rate_limit_oa_request(request, payload.username)
  return await auth_service.request_login_link(payload.username)


@router.post(
  path="/login-by-code",
  response_model=auth_schema.TokenResponse,
  summary="使用 OA 一次性链接完成认证",
)
async def login_by_code(
  payload: auth_schema.CodeLoginRequest,
  request: Request,
) -> auth_schema.TokenResponse:
  rate_limit_oa_verify(request, payload.username)
  return await auth_service.login_by_code(payload.username, payload.code)

@router.post(
  path="/refresh",
  response_model=auth_schema.TokenResponse,
  summary="刷新 Token")
async def refresh_token(
  payload: auth_schema.RefreshTokenRequest,
  request: Request,
) -> auth_schema.TokenResponse:
  """
  使用 refresh token 获取新的 access token 和 refresh token
  """
  rate_limit_refresh(request)
  return await auth_service.refresh_token(payload.refresh_token.strip())

@router.get(
  path="/profile",
  response_model=auth_schema.UserProfileResponse,
  summary="获取用户个人信息")
async def get_user_profile(
  user: User = Depends(get_current_user)) -> auth_schema.UserProfileResponse:
  """
  获取用户个人信息
  """
  return await auth_service.get_user_profile(user)

@router.get(
  path="/logout",
  summary="用户登出")
async def logout(
  user: User = Depends(get_current_user),
  token: str = Depends(oauth2_scheme)) -> dict:
  """
  用户登出
  """
  return await auth_service.logout_user(token)

@router.get(
  path="/users",
  response_model=auth_schema.AdminUserListResponse,
  summary="管理员：用户与角色列表")
async def list_users(
  current_user: User = Depends(get_current_user),
) -> auth_schema.AdminUserListResponse:
  await require_admin(current_user)
  return await auth_service.list_users_for_admin()

@router.put(
  path="/users/{user_id}/role",
  response_model=auth_schema.UpdateUserRoleResponse,
  summary="管理员：设置用户角色")
async def update_user_role(
  user_id: str,
  payload: auth_schema.UpdateUserRoleRequest,
  current_user: User = Depends(get_current_user),
) -> auth_schema.UpdateUserRoleResponse:
  await require_admin(current_user)
  return await auth_service.update_user_role_for_admin(user_id, payload.role)
