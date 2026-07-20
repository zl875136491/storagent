from fastapi import APIRouter, Depends
from src.modules.auth import service as auth_service
from src.modules.auth import schema as auth_schema
from src.core.auth import get_current_user, oauth2_scheme
from src.modules.auth.model import User

router = APIRouter()

@router.post(
  path="/login",
  response_model=auth_schema.TokenResponse,
  summary="用户登录")
async def login(
  payload: auth_schema.LoginRequest) -> auth_schema.TokenResponse:
  """
  用户登录
  """
  username = payload.username.strip()
  password = payload.password.strip()
  return await auth_service.login_user(username, password)

@router.post(
  path="/refresh",
  response_model=auth_schema.TokenResponse,
  summary="刷新 Token")
async def refresh_token(
  payload: auth_schema.RefreshTokenRequest) -> auth_schema.TokenResponse:
  """
  使用 refresh token 获取新的 access token 和 refresh token
  """
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