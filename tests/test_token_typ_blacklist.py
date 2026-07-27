"""access/refresh typ 区分与黑名单过期时间。"""
import asyncio
from datetime import datetime, timedelta, timezone

from jose import jwt

from src.configs.configs import settings
from src.core.auth import TOKEN_TYP_ACCESS, TOKEN_TYP_REFRESH, create_token
from src.modules.auth.crud import blacklist_expiry_for_token


def test_create_token_sets_typ():
  tokens = asyncio.run(create_token("alice"))
  access = jwt.decode(
    tokens["access_token"], settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
  )
  refresh = jwt.decode(
    tokens["refresh_token"], settings.SECRET_KEY, algorithms=[settings.ALGORITHM]
  )
  assert access["typ"] == TOKEN_TYP_ACCESS
  assert refresh["typ"] == TOKEN_TYP_REFRESH
  assert access["sub"] == "alice"
  assert refresh["sub"] == "alice"


def test_blacklist_expiry_matches_jwt_exp():
  exp = datetime.now(timezone.utc) + timedelta(hours=2)
  token = jwt.encode(
    {"sub": "bob", "typ": TOKEN_TYP_ACCESS, "exp": exp},
    settings.SECRET_KEY,
    algorithm=settings.ALGORITHM,
  )
  black_exp = blacklist_expiry_for_token(token)
  assert abs(black_exp.timestamp() - exp.timestamp()) < 2


def test_blacklist_expiry_fallback_for_garbage():
  black_exp = blacklist_expiry_for_token("not-a-jwt")
  assert black_exp > datetime.now(timezone.utc)
