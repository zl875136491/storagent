"""Regression checks for the console demo's opaque APIKey reference contract."""
import inspect

from starlette.middleware.cors import CORSMiddleware

from main import create_app

from src.modules.demo import route as demo_route


def test_demo_routes_require_the_opaque_api_key_header():
  for endpoint in (
    demo_route.demo_multipart_init,
    demo_route.demo_multipart_part,
    demo_route.demo_multipart_complete,
    demo_route.demo_multipart_abort,
    demo_route.demo_object_stat,
    demo_route.demo_object_locate,
    demo_route.demo_object_download,
  ):
    assert "app_context" in inspect.signature(endpoint).parameters


def test_demo_context_never_accepts_api_key_plaintext_parameter():
  parameters = inspect.signature(demo_route._context).parameters
  assert "api_key_id" in parameters
  assert "api_key" not in parameters


def test_cors_allows_the_demo_api_key_reference_header():
  app = create_app()
  cors = next(item for item in app.user_middleware if item.cls is CORSMiddleware)

  assert "x-demo-api-key-id" in cors.kwargs["allow_headers"]
