"""管理面路由依赖：写操作需鉴权依赖声明。"""
import inspect

from src.modules.storage import route as storage_route
from src.modules.public import route as public_route
from src.modules.auth import route as auth_route
from src.modules.graph import route as graph_route


def _endpoint_params(fn):
  return inspect.signature(fn).parameters


def test_storage_write_routes_require_user():
  for fn in (
    storage_route.create_minio_server,
    storage_route.update_minio_server,
    storage_route.create_bucket_replicate,
    storage_route.delete_bucket_replicate,
  ):
    assert "current_user" in _endpoint_params(fn)


def test_storage_read_routes_require_user():
  for fn in (
    storage_route.get_minio_server_list,
    storage_route.get_server_details,
    storage_route.get_buckets,
    storage_route.get_bucket_replicate_infos,
  ):
    assert "current_user" in _endpoint_params(fn)


def test_public_region_and_app_list_require_user():
  assert "current_user" in _endpoint_params(public_route.create_region)
  assert "current_user" in _endpoint_params(public_route.get_region_list)
  assert "current_user" in _endpoint_params(public_route.get_application_list)


def test_public_endpoints_remain_unauthenticated_for_bootstrap():
  assert "current_user" not in _endpoint_params(public_route.get_endpoints)
  assert "current_user" not in _endpoint_params(public_route.test_endpoints)


def test_auth_admin_user_role_routes_require_user():
  assert "current_user" in _endpoint_params(auth_route.list_users)
  assert "current_user" in _endpoint_params(auth_route.update_user_role)


def test_graph_write_routes_require_user():
  assert "current_user" in _endpoint_params(graph_route.update_bucket_node_position)
  assert "current_user" in _endpoint_params(graph_route.update_bucket_edge_position)
