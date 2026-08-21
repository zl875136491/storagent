"""管理面路由依赖：写操作需鉴权依赖声明。"""
import inspect

from src.core import auth as auth_core
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
    storage_route.reconcile_bucket_replication,
    storage_route.start_bucket_replication_resync,
    storage_route.start_cluster_heal,
    storage_route.create_one_time_object_download,
  ):
    assert "current_user" in _endpoint_params(fn)


def test_storage_read_routes_require_user():
  for fn in (
    storage_route.get_minio_server_list,
    storage_route.get_server_details,
    storage_route.get_buckets,
    storage_route.get_bucket_replicate_infos,
    storage_route.get_replication_operations,
    storage_route.get_cluster_health_operations,
    storage_route.get_cluster_heal_status,
    storage_route.get_storage_operations,
  ):
    assert "current_user" in _endpoint_params(fn)


def test_public_region_and_app_list_require_user():
  assert "current_user" in _endpoint_params(public_route.create_region)
  assert "current_user" in _endpoint_params(public_route.get_region_list)
  assert "current_user" in _endpoint_params(public_route.get_application_list)
  assert "current_user" in _endpoint_params(public_route.update_application_quota)
  assert "current_user" in _endpoint_params(public_route.add_application_domain)
  assert "current_user" in _endpoint_params(public_route.delete_application_domain)
  assert "current_user" in _endpoint_params(public_route.delete_application)


def test_public_endpoints_remain_unauthenticated_for_bootstrap():
  # get_endpoints 采用可选认证（auto_error=False）：匿名仍可达以支持启动探测，
  # 仅当携带有效管理员 token 时才返回 minio_endpoint。
  params = _endpoint_params(public_route.get_endpoints)
  dep = params["current_user"].default
  assert getattr(dep, "dependency", None) is auth_core.get_current_user_optional
  assert "current_user" not in _endpoint_params(public_route.test_endpoints)


def test_auth_admin_user_role_routes_require_user():
  assert "current_user" in _endpoint_params(auth_route.list_users)
  assert "current_user" in _endpoint_params(auth_route.update_user_role)


def test_graph_write_routes_require_user():
  assert "current_user" in _endpoint_params(graph_route.update_bucket_node_position)
  assert "current_user" in _endpoint_params(graph_route.update_bucket_edge_position)
