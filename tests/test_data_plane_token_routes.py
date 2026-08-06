"""v1 数据面路由：分片上传与下载必须同时接受 x-api-key 与能力令牌 token。"""
import inspect

from src.modules.files import route as files_route


def _params(fn):
  return inspect.signature(fn).parameters


def test_multipart_part_route_accepts_optional_token_and_api_key():
  params = _params(files_route.multipart_upload_part)
  assert "token" in params
  assert "api_key" in params
  # x-api-key 仍然可用（App 后端到 Storagent 的服务端调用），但不再是唯一手段。
  assert params["api_key"].default is not inspect.Parameter.empty


def test_download_chunk_route_accepts_optional_token_and_api_key():
  params = _params(files_route.download_chunk)
  assert "token" in params
  assert "api_key" in params
  assert params["api_key"].default is not inspect.Parameter.empty


def test_control_plane_routes_still_require_strict_api_key_context():
  # init / complete / abort / parts / stat / locate 属于控制面，只允许 App 后端持有的
  # x-api-key（不接受前端能力令牌），因此保持使用严格模式的 get_current_app_context。
  for fn in (
    files_route.multipart_init,
    files_route.multipart_complete,
    files_route.multipart_abort,
    files_route.multipart_list_parts,
    files_route.object_stat,
    files_route.object_locate,
  ):
    params = _params(fn)
    assert "app_context" in params
    assert "token" not in params
