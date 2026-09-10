from datetime import datetime, timezone

from src.modules.storage.inventory import (
  build_inventory_documents,
  search_regex,
  slice_child_page,
  split_object_key,
)


def test_split_object_key_strips_slashes_and_empties():
  assert split_object_key("/a//b/c.txt/") == ["a", "b", "c.txt"]
  assert split_object_key("") == []


def test_build_inventory_documents_rolls_up_directory_size_and_children():
  now = datetime(2026, 9, 10, tzinfo=timezone.utc)
  earlier = datetime(2026, 9, 9, tzinfo=timezone.utc)
  documents, buckets = build_inventory_documents(
    "sid",
    "gen1",
    [
      {"bucket": "app", "object_key": "a/b/large.bin", "size": 80, "last_modified": now},
      {"bucket": "app", "object_key": "a/b/small.bin", "size": 20, "last_modified": earlier},
      {"bucket": "app", "object_key": "a/readme.txt", "size": 5, "last_modified": now},
      {"bucket": "app", "object_key": "root.txt", "size": 1, "last_modified": earlier},
      {"bucket": "logs", "object_key": "today.log", "size": 4, "last_modified": now},
    ],
    bucket_created={"app": earlier, "logs": now, "empty": now},
  )

  dirs = {
    (item["parent"], item["name"]): item
    for item in documents
    if item["kind"] == "dir"
  }
  files = [item for item in documents if item["kind"] == "file"]

  assert len(files) == 5
  assert dirs[("", "a")]["size"] == 105
  assert dirs[("", "a")]["object_count"] == 3
  assert dirs[("", "a")]["child_count"] == 2
  assert dirs[("a", "b")]["size"] == 100
  assert dirs[("a", "b")]["child_count"] == 2
  assert dirs[("a", "b")]["object_count"] == 2

  by_name = {item["name"]: item for item in buckets}
  assert by_name["app"]["total_size"] == 106
  assert by_name["app"]["object_count"] == 4
  assert by_name["logs"]["object_count"] == 1
  assert by_name["empty"]["object_count"] == 0
  assert by_name["empty"]["total_size"] == 0


def test_slice_child_page_returns_largest_first_then_remainder():
  items = [
    {"name": "tiny", "size": 1},
    {"name": "huge", "size": 90},
    {"name": "mid", "size": 9},
  ]
  page, total = slice_child_page(items, offset=0, limit=2, sort="size", order="desc")
  assert total == 3
  assert [item["name"] for item in page] == ["huge", "mid"]

  rest, _ = slice_child_page(items, offset=2, limit=2, sort="size", order="desc")
  assert [item["name"] for item in rest] == ["tiny"]


def test_search_regex_escapes_user_input():
  assert search_regex("a+b.txt") == r"a\+b\.txt"
