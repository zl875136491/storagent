"""部署后的边界检查。只打印计数和拒绝结果，不打印连接串。"""
from pathlib import Path

from pymongo import MongoClient
from pymongo.errors import OperationFailure


def parse_env(path: str) -> dict:
  values = {}
  for line in Path(path).read_text().splitlines():
    if not line or line.startswith("#") or "=" not in line:
      continue
    key, value = line.split("=", 1)
    values[key] = value
  return values


def main() -> None:
  legacy_env = parse_env("/opt/storagent-test/env/backend-a.env")
  passwords = parse_env("/opt/storagent-vs/env/passwords.env")
  legacy = MongoClient(
    host="127.0.0.1",
    port=27017,
    username=legacy_env["MONGO_DB_USER"],
    password=legacy_env["MONGO_DB_PASSWD"],
    authSource=legacy_env["MONGO_DB_AUTH_SOURCE"],
    serverSelectionTimeoutMS=4000,
  )
  for name in ("storagent_nuc_docker_a", "storagent_nuc_docker_b"):
    collection = legacy[name].minio_server
    print(
      f"legacy {name} minio_server={collection.count_documents({})} "
      f"port_19020={collection.count_documents({'minio_port': 19020})}"
    )
  try:
    MongoClient(
      host="127.0.0.1",
      port=27017,
      username="vs_api",
      password=passwords["API_PASS"],
      authSource="admin",
      serverSelectionTimeoutMS=4000,
    ).admin.command("ping")
    print("shared_mongo_vs_api=unexpected_success")
  except Exception as exc:
    print("shared_mongo_vs_api=denied", type(exc).__name__)
  virtual = MongoClient(
    host="10.32.12.110",
    port=27018,
    username="vs_api",
    password=passwords["API_PASS"],
    authSource="admin",
    replicaSet="vsrs",
    serverSelectionTimeoutMS=4000,
  )
  try:
    virtual.admin.command("usersInfo")
    print("vs_api_admin_users=unexpected_success")
  except OperationFailure as exc:
    print("vs_api_admin_users=denied", exc.code)
  hello = virtual.admin.command("hello")
  print("replica", hello.get("setName"), "writable", hello.get("isWritablePrimary"))


if __name__ == "__main__":
  main()
