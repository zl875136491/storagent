#!/bin/sh
# 在 18088 重建 v2 测试入口，复用已经健康的虚拟存储 etcd，不占用 B 的 8088。
set -eu
ROOT=/opt/storagent-v2reg
rm -rf "$ROOT"
mkdir -p "$ROOT"
rsync -a --exclude data /opt/storagent-test/ "$ROOT/"
mkdir -p "$ROOT/data/minio"
python3 - <<'PY'
from pathlib import Path
root = Path("/opt/storagent-v2reg")
for name in ("docker-compose.a.yml", "docker-compose.b.yml"):
    text = (root / name).read_text()
    text = text.replace('"8088:80"', '"18088:80"')
    # 不启动第二套 etcd，业务容器不再等待它。
    text = text.replace("      etcd:\n        condition: service_healthy\n", "")
    (root / name).write_text(text)
for name in ("nginx-a/default.conf", "nginx-b/default.conf"):
    path = root / name
    text = path.read_text().replace("http://backend:8088", "http://backend:18088").replace(":8088", ":18088")
    path.write_text(text)
for name in ("env/backend-a.env", "env/backend-b.env"):
    path = root / name
    lines = []
    for line in path.read_text().splitlines():
        if line.startswith("SERVER_PORT="):
            lines.append("SERVER_PORT=18088")
        elif line.startswith("FRONT_URL="):
            host = "10.32.12.110" if name.endswith("a.env") else "10.32.12.111"
            lines.append(f"FRONT_URL=https://{host}:18443")
        elif line.startswith("ETCD_ENDPOINTS="):
            lines.append("ETCD_ENDPOINTS=http://10.32.12.110:12379,http://10.32.12.111:12379,http://10.32.12.110:12383")
        elif line.startswith("ETCD_HOST="):
            host = "10.32.12.110" if name.endswith("a.env") else "10.32.12.111"
            lines.append(f"ETCD_HOST={host}")
        elif line.startswith("ETCD_PORT="):
            lines.append("ETCD_PORT=12379")
        else:
            lines.append(line)
    path.write_text("\n".join(lines) + "\n")
print("layout ready")
PY
# compose 里的 ETCD_ENDPOINTS 会覆盖 env 文件，改成现有三成员集群。
python3 - <<'PY'
from pathlib import Path
root = Path("/opt/storagent-v2reg")
for name in ("docker-compose.a.yml", "docker-compose.b.yml"):
    path = root / name
    text = path.read_text()
    text = text.replace(
        "ETCD_ENDPOINTS: http://etcd:2379,http://10.32.12.111:12379",
        "ETCD_ENDPOINTS: http://10.32.12.110:12379,http://10.32.12.111:12379,http://10.32.12.110:12383",
    )
    text = text.replace(
        "ETCD_ENDPOINTS: http://etcd:2379,http://10.32.12.110:12379",
        "ETCD_ENDPOINTS: http://10.32.12.110:12379,http://10.32.12.111:12379,http://10.32.12.110:12383",
    )
    text = text.replace(
        "ETCD_ENDPOINTS: http://10.32.12.110:12379,http://etcd:2379",
        "ETCD_ENDPOINTS: http://10.32.12.110:12379,http://10.32.12.111:12379,http://10.32.12.110:12383",
    )
    path.write_text(text)
print("compose endpoints patched")
PY
echo prepared
