#!/bin/sh
# 只停止本次新增的虚拟存储容器，不删除卷，不碰旧 storagent 容器。
set -eu
if [ -f /opt/storagent-vs/docker-compose.b.yml ] && [ "$(hostname)" = "cubedev" ]; then
  docker compose -p storagent-vs -f /opt/storagent-vs/docker-compose.b.yml down
else
  docker compose -p storagent-vs -f /opt/storagent-vs/docker-compose.yml down
fi
echo "virtual storage containers stopped; volumes kept"
