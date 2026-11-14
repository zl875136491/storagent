#!/bin/sh
# 把已在 A 构建的镜像送到 B，并只启动转发节点。不打印口令。
set -eu
BASE=/opt/storagent-vs
REMOTE=root@10.32.12.111
ssh -o StrictHostKeyChecking=no "$REMOTE" 'mkdir -p /opt/storagent-vs/certs /opt/storagent-vs/env /opt/storagent-vs/src/deploy/virtual-storage'
echo "sending image"
docker save storagent-virtual-storage-nuc:dev | ssh -o StrictHostKeyChecking=no "$REMOTE" docker load
scp -o StrictHostKeyChecking=no "$BASE/certs/cert.pem" "$BASE/certs/key.pem" "$REMOTE:/opt/storagent-vs/certs/"
scp -o StrictHostKeyChecking=no "$BASE/env/passwords.env" "$REMOTE:/opt/storagent-vs/env/passwords.env"
scp -o StrictHostKeyChecking=no "$BASE/src/deploy/virtual-storage/docker-compose.b.yml" "$REMOTE:/opt/storagent-vs/src/deploy/virtual-storage/docker-compose.b.yml"
ssh -o StrictHostKeyChecking=no "$REMOTE" 'sh -s' <<'REMOTE_SCRIPT'
set -eu
set -a
. /opt/storagent-vs/env/passwords.env
set +a
umask 077
cat > /opt/storagent-vs/env/api.env <<EOF
SECRET_KEY=${SECRET_KEY}
REGION=nuc-docker-b
VS_REGION=nuc-docker-b
VS_HOME_REGION=nuc-docker-a
VS_HOME_BASE_URL=https://10.32.12.110:8443
VS_MONGO_URL=mongodb://vs_api:${API_PASS}@10.32.12.110:27018/storagent_virtual_storage?authSource=admin&replicaSet=vsrs&w=majority
VS_MONGO_DB=storagent_virtual_storage
VS_MINIO_ENDPOINT=
VS_MINIO_ACCESS_KEY=
VS_MINIO_SECRET_KEY=
VS_MINIO_BUCKET=vs-pool-v1
VS_REQUIRE_HTTPS=true
VS_CELERY_BROKER_URL=mongodb://vs_api:${API_PASS}@10.32.12.110:27018/storagent_vs_broker?authSource=admin&replicaSet=vsrs
VS_QUEUE_PREFIX=storagent-vs
VS_PROTOCOL_VERSION=1
LOG_PATH=/app/logs/
EOF
cat > /opt/storagent-vs/env/worker.env <<EOF
SECRET_KEY=${SECRET_KEY}
REGION=nuc-docker-b
VS_REGION=nuc-docker-b
VS_HOME_REGION=nuc-docker-a
VS_MONGO_URL=mongodb://vs_worker:${WORKER_PASS}@10.32.12.110:27018/storagent_virtual_storage?authSource=admin&replicaSet=vsrs&w=majority
VS_MONGO_DB=storagent_virtual_storage
VS_MINIO_ENDPOINT=
VS_MINIO_ACCESS_KEY=
VS_MINIO_SECRET_KEY=
VS_MINIO_BUCKET=vs-pool-v1
VS_REQUIRE_HTTPS=true
VS_CELERY_BROKER_URL=mongodb://vs_worker:${WORKER_PASS}@10.32.12.110:27018/storagent_vs_broker?authSource=admin&replicaSet=vsrs
VS_QUEUE_PREFIX=storagent-vs
VS_PROTOCOL_VERSION=1
LOG_PATH=/app/logs/
EOF
chmod 600 /opt/storagent-vs/env/*.env
chmod 644 /opt/storagent-vs/certs/key.pem /opt/storagent-vs/certs/cert.pem
ss -lnt | awk '{print $4}' | grep -E ':18443$' && echo "port 18443 busy" && exit 1 || true
docker compose -p storagent-vs -f /opt/storagent-vs/src/deploy/virtual-storage/docker-compose.b.yml up -d
sleep 5
docker ps --filter name=storagent-vs --format '{{.Names}} {{.Image}} {{.Status}}'
REMOTE_SCRIPT
echo "node b started"
