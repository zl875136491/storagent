#!/bin/sh
# 在 10.32.12.110 上补齐独立副本集和专用 MinIO。不打印口令，不重启旧容器。
set -eu
BASE=/opt/storagent-vs
mkdir -p "$BASE/data/mongo" "$BASE/data/minio" "$BASE/certs" "$BASE/env" "$BASE/src"
if [ ! -f "$BASE/env/passwords.env" ]; then
  umask 077
  api_pass="$(openssl rand -hex 24)"
  worker_pass="$(openssl rand -hex 24)"
  minio_pass="$(openssl rand -hex 24)"
  app_pass="$(openssl rand -hex 24)"
  secret="$(openssl rand -hex 32)"
  cat > "$BASE/env/passwords.env" <<EOF
API_PASS=${api_pass}
WORKER_PASS=${worker_pass}
MINIO_PASS=${minio_pass}
APP_PASS=${app_pass}
SECRET_KEY=${secret}
EOF
  chmod 600 "$BASE/env/passwords.env"
fi
set -a
# shellcheck disable=SC1091
. "$BASE/env/passwords.env"
set +a
umask 077
cat > "$BASE/env/minio.env" <<EOF
VS_MINIO_ACCESS_KEY=vsminio
VS_MINIO_SECRET_KEY=${MINIO_PASS}
EOF
cat > "$BASE/env/api.env" <<EOF
SECRET_KEY=${SECRET_KEY}
REGION=nuc-docker-a
VS_REGION=nuc-docker-a
VS_HOME_REGION=nuc-docker-a
VS_MONGO_URL=mongodb://vs_api:${API_PASS}@10.32.12.110:27018/storagent_virtual_storage?authSource=admin&replicaSet=vsrs&w=majority
VS_MONGO_DB=storagent_virtual_storage
VS_MINIO_ENDPOINT=127.0.0.1:19020
VS_MINIO_ACCESS_KEY=vsminio
VS_MINIO_SECRET_KEY=${MINIO_PASS}
VS_MINIO_SECURE=false
VS_MINIO_BUCKET=vs-pool-v1
VS_REQUIRE_HTTPS=true
VS_BOOTSTRAP=true
VS_BOOTSTRAP_APP_NAME=apps
VS_BOOTSTRAP_DISPLAY_NAME=APPS
VS_BOOTSTRAP_CREDENTIAL=${APP_PASS}
VS_BOOTSTRAP_QUOTA_BYTES=10737418240
VS_BOOTSTRAP_QUOTA_OBJECTS=100000
VS_CELERY_BROKER_URL=mongodb://vs_api:${API_PASS}@10.32.12.110:27018/storagent_vs_broker?authSource=admin&replicaSet=vsrs
VS_QUEUE_PREFIX=storagent-vs
VS_PROTOCOL_VERSION=1
LOG_PATH=/app/logs/
EOF
cat > "$BASE/env/worker.env" <<EOF
SECRET_KEY=${SECRET_KEY}
REGION=nuc-docker-a
VS_REGION=nuc-docker-a
VS_HOME_REGION=nuc-docker-a
VS_MONGO_URL=mongodb://vs_worker:${WORKER_PASS}@10.32.12.110:27018/storagent_virtual_storage?authSource=admin&replicaSet=vsrs&w=majority
VS_MONGO_DB=storagent_virtual_storage
VS_MINIO_ENDPOINT=127.0.0.1:19020
VS_MINIO_ACCESS_KEY=vsminio
VS_MINIO_SECRET_KEY=${MINIO_PASS}
VS_MINIO_SECURE=false
VS_MINIO_BUCKET=vs-pool-v1
VS_REQUIRE_HTTPS=true
VS_BOOTSTRAP=false
VS_CELERY_BROKER_URL=mongodb://vs_worker:${WORKER_PASS}@10.32.12.110:27018/storagent_vs_broker?authSource=admin&replicaSet=vsrs
VS_QUEUE_PREFIX=storagent-vs
VS_PROTOCOL_VERSION=1
LOG_PATH=/app/logs/
EOF
cat > "$BASE/env/test.env" <<EOF
SECRET_KEY=${SECRET_KEY}
REGION=nuc-docker-a
VS_TEST_MONGO_URL=mongodb://vs_api:${API_PASS}@10.32.12.110:27018/?authSource=admin&replicaSet=vsrs&w=majority
VS_TEST_MONGO_DB=storagent_virtual_storage_test
VS_TEST_REGION=nuc-docker-a
VS_TEST_MINIO_ENDPOINT=127.0.0.1:19020
VS_TEST_MINIO_ACCESS_KEY=vsminio
VS_TEST_MINIO_SECRET_KEY=${MINIO_PASS}
VS_TEST_MINIO_BUCKET=vs-pool-v1-test
EOF
chmod 600 "$BASE"/env/*.env
if [ ! -f "$BASE/mongo.key" ]; then
  openssl rand -base64 756 > "$BASE/mongo.key"
  chown 999:999 "$BASE/mongo.key"
  chmod 400 "$BASE/mongo.key"
fi
if [ ! -f "$BASE/certs/cert.pem" ]; then
  openssl req -x509 -newkey rsa:2048 -keyout "$BASE/certs/key.pem" -out "$BASE/certs/cert.pem" -days 365 -nodes \
    -subj "/CN=storagent-vs" \
    -addext "subjectAltName=IP:10.32.12.110,IP:10.32.12.111,DNS:localhost"
  chown 10001:10001 "$BASE/certs/key.pem" "$BASE/certs/cert.pem"
  chmod 640 "$BASE/certs/key.pem"
fi
if ! docker ps --format '{{.Names}}' | grep -qx storagent-vs-mongo; then
  if ! docker ps --format '{{.Names}}' | grep -qx storagent-vs-mongo-init; then
  docker rm -f storagent-vs-mongo-init >/dev/null 2>&1 || true
  docker run -d --name storagent-vs-mongo-init --network host \
    -v "$BASE/data/mongo:/data/db" \
    mongo:6.0 --replSet vsrs --bind_ip_all --port 27018
  ready=0
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if docker exec storagent-vs-mongo-init mongosh --quiet --port 27018 --eval 'db.runCommand({ping:1}).ok' | grep -q 1; then
      ready=1
      break
    fi
    sleep 2
  done
  [ "$ready" = 1 ]
  docker exec storagent-vs-mongo-init mongosh --quiet --port 27018 --eval 'try { rs.status().ok } catch (e) { rs.initiate({_id:"vsrs", members:[{_id:0, host:"10.32.12.110:27018"}]}); "init" }'
  primary=0
  for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
    if docker exec storagent-vs-mongo-init mongosh --quiet --port 27018 --eval 'db.hello().isWritablePrimary' | grep -q true; then
      primary=1
      break
    fi
    sleep 2
  done
  [ "$primary" = 1 ]
  fi
  docker exec -e API_PASS -e WORKER_PASS storagent-vs-mongo-init mongosh --quiet --port 27018 --eval '
    function ensure(name, pwd) {
      const admin = db.getSiblingDB("admin");
      if (admin.getUser(name)) return;
      admin.createUser({user:name, pwd:pwd, roles:[
        {role:"readWrite", db:"storagent_virtual_storage"},
        {role:"readWrite", db:"storagent_virtual_storage_test"},
        {role:"readWrite", db:"storagent_vs_broker"}
      ]});
    }
    ensure("vs_api", process.env.API_PASS);
    ensure("vs_worker", process.env.WORKER_PASS);
  '
  docker rm -f storagent-vs-mongo-init
fi
echo "bootstrap data ready"
