#!/bin/sh
# 把单节点副本集扩展到两台数据节点加一个仲裁节点。不打印口令。
set -eu
BASE=/opt/storagent-vs
set -a
. "$BASE/env/passwords.env"
set +a
if ! grep -q '^OPS_PASS=' "$BASE/env/passwords.env"; then
  ops="$(openssl rand -hex 24)"
  printf 'OPS_PASS=%s\n' "$ops" >> "$BASE/env/passwords.env"
  OPS_PASS="$ops"
fi
export OPS_PASS
chmod 600 "$BASE/env/passwords.env"
docker compose --env-file "$BASE/env/minio.env" -p storagent-vs -f "$BASE/src/deploy/virtual-storage/docker-compose.yml" stop api worker || true
ssh -o StrictHostKeyChecking=no root@10.32.12.111 'docker compose -p storagent-vs -f /opt/storagent-vs/src/deploy/virtual-storage/docker-compose.b.yml stop api worker || true'

ssh -o StrictHostKeyChecking=no root@10.32.12.111 'mkdir -p /opt/storagent-vs/data/mongo /opt/storagent-vs/data/etcd-b'
scp -o StrictHostKeyChecking=no "$BASE/mongo.key" root@10.32.12.111:/opt/storagent-vs/mongo.key
ssh -o StrictHostKeyChecking=no root@10.32.12.111 'chown 999:999 /opt/storagent-vs/mongo.key && chmod 400 /opt/storagent-vs/mongo.key'
if ! ssh -o StrictHostKeyChecking=no root@10.32.12.111 'docker ps --format {{.Names}}' | grep -qx storagent-vs-mongo-b; then
  ssh -o StrictHostKeyChecking=no root@10.32.12.111 'docker run -d --name storagent-vs-mongo-b --network host --restart unless-stopped -v /opt/storagent-vs/data/mongo:/data/db -v /opt/storagent-vs/mongo.key:/etc/mongo.key:ro mongo:6.0 --replSet vsrs --bind_ip 10.32.12.111 --port 27018 --auth --keyFile /etc/mongo.key'
fi

docker stop storagent-vs-mongo
docker run -d --name storagent-vs-mongo-transition --network host -v "$BASE/data/mongo:/data/db" -v "$BASE/mongo.key:/etc/mongo.key:ro" mongo:6.0 --replSet vsrs --bind_ip 10.32.12.110 --port 27018 --transitionToAuth --keyFile /etc/mongo.key
ready=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if docker exec storagent-vs-mongo-transition mongosh --quiet --host 10.32.12.110 --port 27018 --eval 'db.hello().isWritablePrimary' | grep -q true; then
    ready=1
    break
  fi
  sleep 2
done
[ "$ready" = 1 ]
docker exec -e OPS_PASS storagent-vs-mongo-transition mongosh --quiet --host 10.32.12.110 --port 27018 --eval '
  const admin = db.getSiblingDB("admin");
  if (!admin.getUser("vs_ops")) {
    admin.createUser({user:"vs_ops", pwd:process.env.OPS_PASS, roles:[{role:"root", db:"admin"}]});
  }
'
docker rm -f storagent-vs-mongo-transition
docker start storagent-vs-mongo
ready=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do
  if docker exec -e OPS_PASS storagent-vs-mongo mongosh --quiet --host 10.32.12.110 --port 27018 -u vs_ops -p "$OPS_PASS" --authenticationDatabase admin --eval 'db.hello().isWritablePrimary' | grep -q true; then
    ready=1
    break
  fi
  sleep 2
done
[ "$ready" = 1 ]
mkdir -p "$BASE/data/mongo-arb"
if ! docker ps --format '{{.Names}}' | grep -qx storagent-vs-mongo-arb; then
  docker run -d --name storagent-vs-mongo-arb --network host --restart unless-stopped -v "$BASE/data/mongo-arb:/data/db" -v "$BASE/mongo.key:/etc/mongo.key:ro" mongo:6.0 --replSet vsrs --bind_ip 10.32.12.110 --port 27019 --auth --keyFile /etc/mongo.key
fi
docker exec -e OPS_PASS storagent-vs-mongo mongosh --quiet "mongodb://vs_ops:${OPS_PASS}@10.32.12.110:27018/admin?authSource=admin&replicaSet=vsrs" --eval '
  const hosts = rs.conf().members.map((member) => member.host);
  if (!hosts.includes("10.32.12.111:27018")) rs.add({host:"10.32.12.111:27018"});
'
docker exec -e OPS_PASS storagent-vs-mongo mongosh --quiet "mongodb://vs_ops:${OPS_PASS}@10.32.12.110:27018/admin?authSource=admin&replicaSet=vsrs" --eval '
  const hosts = rs.conf().members.map((member) => member.host);
  if (!hosts.includes("10.32.12.110:27019")) rs.addArb("10.32.12.110:27019");
  const status = rs.status();
  status.members.forEach((member) => print(member.name + " " + member.stateStr));
'
python3 "$BASE/src/deploy/virtual-storage/update-mongo-hosts.py"
docker compose --env-file "$BASE/env/minio.env" -p storagent-vs -f "$BASE/src/deploy/virtual-storage/docker-compose.yml" up -d --force-recreate api worker
scp -o StrictHostKeyChecking=no "$BASE/src/deploy/virtual-storage/update-mongo-hosts.py" root@10.32.12.111:/opt/storagent-vs/src/deploy/virtual-storage/update-mongo-hosts.py
ssh -o StrictHostKeyChecking=no root@10.32.12.111 'python3 /opt/storagent-vs/src/deploy/virtual-storage/update-mongo-hosts.py && docker compose -p storagent-vs -f /opt/storagent-vs/src/deploy/virtual-storage/docker-compose.b.yml up -d --force-recreate api worker'
echo "replica expansion requested"
