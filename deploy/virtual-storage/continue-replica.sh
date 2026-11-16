#!/bin/sh
set -eu
BASE=/opt/storagent-vs
set -a
. "$BASE/env/passwords.env"
set +a
export OPS_PASS
if docker ps --format '{{.Names}}' | grep -qx storagent-vs-mongo-transition; then
  docker exec -e OPS_PASS storagent-vs-mongo-transition mongosh --quiet --host 10.32.12.110 --port 27018 --eval '
    const admin = db.getSiblingDB("admin");
    if (!admin.getUser("vs_ops")) {
      admin.createUser({user:"vs_ops", pwd:process.env.OPS_PASS, roles:[{role:"root", db:"admin"}]});
    }
    print("ops_user_ready");
  '
  docker rm -f storagent-vs-mongo-transition
fi
if ! docker ps --format '{{.Names}}' | grep -qx storagent-vs-mongo; then
  docker start storagent-vs-mongo
fi
ready=0
for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do
  if docker exec storagent-vs-mongo mongosh --quiet --host 10.32.12.110 --port 27018 -u vs_ops -p "$OPS_PASS" --authenticationDatabase admin --eval 'db.hello().isWritablePrimary' | grep -q true; then
    ready=1
    break
  fi
  sleep 2
done
[ "$ready" = 1 ]
mkdir -p "$BASE/data/mongo-arb"
if ! docker ps --format '{{.Names}}' | grep -qx storagent-vs-mongo-arb; then
  docker run -d --name storagent-vs-mongo-arb --network host --restart unless-stopped \
    -v "$BASE/data/mongo-arb:/data/db" -v "$BASE/mongo.key:/etc/mongo.key:ro" \
    mongo:6.0 --replSet vsrs --bind_ip 10.32.12.110 --port 27019 --auth --keyFile /etc/mongo.key
fi
docker exec storagent-vs-mongo mongosh --quiet --host 10.32.12.110 --port 27018 -u vs_ops -p "$OPS_PASS" --authenticationDatabase admin --eval '
  const hosts = rs.conf().members.map((member) => member.host);
  if (!hosts.includes("10.32.12.111:27018")) rs.add({host:"10.32.12.111:27018"});
  print("secondary_requested");
'
sleep 3
docker exec storagent-vs-mongo mongosh --quiet --host 10.32.12.110 --port 27018 -u vs_ops -p "$OPS_PASS" --authenticationDatabase admin --eval '
  const hosts = rs.conf().members.map((member) => member.host);
  if (!hosts.includes("10.32.12.110:27019")) rs.addArb("10.32.12.110:27019");
  rs.status().members.forEach((member) => print(member.name + " " + member.stateStr));
'
python3 "$BASE/src/deploy/virtual-storage/update-mongo-hosts.py"
docker compose --env-file "$BASE/env/minio.env" -p storagent-vs -f "$BASE/src/deploy/virtual-storage/docker-compose.yml" up -d --force-recreate api worker
scp -o StrictHostKeyChecking=no "$BASE/src/deploy/virtual-storage/update-mongo-hosts.py" root@10.32.12.111:/opt/storagent-vs/src/deploy/virtual-storage/update-mongo-hosts.py
ssh -o StrictHostKeyChecking=no root@10.32.12.111 'python3 /opt/storagent-vs/src/deploy/virtual-storage/update-mongo-hosts.py && docker compose -p storagent-vs -f /opt/storagent-vs/src/deploy/virtual-storage/docker-compose.b.yml up -d --force-recreate api worker'
echo continue_done
