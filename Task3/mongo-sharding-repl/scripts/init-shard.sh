#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[1/6] Инициализация config server replica set..."
docker compose exec -T configSrv mongosh --port 27019 --quiet <<'EOF'
try {
  rs.status();
  print("config_server already initialized");
} catch (e) {
  rs.initiate({
    _id: "config_server",
    configsvr: true,
    members: [{ _id: 0, host: "configSrv:27019" }]
  });
}
EOF

echo "[2/6] Инициализация shard1 replica set (3 ноды: shard1-1, shard1-2, shard1-3)..."
docker compose exec -T shard1-1 mongosh --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard1 already initialized");
} catch (e) {
  rs.initiate({
    _id: "shard1",
    members: [
      { _id: 0, host: "shard1-1:27018" },
      { _id: 1, host: "shard1-2:27018" },
      { _id: 2, host: "shard1-3:27018" }
    ]
  });
}
EOF

echo "[3/6] Инициализация shard2 replica set (3 ноды: shard2-1, shard2-2, shard2-3)..."
docker compose exec -T shard2-1 mongosh --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard2 already initialized");
} catch (e) {
  rs.initiate({
    _id: "shard2",
    members: [
      { _id: 0, host: "shard2-1:27018" },
      { _id: 1, host: "shard2-2:27018" },
      { _id: 2, host: "shard2-3:27018" }
    ]
  });
}
EOF

echo "[4/6] Ждём выбора primary в каждом replica set..."
for rs_pair in "shard1-1:shard1" "shard2-1:shard2"; do
  host="${rs_pair%%:*}"
  rs_name="${rs_pair##*:}"
  for i in {1..30}; do
    state=$(docker compose exec -T "$host" mongosh --port 27018 --quiet --eval \
      'try { rs.isMaster().ismaster ? "PRIMARY" : (rs.isMaster().secondary ? "SECONDARY" : "PENDING") } catch(e) { "PENDING" }' 2>/dev/null | tr -d '\r' | tail -1)
    if [[ "$state" == "PRIMARY" || "$state" == "SECONDARY" ]]; then
      echo "  $rs_name: primary выбран ($state на $host)"
      break
    fi
    sleep 1
  done
done

echo "[5/6] Регистрация шардов в кластере и включение шардирования..."
docker compose exec -T mongos_router mongosh --port 27017 --quiet <<'EOF'
sh.addShard("shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018");
sh.addShard("shard2/shard2-1:27018,shard2-2:27018,shard2-3:27018");
sh.enableSharding("somedb");
sh.shardCollection("somedb.helloDoc", { "_id": "hashed" });
EOF

echo "[6/6] Засеваем somedb.helloDoc 1000 документами..."
docker compose exec -T mongos_router mongosh --port 27017 --quiet <<'EOF'
use somedb
if (db.helloDoc.countDocuments() >= 1000) {
  print("helloDoc уже содержит >= 1000 документов, пропускаю засев");
} else {
  const bulk = [];
  for (let i = 0; i < 1000; i++) {
    bulk.push({ insertOne: { document: { age: i, name: "ly" + i } } });
  }
  db.helloDoc.bulkWrite(bulk);
  print("Вставлено документов: " + db.helloDoc.countDocuments());
}
EOF

echo
echo "Готово. Проверка:"
echo "  curl -s http://localhost:8080/ | jq"
echo "  curl -s http://localhost:8080/helloDoc/count | jq"
echo "  docker compose exec -T shard1-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'"
