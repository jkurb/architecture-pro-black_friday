#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[1/5] Инициализация config server replica set..."
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

echo "[2/5] Инициализация shard1 replica set..."
docker compose exec -T shard1 mongosh --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard1 already initialized");
} catch (e) {
  rs.initiate({
    _id: "shard1",
    members: [{ _id: 0, host: "shard1:27018" }]
  });
}
EOF

echo "[3/5] Инициализация shard2 replica set..."
docker compose exec -T shard2 mongosh --port 27018 --quiet <<'EOF'
try {
  rs.status();
  print("shard2 already initialized");
} catch (e) {
  rs.initiate({
    _id: "shard2",
    members: [{ _id: 0, host: "shard2:27018" }]
  });
}
EOF

echo "Ждём, пока mongos увидит config server и шарды..."
sleep 5

echo "[4/5] Регистрация шардов в кластере и включение шардирования..."
docker compose exec -T mongos_router mongosh --port 27017 --quiet <<'EOF'
sh.addShard("shard1/shard1:27018");
sh.addShard("shard2/shard2:27018");
sh.enableSharding("somedb");
sh.shardCollection("somedb.helloDoc", { "_id": "hashed" });
EOF

echo "[5/5] Засеваем somedb.helloDoc 1000 документами..."
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
