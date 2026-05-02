# mongo-sharding

Шардированный стенд MongoDB с двумя шардами и одним `mongos`-роутером.

## Топология

| Сервис          | Тип             | Replica set     | Внутренний порт |
| --------------- | --------------- | --------------- | --------------- |
| `configSrv`     | configsvr       | `config_server` | 27019           |
| `shard1`        | shardsvr        | `shard1`        | 27018           |
| `shard2`        | shardsvr        | `shard2`        | 27018           |
| `mongos_router` | mongos (роутер) | —               | 27017           |
| `pymongo_api`   | приложение      | —               | 8080            |

Приложение подключается к `mongos_router:27017` (значение `MONGODB_URL`),
БД называется `somedb`, шардируемая коллекция — `helloDoc`.

## Требования

- Docker и Docker Compose v2.
- Минимум 2 CPU и 4 ГБ ОЗУ.

## Запуск

```bash
# 1. Поднять контейнеры
docker compose up -d

# 2. Инициализировать шардирование и засеять данные
./scripts/init-shard.sh
```

`init-shard.sh` идемпотентен — если запустить повторно, replica set
не пересоздаются и документы не дублируются.

### Что делает скрипт инициализации

1. `rs.initiate(...)` для config-server (`config_server`).
2. `rs.initiate(...)` для каждого шарда (`shard1`, `shard2`).
3. На `mongos`: `sh.addShard()` для обоих шардов.
4. На `mongos`: `sh.enableSharding("somedb")` и
   `sh.shardCollection("somedb.helloDoc", { _id: "hashed" })`.
5. Засев 1000 документов в `somedb.helloDoc` через `mongos`. Шардирование
   с хеш-ключом распределяет вставки по двум шардам автоматически.

## Проверка

### Через приложение

```bash
# Сводка кластера: топология, список шардов, количество документов
curl -s http://localhost:8080/ | jq

# Общее количество документов в helloDoc
curl -s http://localhost:8080/helloDoc/count | jq
```

В ответе `/` должны присутствовать:

- `"mongo_is_mongos": true`;
- блок `shards` с двумя шардами (`shard1`, `shard2`);
- блок `collections.helloDoc.documents_count` равный 1000.

### Количество документов в каждом из шардов

```bash
docker compose exec -T shard1 mongosh --port 27018 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF

docker compose exec -T shard2 mongosh --port 27018 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF
```

Сумма обоих чисел должна быть равна 1000, а сами числа — близки
(хеш-распределение должно дать примерно по 500 на шард).

### Состояние шардирования

```bash
docker compose exec -T mongos_router mongosh --port 27017 --quiet <<EOF
sh.status()
EOF
```

## Остановка и очистка

```bash
# Остановить контейнеры
docker compose down

# Полная очистка вместе с данными MongoDB
docker compose down -v
```
