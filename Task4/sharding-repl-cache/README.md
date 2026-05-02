# sharding-repl-cache

Шардированный стенд MongoDB с **двумя шардами по 3 реплики**, `mongos`-роутером
и **Redis-кэшем** перед приложением.

## Топология

| Сервис          | Тип             | Replica set     | Внутренний порт | Роль       |
| --------------- | --------------- | --------------- | --------------- | ---------- |
| `configSrv`     | configsvr       | `config_server` | 27019           | metadata   |
| `shard1-1`      | shardsvr        | `shard1`        | 27018           | primary*   |
| `shard1-2`      | shardsvr        | `shard1`        | 27018           | secondary  |
| `shard1-3`      | shardsvr        | `shard1`        | 27018           | secondary  |
| `shard2-1`      | shardsvr        | `shard2`        | 27018           | primary*   |
| `shard2-2`      | shardsvr        | `shard2`        | 27018           | secondary  |
| `shard2-3`      | shardsvr        | `shard2`        | 27018           | secondary  |
| `mongos_router` | mongos (роутер) | —               | 27017           | router     |
| `redis`         | Redis 7.x       | —               | 6379            | cache      |
| `pymongo_api`   | приложение      | —               | 8080            | API        |

\* — primary выбирается автоматически.


Само приложение `app.py` уже содержит логику кэширования через `fastapi-cache` — оно
включается автоматически, как только в окружении появляется `REDIS_URL`
(см. блок `if REDIS_URL: cache = cache` в `api_app/app.py`).

## Требования

- Docker и Docker Compose v2.
- Минимум 2 CPU и 4 ГБ ОЗУ.

## Запуск

```bash
# 1. Поднять контейнеры (включая redis)
docker compose up -d

# 2. Инициализировать replica set'ы и шардирование, засеять данные
./scripts/init-shard.sh
```

`init-shard.sh` идемпотентен.

## Проверка

### 1. Базовая работа кластера

```bash
curl -s http://localhost:8080/ | jq
```

Должно быть:
- `"mongo_is_mongos": true`
- `"cache_enabled": true`  ← это новое поле, появляется когда `REDIS_URL` задан
- блок `shards` с двумя шардами;
- `collections.helloDoc.documents_count` = 1000.

### 2. Общее количество документов

```bash
curl -s http://localhost:8080/helloDoc/count | jq
```

Ожидается `"items_count": 1000`.

### 3. Документы в каждом из шардов

```bash
docker compose exec -T shard1-1 mongosh --port 27018 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF

docker compose exec -T shard2-1 mongosh --port 27018 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF
```

Сумма = 1000.

### 4. Количество реплик в каждом replica set

```bash
docker compose exec -T shard1-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'
docker compose exec -T shard2-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'
```

Должно вывести `3` для каждого.

### 5. Кэш — главная проверка задания

Эндпоинт `/helloDoc/users` имеет искусственную задержку `time.sleep(1)` в коде:
первый запрос ≥ 1 секунды, повторные — из Redis за миллисекунды.

```bash
# Первый запрос — идёт в MongoDB через time.sleep(1)
time curl -s -o /dev/null http://localhost:8080/helloDoc/users

# Повторный запрос — должен быть < 100 мс (из Redis)
time curl -s -o /dev/null http://localhost:8080/helloDoc/users
time curl -s -o /dev/null http://localhost:8080/helloDoc/users
```

Ожидаемый вывод:
```
real    0m1.0XXs    ← первый запрос: ~1 секунда
real    0m0.0XXs    ← повторный: < 100 мс
real    0m0.0XXs    ← повторный: < 100 мс
```

TTL — 60 секунд (см. `@cache(expire=60 * 1)` в `api_app/app.py`),
по истечении кэш протухает и следующий запрос снова идёт в Mongo.

### 6. Проверить, что в Redis действительно лежит ключ

```bash
docker compose exec -T redis redis-cli KEYS 'api:cache*'
```

Должна быть пара ключей вида `api:cache:...:helloDoc`.

## Остановка и очистка

```bash
docker compose down       # остановить
docker compose down -v    # вместе с данными MongoDB и Redis
```
