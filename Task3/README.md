# mongo-sharding-repl

Шардированный стенд MongoDB с двумя шардами, **в каждом — replica set из 3 нод**, и одним `mongos`-роутером.

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
| `pymongo_api`   | приложение      | —               | 8080            | API        |

\* — primary выбирается автоматически при инициализации, может оказаться любой нодой replica set.

Приложение подключается к `mongos_router:27017` (значение `MONGODB_URL`),
БД называется `somedb`, шардируемая коллекция — `helloDoc`.

## Что нового по сравнению с Task 2

| Было (mongo-sharding) | Стало (mongo-sharding-repl) |
|---|---|
| 1 нода в каждом шарде | 3 ноды в каждом шарде |
| `rs.initiate({ members: [1 host] })` | `rs.initiate({ members: [3 hosts] })` |
| `sh.addShard("shard1/shard1:27018")` | `sh.addShard("shard1/shard1-1,shard1-2,shard1-3")` |
| Падение ноды → шард недоступен | Падение 1 ноды → primary переизбирается, кластер живёт |

## Требования

- Docker и Docker Compose v2.
- Минимум 2 CPU и 4 ГБ ОЗУ (сборка тяжелее: 7 контейнеров MongoDB вместо 3).

## Запуск

```bash
# 1. Поднять контейнеры
docker compose up -d

# 2. Инициализировать replica set'ы и шардирование, засеять данные
./scripts/init-shard.sh
```

`init-shard.sh` идемпотентен — если запустить повторно, replica set
не пересоздаются и документы не дублируются.

### Что делает скрипт инициализации

1. **`rs.initiate(...)` для config-server** (`config_server`) — 1 нода.
2. **`rs.initiate(...)` для shard1** с тремя членами:
   - `shard1-1:27018` (`_id: 0`)
   - `shard1-2:27018` (`_id: 1`)
   - `shard1-3:27018` (`_id: 2`)
3. **`rs.initiate(...)` для shard2** аналогично с `shard2-1`, `shard2-2`, `shard2-3`.
4. **Ожидание выбора primary** — опрашивает каждый replica set до 30 секунд, пока MongoDB не выберет primary через выборы по протоколу Raft.
5. **На `mongos`:** `sh.addShard("shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018")` — все три члена replica set передаются в одной строке через запятую. Аналогично для `shard2`.
6. **На `mongos`:** `sh.enableSharding("somedb")` и `sh.shardCollection("somedb.helloDoc", { _id: "hashed" })`.
7. Засев 1000 документов в `somedb.helloDoc` через `mongos`.

## Проверка

### Через приложение

```bash
# Сводка кластера: топология, список шардов, количество документов
curl -s http://localhost:8080/ | jq

# Общее количество документов в helloDoc (≥ 1000)
curl -s http://localhost:8080/helloDoc/count | jq
```

В ответе `/` должны присутствовать:

- `"mongo_is_mongos": true`;
- блок `shards` с двумя шардами (`shard1`, `shard2`); значения хостов теперь содержат
  все 3 члена replica set, например: `"shard1": "shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018"`;
- `collections.helloDoc.documents_count` равный 1000.

### Количество документов в каждом из шардов

Подключаемся к **любой** ноде каждого replica set (по умолчанию `mongosh` отвечает с primary):

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

Сумма обоих чисел = 1000. Хеш-распределение должно дать примерно по 500 на шард.

### Количество реплик в каждом replica set

```bash
# shard1: должно быть 3
docker compose exec -T shard1-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'

# shard2: должно быть 3
docker compose exec -T shard2-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'
```

Подробное состояние с ролями каждой ноды:

```bash
docker compose exec -T shard1-1 mongosh --port 27018 --quiet --eval \
  'rs.status().members.map(m => ({ name: m.name, state: m.stateStr }))'
```

Должно вывести что-то вроде:
```js
[
  { name: 'shard1-1:27018', state: 'PRIMARY' },
  { name: 'shard1-2:27018', state: 'SECONDARY' },
  { name: 'shard1-3:27018', state: 'SECONDARY' }
]
```

### Состояние шардирования

```bash
docker compose exec -T mongos_router mongosh --port 27017 --quiet <<EOF
sh.status()
EOF
```

В выводе раздел `shards:` теперь покажет состав каждого replica set, например:
```
{ _id: 'shard1', host: 'shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018', state: 1 }
```

## Проверка отказоустойчивости (опционально)

Один из плюсов репликации — переживание падений. Проверим:

```bash
# Останавливаем primary шарда 1 (узнайте host командой rs.status() выше)
docker compose stop shard1-1

# Через 10–30 секунд один из secondary становится новым primary.
# Запросы продолжают работать:
curl -s http://localhost:8080/helloDoc/count | jq

# Возвращаем ноду в кластер
docker compose start shard1-1
```

## Остановка и очистка

```bash
# Остановить контейнеры
docker compose down

# Полная очистка вместе с данными MongoDB
docker compose down -v
```
