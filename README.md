# architecture-pro: «Мобильный мир»

Проектная работа по архитектуре онлайн-магазина «Мобильный мир» для подготовки
к Чёрной пятнице. Решает задачи горизонтального масштабирования и отказоустойчивости
через шардирование и репликацию MongoDB, кэширование Redis и геораспределённую CDN.

## Структура репозитория

```
.
├── README.md                      ← инструкция запуска (вы здесь)
├── ARCHITECTURE.md                ← единый архитектурный документ (задания 7–10)
├── architecture-final.drawio      ← финальная схема (variant 5: + CDN)
├── diagrams/                      ← пять вариантов схем (задания 1, 5, 6) + README
│   ├── README.md
│   ├── task1_v1_sharding.drawio
│   ├── task1_v2_replication.drawio
│   ├── task1_v3_cache.drawio
│   ├── task1_v4_scaling.drawio
│   └── task1_v5_cdn.drawio
├── mongo-sharding/                ← задание 2: только шардирование
├── mongo-sharding-repl/           ← задание 3: + репликация (3 ноды на шард)
└── sharding-repl-cache/           ← задание 4: + Redis-кэш  ★ для проверки
```

## Что проверять

**Финальный стенд для ревью — `sharding-repl-cache/`.** Он содержит решения заданий 2, 3 и 4:
шардирование MongoDB (2 шарда), репликация (3 ноды на шард) и Redis-кэш.

Остальные две папки оставлены как промежуточные шаги для иллюстрации эволюции стенда.

## Требования

- Docker и Docker Compose v2
- Минимум 2 CPU и 4 ГБ ОЗУ
- Используется готовый docker-образ приложения `kazhem/pymongo_api:1.0.0`

## Запуск финального стенда

```bash
cd sharding-repl-cache

# 1. Поднять контейнеры (10 сервисов: 1 configSrv + 6 shard nodes + mongos + redis + pymongo_api)
docker compose up -d

# 2. Проверить, что все сервисы up
docker compose ps

# 3. Инициализировать replica set'ы, шардирование и засеять 1000 документов
./scripts/init-shard.sh
```

Скрипт `init-shard.sh` идемпотентен: его можно запускать повторно при необходимости.

## Проверка

### Через браузер

Откройте http://localhost:8080/ — должен отобразиться JSON со статусом MongoDB и кэша:

```json
{
  "mongo_topology_type": "Sharded",
  "mongo_is_mongos": true,
  "cache_enabled": true,
  "shards": {
    "shard1": "shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018",
    "shard2": "shard2/shard2-1:27018,shard2-2:27018,shard2-3:27018"
  },
  "collections": {
    "helloDoc": { "documents_count": 1000 }
  },
  "status": "OK"
}
```

### Через curl

```bash
# Сводка кластера
curl -s http://localhost:8080/ | jq

# Общее количество документов в коллекции
curl -s http://localhost:8080/helloDoc/count | jq
```

### Распределение документов по шардам

```bash
docker compose exec -T shard1-1 mongosh --port 27018 --quiet --eval 'use somedb; db.helloDoc.countDocuments()'
docker compose exec -T shard2-1 mongosh --port 27018 --quiet --eval 'use somedb; db.helloDoc.countDocuments()'
# Сумма должна быть = 1000, числа примерно равны
```

### Количество реплик в каждом replica set

```bash
docker compose exec -T shard1-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'
docker compose exec -T shard2-1 mongosh --port 27018 --quiet --eval 'rs.status().members.length'
# Ожидается: 3 для каждого
```

### Проверка кэша

Эндпоинт `/<collection>/users` имеет искусственную задержку 1 секунда в коде. При наличии Redis-кэша повторные вызовы должны возвращаться < 100 мс.

```bash
# Первый запрос — cache miss → читает из MongoDB
time curl -s -o /dev/null http://localhost:8080/helloDoc/users
# real ~1s

# Повторный — cache hit из Redis
time curl -s -o /dev/null http://localhost:8080/helloDoc/users
# real <100ms
```

## Альтернативные стенды

| Папка | Что внутри | Задание |
|---|---|---|
| `mongo-sharding/` | 2 шарда без репликации, без кэша | 2 |
| `mongo-sharding-repl/` | + 3 реплики на шард | 3 |
| `sharding-repl-cache/` | + Redis-кэш ★ | 4 |

В каждой подпапке — собственный `README.md` с подробной инструкцией.

## Архитектурные документы и схемы

- [diagrams/](diagrams/) — пять вариантов архитектурных схем (`.drawio`) и пояснения по эволюции стенда (задания 1, 5, 6).
- [architecture-final.drawio](architecture-final.drawio) — копия финальной схемы (variant 5: добавлен CDN).
- [ARCHITECTURE.md](ARCHITECTURE.md) — единый архитектурный документ для заданий 7–10:
  - **7** — схемы коллекций (`orders`, `products`, `carts`) и стратегии шардирования
  - **8** — выявление и устранение «горячих» шардов, метрики мониторинга
  - **9** — read preferences и стратегии консистентности
  - **10** — миграция отдельных сущностей на Apache Cassandra

## Остановка и очистка

```bash
cd sharding-repl-cache
docker compose down       # остановить
docker compose down -v    # + удалить тома с данными
```
