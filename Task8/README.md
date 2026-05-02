# Задание 8. Hot shards: метрики и стратегии устранения

## Контекст

Один из шардов перегружен: 70% запросов касались товаров категории «Электроника».
Документы могли быть распределены равномерно по количеству, но **нагрузка по запросам**
оказалась перекошенной. Нужно:
1. Выявлять hot shards заранее.
2. Иметь готовую процедуру для их разгрузки.

---

## Возможные причины hot shards

| Причина | Пример |
|---|---|
| **Плохой шард-ключ** — низкая кардинальность | `{category: 1}` → все «Электроника» на одном шарде |
| **Monotonic ключ** | `{created_at: 1}` или `{_id: 1}` (range) → все новые записи в один chunk → hot tail |
| **Hot data subset** | хеш по `_id` распределяет товары равномерно, но **70% запросов** идут к топ-100 товарам, которые случайно попали на один шард |
| **«Celebrity» эффект** | один пользователь генерирует 50% трафика → все его данные на одном шарде (если шард-ключ — `user_id`) |
| **Geo concentration** | шардирование по `country` → шард Russia огромный, шард Belarus пустой |
| **Балансировка по кол-ву chunks** | balancer ориентируется на **количество chunks**, а не на нагрузку. Маленькие, но горячие chunks остаются вместе |

В нашем кейсе виноват **последний пункт** — даже при `{_id: "hashed"}` балансировщик
смотрит на размер/количество, а не на QPS. Если 70% запросов идут к 5% документов,
то скос по запросам появляется случайно и balancer его не видит.

---

## Метрики мониторинга

### 1. Метрики уровня шарда

| Метрика | Источник | Порог alert | Что значит |
|---|---|---|---|
| `mongodb_op_counters_total{type=query}` | Prometheus mongodb_exporter | max/avg > 1.5× | дисбаланс QPS между шардами |
| `mongodb_op_counters_total{type=update}` | -//- | max/avg > 1.5× | дисбаланс write-нагрузки |
| `mongodb_op_latencies_latency` (p95) | -//- | > 100ms на одном шарде, < 20ms на других | один шард тормозит |
| `mongodb_connections_current` | -//- | > 80% от `maxIncomingConnections` | насыщение соединениями |
| `mongodb_global_lock_current_queue` | -//- | > 0 устойчиво | очередь блокировок — узкое место |
| CPU host | node_exporter | > 80% | железная загрузка |
| Disk I/O wait | node_exporter | > 30% | упёрлись в диск |
| Network egress | node_exporter | > 80% от пропускной | сетевой ботлнек |
| Replication lag (primary → secondary) | `rs.printReplicationInfo()` | > 10s | secondary не успевает |

### 2. Метрики уровня коллекции

| Метрика | Команда | Что значит |
|---|---|---|
| Распределение документов | `db.products.getShardDistribution()` | сколько данных и chunks на шарде |
| Распределение chunks | `sh.status()` или `config.chunks` | количество chunks по шардам |
| Миграции за период | `config.changelog` (фильтр по `what: "moveChunk.*"`) | сколько чанков двигалось — индикатор работы балансировщика |
| Slow queries | `db.system.profile` (с `setProfilingLevel(1, 100)`) | какие запросы тяжёлые и где |
| Targeted vs scatter ratio | `mongos` лог + парсинг | доля scatter-gather запросов |

### 3. Application-level метрики

MongoDB не видит **бизнес-нагрузку**, только техническую. Нужно добавить:

| Метрика | Где собирать | Зачем |
|---|---|---|
| QPS по `category` | в приложении (FastAPI middleware) | поймать «Электроника = 70% трафика» сразу |
| QPS по `product_id` (top-N) | приложение | заранее видеть hot products до того, как они уложат шард |
| Cache hit rate (Redis) | приложение / Redis INFO | низкий hit rate → нагрузка идёт в Mongo |
| Latency p99 по эндпоинтам | трейсы (OpenTelemetry) | привязать пользовательскую боль к шарду |

Без application-метрик невозможно понять, что hot shard вызван «горячим контентом», а не общей нагрузкой.

### 4. Ключевая производная метрика — **shard imbalance ratio**

```
imbalance_ratio = max(QPS по шардам) / avg(QPS по шардам)

1.0  → идеальный баланс
1.2  → нормально
1.5  → подозрительно
> 2.0 → проблема, требуется вмешательство
```

PromQL:
```
max(rate(mongodb_op_counters_total{type="query"}[5m])) by (cluster)
  /
avg(rate(mongodb_op_counters_total{type="query"}[5m])) by (cluster)
```

### 5. Дашборд (Grafana / Cloud Manager)

Минимальный набор панелей:

1. **QPS per shard** (line chart, по типам операций) — видны скосы
2. **Latency p95/p99 per shard** — какой шард лагает
3. **CPU / RAM / Disk** для каждого хоста — железные ограничения
4. **Chunks count per shard** — работа балансировщика
5. **Migration count** (за час) — сколько раз balancer двигал данные
6. **Top-N hot products** (бизнес-метрика) — какие конкретно товары горячие
7. **Cache hit rate (Redis)** — насколько кэш разгружает БД

---

## Стратегии устранения hot shards

### Уровень 1 — Оперативное реагирование

Когда шард уже горит и роняет SLA:

#### a) Перенаправить чтения на secondary

```js
// Применить ко всему клиенту
client.options.readPreference = "secondaryPreferred"
```

Снимает 50–70% read-нагрузки с primary мгновенно.

#### b) Включить агрессивный кеш на горячие товары

В Redis форсировать TTL побольше для популярных товаров:
```python
@cache(expire=300 if product_id in HOT_PRODUCTS else 60)
async def get_product(product_id): ...
```

#### c) Ручная миграция конкретного hot chunk

Если знаем конкретный chunk:
```js
sh.moveChunk("shop.products",
  { _id: ObjectId("...") },   // любой документ из chunk
  "shard3"                     // менее загруженный шард
)
```

Перевозит chunk на менее нагруженный шард. Не решает первопричину, но снимает остроту.

### Уровень 2 — Изменение шард-ключа

Если первопричина — плохой шард-ключ. **MongoDB 5.0+ поддерживает онлайн-resharding**:

```js
// Текущий ключ {category: 1} ушёл в hot shard, перешардируем по hashed _id
db.adminCommand({
  reshardCollection: "shop.products",
  key: { _id: "hashed" }
})

// Мониторим прогресс
db.adminCommand({ currentOp: 1 })
```

Resharding занимает время пропорциональное размеру коллекции (часы для миллиардов
документов), работает онлайн без даунтайма, использует временное удвоенное место на
дисках.

### Уровень 3 — Архитектурный

#### a) Кэш для hot data

Если 70% запросов к 5% товаров — это идеальный сценарий для Redis

#### b) Read-replicas пропорционально нагрузке

В горячий шард добавить дополнительные secondary специально для read offload:
- Обычный шард: 1 primary + 2 secondary
- Hot шард: 1 primary + 4–6 secondary с `readPreference: secondary`

#### c) Zone sharding (если есть гео-паттерн)

Если выявили географический скос — закрепить регионы за шардами:

```js
sh.addShardTag("shard-eu", "EU")
sh.addShardTag("shard-asia", "ASIA")
sh.updateZoneKeyRange("shop.products",
  { geo_zone: "RU-MOW" }, { geo_zone: "RU-MOW￿" }, "shard-eu")
```

#### d) Денормализация для горячих чтений

Создать отдельную коллекцию `hot_products_summary` с более выгодным шард-ключом
и обновлять её через change streams.

---

## Автоматизация перераспределения

### Встроенный балансировщик MongoDB

Работает по умолчанию, балансирует **по размеру/количеству chunks**. Не учитывает QPS!

Настройка окна работы:
```js
use config
db.settings.updateOne(
  { _id: "balancer" },
  { $set: { activeWindow: { start: "01:00", stop: "05:00" } } },
  { upsert: true }
)
```

Чтобы миграции не били по продакшену в час пик.

### Кастомный watchdog (рекомендация)

MongoDB не умеет балансировать по QPS. Можно поднять собственный сервис:

```python
# pseudo-code, запускается раз в 5 минут
async def hot_shard_watchdog():
    qps_per_shard = await prometheus.query(
        "rate(mongodb_op_counters_total[5m])"
    )
    avg = sum(qps_per_shard.values()) / len(qps_per_shard)
    hot = max(qps_per_shard, key=qps_per_shard.get)

    if qps_per_shard[hot] > 1.5 * avg:
        # 1. Найти топ-N hot chunks через config.changelog
        hot_chunks = find_hot_chunks_via_profiler(hot)
        # 2. Подвинуть на менее нагруженный шард
        cold = min(qps_per_shard, key=qps_per_shard.get)
        for chunk in hot_chunks[:3]:
            await mongo.adminCommand({
                "moveChunk": "shop.products",
                "find": chunk["min"],
                "to": cold
            })
        # 3. Алерт в Slack
        await slack.alert(f"Migrated {len(hot_chunks)} chunks from {hot} to {cold}")
```

---

## Полезные команды для диагностики

```js
// 1. Распределение документов по шардам для коллекции
db.products.getShardDistribution()
// Output:
//   Shard shard1 contains 33.4% data, 33.5% docs in cluster, ...
//   Shard shard2 contains 33.3% data, 33.2% docs in cluster, ...

// 2. История миграций за последний час
use config
db.changelog.find({
  what: { $regex: /^moveChunk/ },
  time: { $gte: new Date(Date.now() - 3600000) }
}).pretty()

// 3. Текущее состояние всего кластера
sh.status({ verbose: true })

// 4. Профилирование медленных запросов
db.setProfilingLevel(1, { slowms: 100 })
db.system.profile.find().sort({ ts: -1 }).limit(20)

// 5. Какие chunks на каком шарде
db.getSiblingDB("config").chunks.aggregate([
  { $match: { ns: "shop.products" } },
  { $group: { _id: "$shard", count: { $sum: 1 } } },
  { $sort: { count: -1 } }
])

// 6. Топ-операции (живой view)
// Из shell:
mongostat --host mongos_router:27017
mongotop  --host mongos_router:27017

// 7. Найти текущие тяжёлые операции
db.currentOp({ secs_running: { $gte: 5 }, op: "query" })
```


---

## Action items

1. **Снять метрику** — `db.products.getShardDistribution()` + Prometheus QPS-метрика для подтверждения дисбаланса.
2. **Проверить шард-ключ** — если `{category: 1}` или подобный, шардировать по `{_id: "hashed"}` через `reshardCollection`.
3. **Включить Redis-кэш** для топ-100 товаров «Электроники» с TTL=300s, прогревать при деплое.
4. **Перевести reads на secondary** через `readPreference: secondaryPreferred`.
5. **Добавить алерты** на `imbalance_ratio > 1.5` и `migrations > 50/h`.
6. **Поднять watchdog** для авто-миграции chunks при выявлении hot QPS.
7. **Application metrics** — измерять QPS по `category` и `product_id`, чтобы такое не повторилось.

Долгосрочно — рассмотреть выделение «Электроники» в отдельную коллекцию с собственной
стратегией шардирования, если объёмы продолжат расти.
