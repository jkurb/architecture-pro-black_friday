# Задание 10. Миграция на Cassandra

## Контекст и мотивация перехода

MongoDB с range-based sharding в пик «чёрной пятницы» столкнулась с проблемой:
**добавление нового шарда триггерило массовое перемещение chunks** между нодами,
что в час пик уронило latency. Cassandra решает это иначе:

| Аспект | MongoDB (range-based shards) | Cassandra |
|---|---|---|
| Распределение данных | chunks по диапазонам шард-ключа, balancer перевозит | **consistent hashing** + virtual nodes (vnodes) |
| Добавление узла | massive rebalance — данные перемещаются между всеми | новые vnode «забирают» только свои диапазоны, остальные не двигаются |
| Лидерство | primary/secondary в replica set | **leaderless** (Dynamo-style), все ноды равны |
| Multi-DC | через zone sharding (ручной setup) | **встроенно**, NetworkTopologyStrategy |
| Репликация | sync majority writeConcern | tunable (CL.ONE/QUORUM/ALL) |

Однако Cassandra **не серебряная пуля**:
- ✗ нет JOIN, нет ad-hoc queries — данные моделируются под конкретные запросы
- ✗ нет multi-partition транзакций
- ✗ нет referential integrity
- ✗ eventual consistency по умолчанию (хотя tunable)

Поэтому **переезжать всё** в Cassandra — плохая идея. Нужно выбрать сущности,
которые подходят под её модель.

---

## Задание 10.1. Что переносим в Cassandra

### Анализ сущностей по критериям

| Сущность | Запись/Чтение | Паттерн доступа | Транзакции? | Кандидат? |
|---|---|---|---|---|
| **orders** (заказы) | write-once, read часто (по user) | `find by user_id`, time-series | сейчас да (списание остатка) | **✓ да** (с разделением tx-логики) |
| **order_events** (история статусов заказа) | append-only | `find by order_id`, time-series | нет | **✓ да** (идеально для C\*) |
| **products** (каталог) | редкие writes, частые ad-hoc reads | filter по category, price, name (text search) | да (write на остаток) | ✗ нет (ad-hoc queries в C\* плохи) |
| **carts** | частые updates, RYOW | `find by user_id`, частые модификации | да (merge guest→user) | ✗ нет (нужны транзакции и RYOW) |
| **user_sessions** | append + TTL, простой K-V | `find by session_id` | нет | **✓ да** (или Redis — оба подходят) |
| **product_views** (клики/просмотры) | append-only, очень много | `find by product_id, day` | нет | **✓ да** (идеально) |
| **product_reviews** | append, изредка edit | `find by product_id` time-sorted | нет | **✓ да** |
| **stock_changes_log** | append-only audit | `find by product_id` time-sorted | нет | **✓ да** |

### Решение

**В Cassandra переезжают** (преимущественно append-mostly данные):
1. `orders` — заказы (исторические, после создания почти не меняются)
2. `order_events` — лог изменений статуса заказов
3. `product_views` — clickstream / телеметрия просмотров
4. `product_reviews` — отзывы пользователей
5. `user_sessions` — активные сессии (с TTL)
6. `stock_changes_log` — аудит изменений остатков

**В MongoDB остаются** (нужны транзакции, RYOW, гибкие queries):
1. `products` — каталог (ad-hoc filters, full-text)
2. `carts` — корзины (interactive, RYOW)
3. Текущие остатки `stock` (в составе products)

### Обоснование

**Почему `orders` — да:**
- 99% операций после создания — это чтение истории по `user_id` (известный паттерн)
- Заказ почти не меняется после создания (статусы — отдельный лог)
- В пик Чёрной пятницы — 50K writes/сек: Cassandra с её LSM-tree это нормально
- Multi-DC репликация — заказ в Москве → реплицируется в EU
- Транзакция «списать остаток + записать заказ» останется в MongoDB:
  списание остатка происходит в MongoDB `products`, **затем** через outbox / change stream
  событие реплицируется в Cassandra `orders`

**Почему `products` — нет:**
- Запросы типа `WHERE category=X AND price BETWEEN A AND B ORDER BY price` —
  в Cassandra либо невозможны без секондарных индексов (которые медленные),
  либо требуют создания денормализованных таблиц под каждый запрос
- Поиск по тексту — отдельный мир (вынесен в Elasticsearch, см. Task 7)

**Почему `carts` — нет:**
- Интенсивные updates с RYOW-семантикой — Cassandra последнее ack-ed не гарантирует
- Транзакция merge guest→user — Cassandra не поддерживает multi-partition tx без LWT,
  а LWT медленные

---

## Задание 10.2. Модель данных

### Принцип Cassandra: **query-first design**

В Cassandra **таблицы строятся под конкретные запросы**, а не под сущности (как в SQL/Mongo).
Денормализация не порок, а норма.

### Partition key и clustering keys — ключевые понятия

```cql
PRIMARY KEY ((partition_key), clustering_key_1, clustering_key_2, ...)
```

| Часть | Что делает |
|---|---|
| **Partition key** | определяет, на какой ноде кластера лежат данные. Hash → vnode → нода |
| **Clustering keys** | задают порядок строк **внутри** партиции; по ним делаются range queries |

Главное правило: **запросы возможны только с указанным partition key** (иначе full scan).

---

### Таблица 1. `orders_by_user` — история заказов пользователя

**Запрос:** `SELECT * FROM orders_by_user WHERE user_id=? ORDER BY created_at DESC LIMIT 50`

```cql
CREATE TABLE shop.orders_by_user (
    user_id        uuid,
    created_at     timestamp,
    order_id       timeuuid,
    items          frozen<list<frozen<map<text, text>>>>,
    status         text,
    total          decimal,
    geo_zone       text,
    PRIMARY KEY ((user_id), created_at, order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);
```

| Что | Почему |
|---|---|
| **Partition key = `user_id`** | все заказы одного юзера на одной ноде → запрос истории = точечный read |
| **Clustering = `created_at DESC, order_id`** | свежие сверху, эффективные range queries «за последний месяц» |
| `timeuuid` для order_id | сортируется по времени и одновременно уникален |
| `frozen<list<...>>` для items | items как структура внутри документа (как в Mongo) |

**Hot partition риск:** активный пользователь с миллионом заказов → одна партиция огромная.
Мера: **time bucketing** для VIP/B2B-юзеров:

```cql
CREATE TABLE shop.orders_by_user_bucketed (
    user_id     uuid,
    year        int,            -- бакет: год создания
    created_at  timestamp,
    order_id    timeuuid,
    ...
    PRIMARY KEY ((user_id, year), created_at, order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);
```

Партиция `(user_id, year)` ограничена максимум ~365×N заказов в год.
Запрос за последние месяцы:
```cql
SELECT * FROM orders_by_user_bucketed
  WHERE user_id=? AND year IN (2025, 2026)
  ORDER BY created_at DESC LIMIT 50;
```

---

### Таблица 2. `orders_by_id` — точечный доступ к заказу

**Запрос:** `SELECT * FROM orders_by_id WHERE order_id=?`

```cql
CREATE TABLE shop.orders_by_id (
    order_id   timeuuid PRIMARY KEY,
    user_id    uuid,
    created_at timestamp,
    items      frozen<list<frozen<map<text, text>>>>,
    status     text,
    total      decimal,
    geo_zone   text
);
```

Это **денормализация** — данные дублируются с `orders_by_user`. Это нормально в Cassandra:
- partition key = `order_id` → равномерное распределение (random UUIDs хешируются равномерно)
- запрос «покажи мне заказ #123» работает без знания user_id

При создании заказа писать в **обе** таблицы (через batch или приложение).

---

### Таблица 3. `order_events` — лог изменений статуса

**Запрос:** «Покажи всю историю статусов заказа»

```cql
CREATE TABLE shop.order_events (
    order_id    timeuuid,
    event_time  timestamp,
    event_type  text,        -- created, paid, shipped, delivered, cancelled
    actor       text,        -- system | user | admin_user_id
    payload     map<text, text>,
    PRIMARY KEY ((order_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

| Что | Почему |
|---|---|
| Partition `order_id` | один заказ — мало событий (десятки), безопасный размер партиции |
| Clustering `event_time DESC` | самые свежие сверху |

---

### Таблица 4. `product_views` — клики (массовая телеметрия)

**Запрос:** «Сколько просмотров у товара X за последние 7 дней»

```cql
CREATE TABLE shop.product_views (
    product_id  uuid,
    day         date,           -- бакетирование по дню
    viewed_at   timestamp,
    user_id     uuid,
    session_id  text,
    geo_zone    text,
    PRIMARY KEY ((product_id, day), viewed_at, user_id)
) WITH CLUSTERING ORDER BY (viewed_at DESC, user_id ASC)
  AND default_time_to_live = 7776000;  -- 90 days TTL
```

| Что | Почему |
|---|---|
| Composite partition `(product_id, day)` | партиция = «просмотры товара за день», ограниченный размер |
| Clustering `viewed_at DESC, user_id` | range queries по времени + дедупликация по user |
| TTL 90 дней | автоудаление старых данных |

**Hot partition:** популярный товар в Чёрную пятницу — миллионы просмотров за день.
Если день — это слишком крупно, **бакет по часу**:

```cql
PRIMARY KEY ((product_id, day, hour), viewed_at, user_id)
```

---

### Таблица 5. `product_reviews` — отзывы

```cql
CREATE TABLE shop.product_reviews (
    product_id   uuid,
    review_id    timeuuid,
    user_id      uuid,
    rating       tinyint,        -- 1..5
    text         text,
    photos       list<text>,
    created_at   timestamp,
    PRIMARY KEY ((product_id), review_id)
) WITH CLUSTERING ORDER BY (review_id DESC);
```

`review_id` — `timeuuid` → сортировка по времени бесплатно, сразу уникальность.

---

### Таблица 6. `user_sessions` — короткоживущий K-V с TTL

```cql
CREATE TABLE shop.user_sessions (
    session_id  text PRIMARY KEY,
    user_id     uuid,
    created_at  timestamp,
    last_seen   timestamp,
    ip          text,
    user_agent  text
) WITH default_time_to_live = 86400;  -- 24h
```

Простой K-V case. Partition key = `session_id` — равномерное распределение.

---

### Таблица 7. `stock_changes_log` — аудит остатков

```cql
CREATE TABLE shop.stock_changes_log (
    product_id   uuid,
    geo_zone     text,
    changed_at   timestamp,
    delta        int,             -- +/- изменение
    new_quantity int,
    actor        text,            -- order_id | admin | warehouse_sync
    PRIMARY KEY ((product_id, geo_zone), changed_at)
) WITH CLUSTERING ORDER BY (changed_at DESC);
```

Partition `(product_id, geo_zone)` — изменения остатка конкретного товара в конкретном регионе.

---

### Replication strategy и keyspace

```cql
CREATE KEYSPACE shop WITH REPLICATION = {
  'class': 'NetworkTopologyStrategy',
  'DC_RU': 3,        -- 3 реплики в датацентре RU
  'DC_EU': 3,        -- 3 реплики в датацентре EU
  'DC_ASIA': 2       -- 2 в Азии
};
```

`NetworkTopologyStrategy` — это и есть «встроенная multi-DC репликация»,
ради которой шли в Cassandra.

---

### Сводка партиционирования

| Таблица | Partition key | Clustering | Risk hot partition | Mitigation |
|---|---|---|---|---|
| `orders_by_user` | `user_id` | `created_at DESC, order_id` | VIP-юзер | bucketed table for них |
| `orders_by_id` | `order_id` (timeuuid) | — | низкий (random UUIDs) | — |
| `order_events` | `order_id` | `event_time DESC` | низкий | — |
| `product_views` | `(product_id, day)` | `viewed_at DESC, user_id` | hot product | bucket by hour |
| `product_reviews` | `product_id` | `review_id DESC` | very popular product | bucket by year |
| `user_sessions` | `session_id` | — | низкий (random) | — |
| `stock_changes_log` | `(product_id, geo_zone)` | `changed_at DESC` | hot product+zone | bucket by month |

---

## Задание 10.3. Стратегии целостности

В Cassandra нет primary — все ноды равны (leaderless). Как же поддерживается консистентность?
Через **три механизма**, работающих параллельно:

### 1. Hinted Handoff — «отдадим, когда вернёшься»

**Сценарий:** одна из реплик упала, write пришёл с CL=QUORUM, остальные подтвердили.

Координатор сохраняет **подсказку (hint)** — write, предназначенный для упавшей ноды.
Когда нода возвращается, координатор «допроигрывает» эти hints.

```yaml
# cassandra.yaml
hinted_handoff_enabled: true
max_hint_window_in_ms: 10800000    # 3 часа: дольше — забываем, нужен repair
hinted_handoff_throttle_in_kb: 1024
max_hints_delivery_threads: 2
```

**Плюсы:** не блокирует writes, дёшево, eventual consistency.
**Минусы:** при долгом outage hints отбрасываются → запись потеряется.

### 2. Read Repair — «лечим во время чтения»

**Сценарий:** запрос с CL≥TWO. Координатор спрашивает несколько реплик, видит расхождение,
**синхронно** (foreground repair) или асинхронно (background) обновляет отстающие реплики.

Настройка на уровне таблицы:

```cql
ALTER TABLE shop.orders_by_user WITH read_repair = 'BLOCKING';
-- BLOCKING (synchronous), NONE
```

**Плюсы:** автоматически выравнивает данные, которые **читаются**.
**Минусы:** холодные данные (которые никто не читает) остаются неконсистентными.

### 3. Anti-Entropy Repair (`nodetool repair`) — «полная сверка»

**Сценарий:** периодически (раз в неделю/месяц) запускается **Merkle-tree сравнение**
всех реплик; расхождения отправляются нодам, отстающим.

```bash
# Полный repair всех keyspaces на этой ноде
nodetool repair --full

# Incremental repair (только то, что не было в прошлом repair) — быстрее
nodetool repair -inc

# Конкретная таблица
nodetool repair shop product_reviews
```

**Плюсы:** **гарантированная** консистентность. Лечит и холодные данные.
**Минусы:** **дорого** — нагрузка на CPU/диск/сеть; нужно планировать на off-peak hours.

---

### Выбор стратегии по сущностям

| Сущность | Hinted Handoff | Read Repair | Anti-Entropy | Comments |
|---|---|---|---|---|
| **`orders_by_user/by_id`** | ✓ enabled | ✓ BLOCKING на каждом read с CL=QUORUM | ✓ еженедельно | критичные данные, нужна гарантия |
| **`order_events`** | ✓ enabled | ✓ BLOCKING | ✓ еженедельно | связаны с заказом, важная аудиторская дорожка |
| **`product_views`** | ✓ enabled | ✗ NONE (мусорная нагрузка для аналитики) | ✓ ежемесячно | объём огромный, точность секунды не критична |
| **`product_reviews`** | ✓ enabled | ✓ BLOCKING | ✓ еженедельно | пользовательский контент, видимость репутации |
| **`user_sessions`** | ✓ enabled | ✗ NONE | ✗ нет (TTL = 24h, само очистится) | TTL короче чем repair-период |
| **`stock_changes_log`** | ✓ enabled | ✗ NONE | ✓ ежемесячно | аудит, точность по операционке не нужна |

### Логика выбора

| Если... | Тогда |
|---|---|
| Данные критичны (заказы, отзывы) | все 3 стратегии включены, repair часто |
| Данные массовые и аналитические (clickstream) | Hinted Handoff + Read Repair OFF + Anti-Entropy реже |
| Данные с TTL короче периода repair (sessions) | только Hinted Handoff |
| Данные часто читаются с CL=QUORUM | Read Repair работает «бесплатно», оставляем |
| Холодные исторические данные | обязательно Anti-Entropy repair (Read Repair их не лечит) |

---

### Как Read Repair взаимодействует с Consistency Level

```
RF (Replication Factor) = 3
Coordinator получает SELECT с CL=QUORUM (нужно 2 ack)

      ┌─→ replica A: вернул value@T=1004
      ↓
SELECT├─→ replica B: вернул value@T=1003 (отстал)
      ↓
      └─→ replica C: вернул value@T=1004

Coordinator берёт самый свежий (T=1004), возвращает клиенту
        ↓
Background read repair: отсылает write на B чтобы догнал
```

При CL=ONE Read Repair не работает (опросили только 1 реплику, сравнить не с чем).
**Чем выше CL — тем активнее лечатся данные при чтении.**

---

### Тонкая настройка: Consistency Level per query

Это **главная сила** Cassandra — CL выбирается **на каждый запрос**:

```python
from cassandra import ConsistencyLevel

# Запись заказа: durability важна, пишем на QUORUM
session.execute(insert_stmt, consistency_level=ConsistencyLevel.QUORUM)

# Чтение clickstream: latency важнее точности, ONE
session.execute(select_views_stmt, consistency_level=ConsistencyLevel.ONE)

# Финансовое чтение (например, баланс): максимум — ALL
session.execute(select_balance_stmt, consistency_level=ConsistencyLevel.ALL)
```

| CL | Кол-во ack | Когда |
|---|---|---|
| ONE | 1 | clickstream, аналитика |
| QUORUM | (RF/2 + 1) | **default для прод-данных** — заказы, отзывы |
| LOCAL_QUORUM | quorum в **своём DC** | multi-DC, когда кросс-DC дорог |
| ALL | все RF | редко, только аудит |
| EACH_QUORUM | quorum в **каждом DC** | сильная multi-DC консистентность |

---

### Сводка writeConcern × readConcern × repair

```
┌────────────┬─────────────┬────────────────┬─────────────┐
│ Сущность   │ Write CL    │ Read CL        │ Repair      │
├────────────┼─────────────┼────────────────┼─────────────┤
│ orders     │ QUORUM      │ QUORUM         │ weekly full │
│ events     │ QUORUM      │ QUORUM         │ weekly      │
│ views      │ ONE         │ ONE            │ monthly     │
│ reviews    │ QUORUM      │ QUORUM         │ weekly      │
│ sessions   │ ONE         │ ONE            │ none (TTL)  │
│ stock log  │ QUORUM      │ ONE (audit)    │ monthly     │
└────────────┴─────────────┴────────────────┴─────────────┘
```

---

## Архитектурная схема

```
                     Application (FastAPI)
                            │
            ┌───────────────┴───────────────┐
            ▼                               ▼
     ┌─────────────┐                 ┌─────────────┐
     │  MongoDB    │                 │  Cassandra  │
     │  (sharded)  │                 │ (multi-DC)  │
     ├─────────────┤                 ├─────────────┤
     │ products    │                 │ orders_*    │
     │ carts       │  ── outbox →    │ order_events│
     │ stock       │  (CDC events)   │product_views│
     └─────────────┘                 │ reviews     │
                                     │ sessions    │
                                     │ stock_log   │
                                     └─────────────┘
```

`outbox` / **change streams** обеспечивают eventual consistency между двумя БД.
Создание заказа:
1. MongoDB: транзакция `update products + insert orders + insert outbox_events`
2. Outbox worker: читает события, пишет в Cassandra `orders_*`
3. При сбое — retry, идемпотентность по `order_id`

---

## TL;DR

> **В Cassandra переезжает то, что append-mostly и read-by-known-key:** заказы, события, clickstream, отзывы, сессии. Каталог и корзины остаются в MongoDB.
>
> **Партиционирование по доминирующему запросу:** `user_id` для истории, `(product_id, day)` для clickstream, time-bucketing для возможных hot partitions.
>
> **Все три стратегии целостности — включены**, но weight зависит от критичности: для заказов и отзывов — QUORUM + еженедельный repair; для clickstream и сессий — CL.ONE + только Hinted Handoff.
