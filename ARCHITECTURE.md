# Архитектурный документ

Единый документ заданий 7–10 проектной работы «Мобильный мир».

## Содержание

- [Задание 7. Схемы коллекций и стратегии шардирования](#задание-7-схемы-коллекций-и-стратегии-шардирования)
- [Задание 8. Hot shards: метрики и стратегии устранения](#задание-8-hot-shards-метрики-и-стратегии-устранения)
- [Задание 9. Read preferences и консистентность](#задание-9-read-preferences-и-консистентность)
- [Задание 10. Миграция на Cassandra](#архитектурный-документ-миграция-данных-интернет-магазина-на-apache-cassandra)

---

# Задание 7. Схемы коллекций и стратегии шардирования

## Контекст

«Мобильный мир» вырос с аксессуаров до полноценного маркетплейса (электроника, аудио,
бытовая техника, книги). Данные хранятся в трёх ключевых коллекциях MongoDB:
`orders`, `products`, `carts`. Объёмы и нагрузка растут — нужно шардировать.

Цель документа — спроектировать схемы и выбрать **шард-ключи**, оптимизированные
под доминирующие операции каждой коллекции.

---

## Сводная таблица решений

| Коллекция | Шард-ключ | Тип | Почему |
|---|---|---|---|
| `orders` | `{ user_id: "hashed" }` | hashed | равномерная запись + targeted-чтение истории |
| `products` | `{ _id: "hashed" }` | hashed | targeted-апдейт остатков и страница товара |
| `carts` | `{ user_id: "hashed" }` | hashed | вся работа с корзиной владельца — на одном шарде |

Принципы выбора:
1. **Кардинальность** — много уникальных значений, иначе hot chunks.
2. **Равномерность распределения** — никаких супер-частых значений.
3. **Targeted queries** — частые запросы должны включать шард-ключ.
4. **Низкий write hotspot** — ключ не растёт монотонно (например, `created_at` плох).

---

## 1. Коллекция `orders`

### Схема документа

```js
{
  _id: ObjectId("..."),
  user_id: ObjectId("507f1f77bcf86cd799439011"),  // ← шард-ключ
  created_at: ISODate("2026-05-02T10:30:00Z"),
  items: [
    {
      product_id: ObjectId("..."),
      name: "Смартфон X 128GB",
      price: NumberDecimal("49990.00"),
      quantity: 1
    },
    {
      product_id: ObjectId("..."),
      name: "Чехол для Смартфон X",
      price: NumberDecimal("1490.00"),
      quantity: 2
    }
  ],
  status: "paid",          // pending | paid | shipped | delivered | cancelled
  total: NumberDecimal("52970.00"),
  geo_zone: "RU-MOW"
}
```

### Целевые операции и паттерны доступа

> **Правило:** во всех запросах по `_id` тащим `user_id` из контекста — иначе scatter-gather.

| # | Операция | Запрос | Доступ |
|---|---|---|---|
| 1 | Создать заказ | `insert({user_id, items, ...})` | targeted — пишем туда, куда хешируется user_id |
| 2 | История пользователя | `find({user_id}).sort({created_at: -1})` | targeted — все заказы юзера на одном шарде |
| 3 | Статус заказа | `find({_id, user_id})` | targeted — `user_id` берём из сессии |

### Анализ кандидатов в шард-ключи

| Кандидат | Кардинальность | Распределение | Targeted history | Проблемы |
|---|---|---|---|---|
| `{_id: "hashed"}` | очень высокая | идеальное | ✗ — история = scatter | scatter-gather на каждый просмотр истории |
| `{user_id: "hashed"}` | высокая | хорошее | ✓ | — |
| `{geo_zone: 1}` | низкая (~10) | **плохое** — Москва >> Калининград | ✗ | hot shard для Москвы |
| `{created_at: 1}` | высокая | плохое — **monotonic** | ✗ | "hot tail": все новые записи в один шард |
| `{user_id: 1, created_at: -1}` (compound) | высокая | хорошее | ✓ | сложнее, преимущество спорное при hashed |

### Выбор: `{ user_id: "hashed" }`

**Почему:**
- **История заказов** — это самый частый read-запрос (личный кабинет, страница «мои заказы»).
  Хеш по `user_id` гарантирует, что все заказы одного пользователя — на одном шарде. История загружается одним запросом.
- **Создание заказа** — каждый пользователь оформляет заказы независимо.
  Хеш равномерно распределяет write-нагрузку.
- **Нет hot users**: даже если у одного юзера миллион заказов — это всё равно один шард, нагрузка на него не критична (хеш разносит разных юзеров).
- **`geo_zone` отвергнут** — низкая кардинальность и неравномерность (Москва/Питер vs Калининград).

### Команды MongoDB

```js
sh.enableSharding("shop")

// Шард-ключ должен быть в индексе. Создаём составной индекс,
// который ещё и поддерживает запрос истории заказов.
db.orders.createIndex({ user_id: 1, created_at: -1 })

// Шардируем по hashed user_id.
sh.shardCollection("shop.orders", { user_id: "hashed" })

// Дополнительные индексы:
db.orders.createIndex({ _id: "hashed" })           // прямой доступ по _id (по необходимости)
db.orders.createIndex({ status: 1, created_at: -1 }) // operational dashboards
```

### Замечания о транзакциях

Создание заказа должно списывать остатки в `products`. Это **multi-document, multi-shard
transaction**. MongoDB 4.2+ поддерживает их:

```js
const session = client.startSession();
session.startTransaction();
try {
  db.products.updateOne({_id: pid}, {$inc: {"stock.$[z].quantity": -1}}, ..., {session});
  db.orders.insertOne({...}, {session});
  await session.commitTransaction();
} catch (e) {
  await session.abortTransaction();
}
```

Альтернатива — паттерн **Saga** или **outbox + eventual consistency**, если транзакции дороги.

---

## 2. Коллекция `products`

### Схема документа

```js
{
  _id: ObjectId("..."),                // ← шард-ключ
  name: "Смартфон X 128GB",
  category: "electronics/smartphones", // иерархическая
  price: NumberDecimal("49990.00"),
  stock: [
    { geo_zone: "RU-MOW", quantity: 50 },
    { geo_zone: "RU-EKB", quantity: 30 },
    { geo_zone: "RU-KGD", quantity: 0  }
  ],
  attributes: {
    color: "black",
    storage: "128GB",
    screen_inch: 6.5
  },
  created_at: ISODate(...),
  updated_at: ISODate(...)
}
```

### Целевые операции

| # | Операция | Запрос | Частота |
|---|---|---|---|
| 1 | Списание остатка при заказе | `update({_id}, {$inc: {"stock.$[z].quantity": -1}})` | очень высокая |
| 2 | Страница товара | `findOne({_id})` | очень высокая (с кэшем Redis) |
| 3 | Поиск по категории + цене | `find({category, price: {$gte, $lte}})` | высокая (но обычно через Elasticsearch) |
| 4 | Обновление каталога | `update({_id}, {$set: {price, ...}})` | низкая |

### Анализ кандидатов

| Кандидат | Targeted stock update | Targeted product page | Category browse | Проблемы |
|---|---|---|---|---|
| `{_id: "hashed"}` | ✓ | ✓ | scatter-gather | category-фильтр медленнее, но обычно идёт в ES + Redis |
| `{category: 1}` | ✗ (нужно знать category) | ✗ | targeted (или ranged) | низкая кардинальность, hot shard для популярных категорий |
| `{category: "hashed"}` | ✗ | ✗ | scatter | как `category`, но без локальности |

### Выбор: `{ _id: "hashed" }`

**Почему:**
- **Списание остатков** — самая частая операция в магазине. По `_id` идёт прямо в нужный шард, write-нагрузка распределена.
- **Страница товара** — большинство просмотров товара идут по ссылке `/products/<id>`. Targeted query.
- **Категорийный браузинг** в современных магазинах **обычно обслуживается отдельной поисковой системой** (Elasticsearch) с агрегатами и фасетами, которая индексирует MongoDB. Прямой `find({category})` — fallback и идёт в кэш Redis.
- **Низкая кардинальность `category`** делает её плохим шард-ключом: «Электроника» содержит 60% каталога — это hot shard.

### Команды MongoDB

```js
sh.enableSharding("shop")

// Дополнительные индексы для категорийного браузинга и фильтрации.
db.products.createIndex({ category: 1, price: 1 })
db.products.createIndex({ "stock.geo_zone": 1 })   // частичная доступность по регионам
db.products.createIndex({ name: "text" })           // полнотекстовый поиск (как fallback)

// Шардируем по hashed _id.
sh.shardCollection("shop.products", { _id: "hashed" })
```

### Альтернатива: zone sharding по гео

Если у магазина появятся **региональные дата-центры** (RU-Moscow, EU-Frankfurt и т.д.),
можно настроить **zone sharding** по `geo_zone`, чтобы московские товары физически лежали
в московских ДЦ:

```js
sh.addShardTag("shard-moscow",   "RU-MOW")
sh.addShardTag("shard-eu",       "EU-DE")
sh.updateZoneKeyRange("shop.products",
  { geo_zone: "RU-MOW" }, { geo_zone: "RU-MOW￿" }, "RU-MOW")
```

---

## 3. Коллекция `carts`

У коллекции **два режима владения** — гостевой (по `session_id`) и
пользовательский (по `user_id`).

### Схема документа (с унификацией владельца)

Чтобы оба режима шардировались по одному ключу, вводим конвенцию:
**`user_id` всегда заполнен**, для гостей это deterministic-строка вида `"guest:<session_id>"`.

```js
{
  _id: ObjectId("..."),
  user_id: "u:507f1f77bcf86cd799439011",  // ← шард-ключ
                                          // для гостя: "g:abc-session-xyz"
  session_id: "abc-session-xyz",          // только для гостей (для merge на логине)
  items: [
    { product_id: ObjectId("..."), quantity: 2 },
    { product_id: ObjectId("..."), quantity: 1 }
  ],
  status: "active",      // active | ordered | abandoned
  created_at: ISODate(...),
  updated_at: ISODate(...),
  expires_at: ISODate(...) // TTL — авто-удаление через N дней
}
```

> Префикс `g:`/`u:` обеспечивает разные хеш-классы для одних и тех же значений (например,
> если user_id и session_id случайно совпадут).

### Целевые операции

> **Правило:** во всех запросах с `_id` обязательно тащим `user_id` — иначе scatter-gather на все шарды.
> `user_id` есть в контексте сессии/JWT (для гостей — `"g:" + session_id`).

| # | Операция | Запрос | Доступ | Частота |
|---|---|---|---|---|
| 1 | Создать корзину | `insert({user_id: "g:..." \| "u:...", ...})` | targeted | при первом действии |
| 2 | Получить активную корзину | `findOne({user_id, status: "active"})` | targeted | каждое обращение к корзине |
| 3 | Добавить/заменить товар | `update({_id, user_id}, {$push/$set: {items}})` | targeted | часто |
| 4 | Удалить товар | `update({_id, user_id}, {$pull: {items}})` | targeted | часто |
| 5 | Слить guest → user | read guest, write user, mark guest abandoned | cross-shard tx | при логине |
| 6 | Отметить как ordered | `update({_id, user_id}, {$set: {status: "ordered"}})` | targeted | при оформлении |
| 7 | TTL очистка | автоматически по `expires_at` | local на каждом шарде | в фоне |

### Анализ кандидатов

| Кандидат | Get cart by owner | Update by _id | Merge guest→user | Проблемы |
|---|---|---|---|---|
| `{_id: "hashed"}` | scatter — owner не в ключе | targeted | targeted | каждое обращение `find by user_id` = scatter |
| `{user_id: "hashed"}` (с конвенцией) | ✓ | ✗ — нужно в запрос добавить user_id | cross-shard transaction (редко) | требует дисциплины с `g:`/`u:` префиксами |
| `{session_id: "hashed"}` | ✓ для гостей, ✗ для юзеров | ✗ | — | у залогиненного юзера session_id может быть пустым |
| `{user_id: 1, _id: 1}` | ✓ | ✓ (с user_id в запросе) | cross-shard | range-shard hot tail если ID растёт монотонно |

### Выбор: `{ user_id: "hashed" }`

**Почему:**
- **Получение активной корзины** (самая частая операция) — targeted, идёт в один шард.
- **Все операции с корзиной владельца** (add/remove/update) тоже targeted, если в запрос добавить `user_id`. Это нормально — клиент знает свой `user_id`/`session_id`.
- **TTL очистка** — каждый шард удаляет свою долю по локальному индексу `expires_at`.
- **Merge guest→user** = cross-shard transaction. Редкая операция (только при логине), MongoDB 4.2+ её поддерживает.
- **Конвенция `g:`/`u:`-префиксов** — единственная цена за унификацию. Реализуется в одном слое DAL.

### Команды MongoDB

```js
sh.enableSharding("shop")

// Индексы для частых запросов
db.carts.createIndex({ user_id: 1, status: 1 })       // get active cart
db.carts.createIndex({ session_id: 1 }, { sparse: true }) // для merge guest→user
db.carts.createIndex({ expires_at: 1 }, { expireAfterSeconds: 0 }) // TTL

// Шардирование
sh.shardCollection("shop.carts", { user_id: "hashed" })
```


## Cross-collection запросы

| Сценарий | Шарды | Решение |
|---|---|---|
| Создание заказа = списание остатка + insert order | `products` (по `_id`) + `orders` (по `user_id`) | multi-shard transaction (2 разных коллекции, разные шард-ключи) |
| Просмотр истории заказов | targeted на 1 шард `orders` | без особенностей |
| Просмотр товара + его остатков | targeted на 1 шард `products` | + Redis cache TTL=60s |
| Добавление в корзину | targeted на 1 шард `carts` | без транзакции |
| Login: merge guest cart | cross-shard в `carts` | transaction |

**Важно:** все cross-shard transaction-ы — операции, которые **нельзя оптимизировать
шард-ключом**, потому что данные принципиально разнесены (разные сущности). С этим
живём, минимизируя их количество (например, eventual consistency для остатков через
очередь, если строгие транзакции дороги).


---

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

---

# Задание 9. Read preferences и консистентность


### Read Preference (куда отправлять чтение)

| Значение | Поведение |
|---|---|
| `primary` *(default)* | только primary, всегда самые свежие данные |
| `primaryPreferred` | primary, при недоступности — secondary |
| `secondary` | только secondary, разгружает primary, может вернуть устаревшие данные |
| `secondaryPreferred` | secondary, при недоступности — primary |
| `nearest` | самая близкая по latency нода |

### `maxStalenessSeconds` — порог устаревания

Параметр, ограничивающий, насколько secondary может отставать от primary, чтобы быть выбран для чтения. Минимум — 90 секунд. Если секондари отстаёт сильнее — драйвер его не использует.

### Read Concern (какую гарантию чтения требовать)

| Concern | Что гарантирует |
|---|---|
| `local` *(default для primary)* | то, что видно в текущей ноде (может быть откачено) |
| `available` *(default для secondary)* | как `local`, но допускает orphans на шарде |
| `majority` | данные подтверждены большинством replica set (durable, не откатятся) |
| `linearizable` | сериализуемое чтение (только primary, медленно) |
| `snapshot` | snapshot-консистентность в транзакции |


---

## Сводная таблица решений

### Коллекция `products`

| Операция | Read Preference | maxStaleness | Read Concern | Обоснование |
|---|---|---|---|---|
| Страница товара (название, описание, фото) | `secondaryPreferred` | 60s | `local` | данные меняются редко, лаг 1 минута пользователя не задевает |
| Список / фильтрация по категории | `secondaryPreferred` | 60s | `local` | каталог редко обновляется, кэш на CDN/Redis |
| Цена при просмотре | `secondaryPreferred` | 30s | `local` | цена показывается «для справки», в корзине пересчитается |
| Остаток для UI («есть в наличии») | `secondaryPreferred` | 10s | `local` | оптимистично — финальная проверка всё равно на checkout |
| **Остаток при checkout (резервирование)** | **`primary`** | — | **`majority`** | риск oversell, нужны самые свежие данные + защита от отката |
| Списание остатка (write) | primary только (write) | — | `majority` writeConcern | гарантия durability при оплате |
| Поиск по тексту (`$text`) | `secondaryPreferred` | 60s | `local` | результаты поиска не критичны к секундной свежести |
| Admin: редактирование товара | `primary` | — | `majority` | оператор должен видеть свои изменения |
| Inventory dashboard (admin) | `secondaryPreferred` | 30s | `local` | аналитика, не операционка |

### Коллекция `orders`

| Операция | Read Preference | maxStaleness | Read Concern | Обоснование |
|---|---|---|---|---|
| Создать заказ (write) | primary только | — | `majority` writeConcern | заказ не должен потеряться |
| **Просмотр заказа сразу после создания** | **`primary`** | — | `majority` | классический «read your own writes»: пользователь только что нажал «оформить» |
| История заказов (общий список) | `secondaryPreferred` | 30s | `local` | пользователю не критична секундная свежесть; его последний заказ виден после редиректа |
| Просмотр статуса конкретного заказа | `primaryPreferred` | 10s | `local` | статус важен, но допустим секундный лаг |
| **Статус сразу после notification «отправлен»** | **`primary` или causal session** | — | `majority` | пользователь только что получил push — должен увидеть актуальный статус |
| Обновление статуса (write) | primary | — | `majority` | критично для бизнес-логики |
| Admin: список всех заказов | `secondaryPreferred` | 60s | `local` | оператор работает в окне времени, секундный лаг не страшен |
| Аналитика, отчёты | `secondary` | 5min | `local` | специально на тёплых secondary, разгрузка primary |

### Коллекция `carts`

| Операция | Read Preference | maxStaleness | Read Concern | Обоснование |
|---|---|---|---|---|
| **Получить активную корзину** | **`primary`** | — | `local` | пользователь сразу видит свои изменения, лаг = «куда делся товар?» |
| Добавить / удалить / заменить товар (write) | primary | — | `local` writeConcern | UX-критично |
| **Корзина при checkout** | **`primary`** | — | `majority` | основа для создания заказа, не должна быть устаревшей |
| Слияние guest → user при логине | primary (cross-shard tx) | — | `majority` | единственная транзакция, требует точности |
| TTL cleanup (фоновое задание) | secondary OK для read, primary для delete | — | `local` | не интерактивная операция |

---

## Принципы выбора

### 1. Любой write — это primary

Для записей вопрос read preference не стоит. Зато стоит вопрос **writeConcern**:
- `w: 1` — записал primary, не дожидается реплик. Быстро, но при падении primary до репликации запись потеряется.
- `w: "majority"` — primary + минимум 1 secondary. Чуть медленнее, но **durable**.
- `w: "majority", j: true` — ещё и в журнале. Самый надёжный.

Для критичных записей (`orders`, остатки `products`, checkout `carts`) — всегда `majority`.

### 2. Read your own writes (RYOW) — primary или causal consistency

Сценарий «пользователь нажал кнопку → редирект → нужно увидеть результат» — самый частый источник багов с secondary read.

Решения, в порядке простоты:

**a)** Конкретно эти чтения — на `primary`:
```python
order = await db.orders.find_one({"_id": id, "user_id": uid}, read_preference=ReadPreference.PRIMARY)
```

**b)** Causal consistency через сессию:

Сессия запоминает временную метку самого позднего события, в котором я участвовал, и при следующем чтении драйвер отправляет это: «дай данные, актуальные не раньше момента T». Если выбранный secondary ещё не догнал — драйвер ждёт.

Внутри сессии read after write ведёт себя так, как будто всё с primary, при этом нагрузка идёт на secondary.

**c)** Sticky-routing на уровне приложения: после write держим пользователя «прибитым» к primary на N секунд, далее запросы снова идут на secondary.


### 3. Допустимый replication lag — производная от UX

| Категория данных | Допустимый lag | Почему |
|---|---|---|
| **Деньги, остатки при checkout** | 0 (primary, majority) | риск oversell / двойная списания |
| **Только что записанные пользователем** | 0 (primary) или causal session | «куда делся мой заказ?» |
| **Статусы заказов в личном кабинете** | до 10 сек | пользователь готов «потянуть pull-to-refresh» |
| **Каталог товаров, описания** | 30–60 сек | оператор изменил — клиент увидит чуть позже |
| **Поиск, фильтрация** | до 1 минуты | новый товар в индексе появится через минуту — норма |
| **Аналитика, дашборды** | 5+ минут | не операционка, важна разгрузка primary |
| **Холодные исторические данные** | сколько угодно | архивные, вообще не пишутся |


---

## Резюме

> **Деньги и UX-критичные операции — `primary` + `majority`. Каталог и аналитика — `secondaryPreferred` с `maxStalenessSeconds` под бизнес-SLA. RYOW — либо primary, либо causal session.**

Конкретно для нашего магазина:
- **`carts` — почти всегда primary** (UX интерактивный, риск рассинхрона критичен)
- **`orders` — primary для свежих, secondary для истории** (с маркером свежести)
- **`products` — secondary почти везде, кроме checkout** (каталог чаще читают, чем пишут)

---

# Задание 10. Миграция на Cassandra: модель данных, стратегии репликации и шардирования

## 1. Контекст и обоснование миграции

В период пиковых нагрузок (около 50 000 запросов/сек) текущая архитектура хранения данных
на базе MongoDB с range-based шардированием демонстрирует значительные просадки latency.
Основная причина — необходимость массового перераспределения chunks между узлами при
расширении кластера: добавление нового шарда инициирует фоновое перемещение данных,
конкурирующее за ресурсы с продуктовой нагрузкой.

Для устранения данного ограничения предлагается частичная миграция в Apache Cassandra
со следующими целевыми характеристиками:

- **Отказоустойчивость** — leaderless-репликация без выделенного primary-узла.
- **Линейное горизонтальное масштабирование** — consistent hashing с virtual nodes
  обеспечивает локальное перераспределение данных, которые
  закрепляются за новым узлом.
- **Равномерное распределение нагрузки** — встроенная hash-партиционирование.
- **Multi-DC репликация** — `NetworkTopologyStrategy` без дополнительных надстроек.

### 1.1. Сравнение архитектурных характеристик

| Аспект | MongoDB (range-based sharding) | Apache Cassandra |
|---|---|---|
| Распределение данных | Chunks по диапазонам shard key, перераспределяемые балансировщиком | Consistent hashing с virtual nodes |
| Расширение кластера | Массовое перемещение chunks между всеми узлами | Перенос только диапазонов, закрепляемых за новым узлом |
| Модель лидерства | Replica set с primary/secondary | Leaderless  |
| Multi-DC репликация | Реализуется через zone sharding с ручной настройкой | Встроенная (`NetworkTopologyStrategy`) |
| Уровень репликации | Управляется через `writeConcern` | Tunable consistency level (CL) на уровне запроса |

### 1.2. Ограничения Apache Cassandra

При проектировании необходимо учитывать следующие архитектурные ограничения:

- Отсутствие операций `JOIN` и поддержки ad-hoc запросов; модель данных проектируется под заранее определённые паттерны доступа.
- Отсутствие полноценных транзакций между несколькими партициями.
- Отсутствие механизмов referential integrity.
- Eventual consistency как поведение по умолчанию (с возможностью усиления через consistency level).

Указанные ограничения исключают полную миграцию данных и требуют дифференцированного подхода: в Cassandra переносится только подмножество сущностей, соответствующее её модели использования.

---

## 2. Задание 10.1. Определение сущностей для миграции

### 2.1. Анализ сущностей

| Сущность | Профиль доступа | Паттерн запросов | Требование к транзакциям | Решение |
|---|---|---|---|---|
| `orders` | Write-once, частое чтение | По `user_id`, time-series | Списание остатка (вынесено) | Перенос в Cassandra |
| `order_events` | Append-only | По `order_id`, time-series | Не требуются | Перенос в Cassandra |
| `products` | Редкие writes, частые ad-hoc reads | Фильтрация по category, price, full-text | Списание остатка | Сохранение в MongoDB |
| `carts` | Частые updates, RYOW-семантика | По `user_id`, частые модификации | Merge guest → user | Сохранение в MongoDB |
| `user_sessions` | Append + TTL, key-value | По `session_id` | Не требуются | Перенос в Cassandra |
| `product_views` | Append-only, высокий объём | По `product_id` за период | Не требуются | Перенос в Cassandra |

### 2.2. Сущности, переносимые в Apache Cassandra

1. `orders` — заказы (исторические данные, минимальное количество модификаций после создания).
2. `order_events` — журнал изменений статусов заказов.
3. `product_views` — телеметрия просмотров товаров (clickstream).
4. `product_reviews` — отзывы пользователей.
5. `user_sessions` — активные пользовательские сессии.

### 2.3. Сущности, остающиеся в MongoDB

1. `products` — каталог товаров.
2. `carts` — пользовательские корзины.


### 2.4. Обоснование выбора

#### 2.4.1. Перенос `orders` в Cassandra

- Доминирующий паттерн доступа после создания заказа — чтение истории по `user_id`,
  что соответствует модели `partition by user_id` в Cassandra.
- Тело заказа после создания не модифицируется; изменения статуса вынесены в отдельный
  журнал событий (`order_events`).
- Высокая интенсивность записи в пиковые периоды (50 000 заказов/сек) согласуется
  с архитектурой LSM-tree, оптимизированной под write-heavy нагрузку.
- Multi-DC репликация на базе `NetworkTopologyStrategy` обеспечивает геораспределённое
  хранение без дополнительной инфраструктуры.
- Транзакционная связка «списание остатка + создание заказа» сохраняется на стороне MongoDB.
  Реплицирование в Cassandra `orders` производится асинхронно через outbox-паттерн или
  change streams.

#### 2.4.2. Сохранение `products` в MongoDB

- Запросы вида `WHERE category = ? AND price BETWEEN ? AND ? ORDER BY ...` несовместимы
  с моделью Cassandra без построения отдельных денормализованных таблиц под каждую
  комбинацию фильтров.
- Поддержка ad-hoc запросов аналитического характера требует наличия индексных структур
  (B-tree, multi-key, text), отсутствующих в Cassandra в эффективной реализации.


#### 2.4.3. Сохранение `carts` в MongoDB

- Read-your-own-writes семантика недостижима средствами leaderless-репликации
  Cassandra без существенной деградации latency (CL=ALL).
- Операции merge guest cart → user cart требуют атомарности и изоляции на уровне
  нескольких партиций; в Cassandra это реализуется только через Lightweight Transactions
  (Paxos), накладные расходы которых неприемлемы для интерактивного UX.
- Частые in-place модификации коллекций (`items`) приводят к накоплению tombstones
  и деградации производительности чтения
    - Любое удаление или замена коллекции создаёт tombstone, живущий минимум gc_grace_seconds. Cart-нагрузка (read-modify-write по коллекции, частые удаления) генерирует tombstones быстрее, чем compaction их удаляет, что приводит к деградации reads

---

## 3. Задание 10.2. Концептуальная модель данных

### 3.1. Принципы проектирования

Модель данных в Cassandra проектируется по принципу **query-first design**: схема таблицы
строится под заранее определённый паттерн запросов, а не под бизнес-сущность. Денормализация
является штатной практикой и не рассматривается как недостаток модели.

### 3.2. Структура первичного ключа

```cql
PRIMARY KEY ((partition_key), clustering_key_1, clustering_key_2, ...)
```

| Компонент | Назначение |
|---|---|
| Partition key | Определяет узел кластера, на котором размещаются данные (через hash → vnode → node). |
| Clustering keys | Задают порядок строк внутри партиции; используются для range-запросов. |

Запросы выполняются эффективно только при наличии в фильтре полного partition key. В противном случае инициируется полный обход кластера, что требует явного указания `ALLOW FILTERING` и не рекомендуется в production.

---

### 3.3. Таблица `orders_by_user` — история заказов пользователя

**Целевой запрос:**

```cql
SELECT * FROM orders_by_user WHERE user_id = ? ORDER BY created_at DESC LIMIT 50;
```

**Схема:**

```cql
CREATE TABLE shop.orders_by_user (
    user_id     uuid,
    created_at  timestamp,
    order_id    timeuuid,
    items       frozen<list<frozen<map<text, text>>>>,
    status      text,
    total       decimal,
    geo_zone    text,
    PRIMARY KEY ((user_id), created_at, order_id)
) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC);
```

| Элемент | Обоснование |
|---|---|
| Partition key = `user_id` | Все заказы одного пользователя локализованы на одной партиции, что обеспечивает targeted-чтение истории. |
| Clustering = `created_at DESC, order_id` | Эффективные range-запросы по времени; новые записи возвращаются первыми. |
| Тип `timeuuid` для `order_id` | Гарантирует уникальность и предоставляет естественную сортировку по времени создания. |
| `frozen<list<...>>` для `items` | Атомарное представление состава заказа; модификации не предполагаются. |


---

### 3.4. Таблица `orders_by_id` — точечный доступ к заказу

**Целевой запрос:**

```cql
SELECT * FROM orders_by_id WHERE order_id = ?;
```

**Схема:**

```cql
CREATE TABLE shop.orders_by_id (
    order_id    timeuuid PRIMARY KEY,
    user_id     uuid,
    created_at  timestamp,
    items       frozen<list<frozen<map<text, text>>>>,
    status      text,
    total       decimal,
    geo_zone    text
);
```

Таблица представляет собой денормализованную копию `orders_by_user`, обеспечивающую доступ к заказу по его идентификатору без указания `user_id`. При создании заказа запись производится в обе таблицы (синхронно средствами приложения).

Partition key = `order_id` (timeuuid) обеспечивает равномерное распределение за счёт хеш-функции при высокой кардинальности значений.

---

### 3.5. Таблица `order_events` — журнал изменений статуса

**Целевой запрос:** получение полной истории событий заказа.

```cql
CREATE TABLE shop.order_events (
    order_id    timeuuid,
    event_time  timestamp,
    event_type  text,
    actor       text,
    payload     map<text, text>,
    PRIMARY KEY ((order_id), event_time)
) WITH CLUSTERING ORDER BY (event_time DESC);
```

Размер партиции ограничен количеством событий жизненного цикла заказа (порядка десяти), что исключает риск hot partition.

---

### 3.6. Таблица `product_views` — телеметрия просмотров товаров

**Целевой запрос:** агрегация просмотров товара за период.

```cql
CREATE TABLE shop.product_views (
    product_id  uuid,
    day         date,
    viewed_at   timestamp,
    user_id     uuid,
    session_id  text,
    geo_zone    text,
    PRIMARY KEY ((product_id, day), viewed_at, user_id)
) WITH CLUSTERING ORDER BY (viewed_at DESC, user_id ASC)
  AND default_time_to_live = 7776000;  -- 90 суток
```

| Элемент | Обоснование |
|---|---|
| Composite partition `(product_id, day)` | Ограничивает размер партиции суточным окном просмотров одного товара. |
| Clustering `viewed_at DESC, user_id` | Range-запросы по времени и дедупликация просмотров одного пользователя. |
| TTL = 90 суток | Автоматическое удаление устаревших данных. |

**Митигация hot partition** для товаров с экстремальным трафиком (топ-позиции в период распродаж) — переход на часовое бакетирование:

```cql
PRIMARY KEY ((product_id, day, hour), viewed_at, user_id)
```

- Hot partitions контролируются через высококардинальные ключи и time-bucketing с параметрической настройкой ширины окна.
- Решардинг минимизирован за счёт consistent hashing + vnodes; добавление узла не приводит к массовому перемещению данных, как в MongoDB.

---

## 4. Задание 10.3. Стратегии обеспечения целостности данных

В отсутствие выделенного primary-узла Cassandra обеспечивает целостность данных тремя взаимодополняющими механизмами.

### 4.1. Hinted Handoff

**Назначение:** обеспечение eventual consistency при кратковременной недоступности реплики.

При недоступности одной из реплик-получателей координатор сохраняет write-операцию в виде hint и автоматически воспроизводит её после восстановления узла.

**Характеристики:**
- Не блокирует write-операции на координаторе.
- Имеет ограниченное окно хранения (по умолчанию 3 часа). По истечении окна hints отбрасываются, и восстановление целостности возлагается на anti-entropy repair.

### 4.2. Read Repair

**Назначение:** автоматическая синхронизация реплик в момент чтения.

При выполнении запроса с consistency level не ниже TWO координатор сравнивает ответы от реплик и инициирует синхронизацию устаревших копий — синхронно (BLOCKING) или асинхронно.

**Характеристики:**
- Восстанавливает целостность только для данных, которые активно читаются.
- Не обеспечивает синхронизацию холодных данных, не участвующих в запросах.

### 4.3. Anti-Entropy Repair

**Назначение:** полная сверка состояния реплик с использованием Merkle-tree сравнения.

**Характеристики:**
- Гарантирует синхронизацию всех данных, включая холодные и редко читаемые.
- Создаёт значительную нагрузку на CPU, диск и сеть; запуск планируется в окна минимальной нагрузки.

---

### 4.4. Дифференциация стратегий по сущностям

| Сущность | Hinted Handoff | Read Repair | Anti-Entropy Repair | Обоснование |
|---|---|---|---|---|
| `orders_by_user`, `orders_by_id` | Включён | BLOCKING | Еженедельно | Критичность данных, требование durability |
| `order_events` | Включён | BLOCKING | Еженедельно | Аудиты, связан с заказом |
| `product_views` | Включён | NONE | Ежемесячно | Высокий объём, низкая критичность точности |
| `product_reviews` | Включён | BLOCKING | Еженедельно | Пользовательский контент, репутационное значение |

### 4.5. Логика выбора стратегий

| Категория данных | Применяемая комбинация |
|---|---|
| Критичные операционные данные (заказы, отзывы) | Все три механизма; repair с высокой частотой |
| Массовые аналитические данные (clickstream) | Hinted Handoff включён, Read Repair выключен, Anti-Entropy с пониженной частотой |
| Часто читаемые данные с CL=QUORUM | Read Repair оставляется включённым (минимальный overhead) |
| Холодные исторические данные | Обязателен Anti-Entropy repair |



### 4.6. Конфигурация консистентности

| Сущность | Write CL | Read CL | Anti-Entropy Repair |
|---|---|---|---|
| `orders_*` | QUORUM | QUORUM | Еженедельно (full) |
| `order_events` | QUORUM | QUORUM | Еженедельно |
| `product_views` | ONE | ONE | Ежемесячно |
| `product_reviews` | QUORUM | QUORUM | Еженедельно |
| `user_sessions` | ONE | ONE | Не требуется (TTL) |
| `stock_changes_log` | QUORUM | ONE | Ежемесячно |

---

## 5. Архитектура интеграции с MongoDB

```
                     Application Layer (FastAPI)
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
       └─────────────┘                 │ reviews     |                                       
                                       └─────────────┘
```

Согласованность между MongoDB и Cassandra обеспечивается асинхронной репликацией событий через outbox-паттерн или change streams.

**Процедура создания заказа:**

1. На стороне MongoDB выполняется транзакция, включающая:
   - модификацию остатка в коллекции `products`;
   - создание документа в коллекции `orders`;
   - запись события в outbox-коллекцию.
2. Outbox-worker асинхронно считывает события и записывает их в соответствующие таблицы Cassandra.
3. Идемпотентность операций обеспечивается использованием `order_id` в качестве идентификатора события; повторное применение не приводит к дублированию данных.


## 6. Заключение

Предложенная архитектура решает задачу масштабирования при сохранении транзакционных гарантий для критичных операций:

- В Cassandra переносятся сущности с предсказуемыми паттернами доступа (read-by-known-key) и преимущественно append-only записью: `orders`, `order_events`, `product_views`, `product_reviews`
- В MongoDB сохраняются сущности, требующие гибкости запросов (`products`) и транзакционной семантики с RYOW (`carts`).
- Стратегии обеспечения целостности дифференцируются по уровню критичности данных: для операционных сущностей применяется полный набор механизмов с QUORUM consistency level; для аналитических — упрощённая конфигурация с приоритетом throughput.
- Интеграция MongoDB и Cassandra осуществляется через outbox-паттерн с обеспечением идемпотентности на стороне consumer.
