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

