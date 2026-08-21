# Асинхронный сервис процессинга платежей

Микросервис принимает запросы на оплату, обрабатывает их асинхронно через эмуляцию
платёжного шлюза и уведомляет клиента webhook'ом.

Стек: FastAPI + Pydantic v2, SQLAlchemy 2.0 (async) + PostgreSQL, RabbitMQ через FastStream,
Alembic, Docker Compose.

## Как это устроено

```
POST /api/v1/payments
      |
      | одна транзакция: payments + outbox
      v
 [PostgreSQL] --- outbox-релей (фоновая задача API) ---> [exchange payments] --> payments.new
                                                                                     |
                                                                                 [consumer]
                                                    эмуляция шлюза 2-5 c (90% успех / 10% отказ)
                                                    обновление статуса -> отправка webhook
                                                                                     |
                                            ошибка -> payments.retry.1/2/3 (TTL 2/4/8 c) -> payments.new
                                            повторы исчерпаны -> reject -> payments.dlx -> payments.dlq
```

Ключевые решения:

- **Outbox.** Платёж и событие `payment.created` пишутся одной транзакцией, так что событие
  не может потеряться при падении между коммитом и публикацией. Релей читает неопубликованные
  строки `SELECT ... FOR UPDATE SKIP LOCKED` и публикует их в RabbitMQ. Гарантия at-least-once:
  publish идёт до отметки `published_at`, поэтому возможен дубль - consumer к этому готов.
- **Идемпотентность на входе.** `Idempotency-Key` - уникальный индекс в БД. Повтор с тем же
  ключом и тем же телом возвращает уже созданный платёж (заголовок `Idempotent-Replay: true`),
  повтор с тем же ключом и другим телом - `409 Conflict`. Гонка двух параллельных запросов
  разрешается через `IntegrityError` на коммите.
- **Идемпотентность на обработке.** Состояние платежа в БД служит чекпойнтом: повторная доставка
  сообщения не спишет деньги второй раз (`status != pending`) и не отправит webhook дважды
  (`webhook_delivered_at`). Обработка идёт двумя фазами - списание и webhook, - каждая берёт
  строку платежа `FOR UPDATE` и перепроверяет состояние под блокировкой, поэтому дубль
  сообщения безопасен даже при конкурентной обработке.
- **Retry.** Задержка перед повтором делается временем жизни сообщения в отдельной retry-очереди
  без потребителей: истёк TTL - брокер сам вернул сообщение в `payments.new`. По очереди на каждую
  задержку (2, 4, 8 c), а не одна общая с per-message TTL, потому что в общей очереди сообщение
  с длинной задержкой задержало бы все следующие за ним.
- **DLQ.** После третьего неудачного повтора сообщение отклоняется (`RejectMessage`), и, поскольку
  у `payments.new` настроен `x-dead-letter-exchange`, попадает в `payments.dlq` вместе с историей
  отказов в заголовке `x-death`.
- **Аутентификация.** Статический ключ в `X-API-Key` на всех эндпоинтах `/api/v1/*`.
  Отсутствующий или неверный ключ - `401`.

## Запуск

```bash
cp .env.example .env      # при необходимости поменять порты и API_KEY
docker compose up -d --build
```

Поднимаются `postgres`, `rabbitmq`, `api` (применяет миграции и стартует), `consumer`.

- API: http://localhost:8000, Swagger: http://localhost:8000/docs
- RabbitMQ UI: http://localhost:15672 (guest / guest)

Если порты заняты, задать другие в `.env`: `API_PORT`, `POSTGRES_PORT`, `RABBITMQ_PORT`,
`RABBITMQ_UI_PORT`.

## Примеры

Создание платежа:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: dev-secret-key" \
  -H "Idempotency-Key: order-42" \
  -H "Content-Type: application/json" \
  -d '{
        "amount": "199.99",
        "currency": "RUB",
        "description": "Заказ №42",
        "metadata": {"order_id": "42", "user_id": "7"},
        "webhook_url": "https://example.com/hooks/payments"
      }'
```

```
HTTP/1.1 202 Accepted

{"payment_id":"66edea0c-da36-473d-b90c-6b6c5fffe30e","status":"pending","created_at":"2026-08-21T11:06:50.501870Z"}
```

Получение платежа:

```bash
curl http://localhost:8000/api/v1/payments/66edea0c-da36-473d-b90c-6b6c5fffe30e \
  -H "X-API-Key: dev-secret-key"
```

```json
{
  "id": "66edea0c-da36-473d-b90c-6b6c5fffe30e",
  "amount": "199.99",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": "42", "user_id": "7"},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "https://example.com/hooks/payments",
  "failure_reason": null,
  "created_at": "2026-08-21T11:06:50.501870Z",
  "processed_at": "2026-08-21T11:06:54.401234Z",
  "webhook_delivered_at": "2026-08-21T11:06:54.409876Z"
}
```

Тело webhook'а, который получает клиент:

```json
{
  "payment_id": "66edea0c-da36-473d-b90c-6b6c5fffe30e",
  "status": "succeeded",
  "amount": "199.99",
  "currency": "RUB",
  "description": "Заказ №42",
  "metadata": {"order_id": "42", "user_id": "7"},
  "failure_reason": null,
  "processed_at": "2026-08-21T11:06:54.401234Z"
}
```

## Проверка

Юнит-тест политики повторов:

```bash
docker compose run --rm api sh -c "pip install -q pytest && python -m pytest -q"
```

Сквозная проверка поднятого окружения (аутентификация, идемпотентность, обработка, доставка
webhook'а, лестница повторов и DLQ). Профиль `test` поднимает приёмник webhook'ов:

```bash
docker compose --profile test up -d --build
docker compose run --rm api python scripts/smoke.py
```

```
PASS  запрос без X-API-Key отклонён :: got 401
PASS  несуществующий платёж -> 404 :: got 404
PASS  создание платежа -> 202
PASS  ответ содержит статус pending
PASS  повтор с тем же Idempotency-Key -> тот же платёж
PASS  тот же ключ с другим телом -> 409
PASS  платёж обработан и webhook доставлен
PASS  метаданные сохранены
PASS  webhook доставлен ровно один раз :: got 1
PASS  webhook несёт финальный статус
PASS  создан платёж с недоступным webhook
PASS  после исчерпания повторов сообщение в payments.dlq

ALL CHECKS PASSED
```

Лестница повторов в логах consumer'а (webhook отвечает 404):

```
11:06:58 WARNING payment 1b97a707...: retry 1 in 2s
11:07:00 WARNING payment 1b97a707...: retry 2 in 4s
11:07:04 WARNING payment 1b97a707...: retry 3 in 8s
11:07:12 ERROR   payment 1b97a707...: retries exhausted, moving to DLQ
```

## Конфигурация

| Переменная | По умолчанию | Назначение |
|---|---|---|
| `API_KEY` | `dev-secret-key` | Значение заголовка `X-API-Key` |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@postgres:5432/payments` | Подключение к PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | Подключение к RabbitMQ |
| `RETRY_DELAYS_SECONDS` | `[2, 4, 8]` | Задержки повторов; длина списка = число повторов до DLQ |
| `FAILURE_RATE` | `0.1` | Доля отказов в эмуляции шлюза |
| `PROCESSING_MIN_SECONDS` / `PROCESSING_MAX_SECONDS` | `2.0` / `5.0` | Длительность эмуляции |
| `WEBHOOK_TIMEOUT` | `5.0` | Таймаут доставки webhook'а |
| `OUTBOX_POLL_INTERVAL` | `0.5` | Пауза релея, когда outbox пуст |

## Структура

```
app/
  api.py          эндпоинты, аутентификация, идемпотентность
  broker.py       топология RabbitMQ (exchange, retry-очереди, DLQ)
  config.py       настройки
  consumer.py     потребитель payments.new и маршрутизация повторов
  db.py           async engine и сессии
  main.py         FastAPI + запуск outbox-релея
  models.py       payments, outbox
  outbox.py       релей outbox -> RabbitMQ
  processing.py   эмуляция шлюза и доставка webhook'а
  retry.py        политика повторов
alembic/          миграции
scripts/smoke.py  сквозная проверка окружения
tools/            приёмник webhook'ов для smoke-теста
tests/            юнит-тесты
```

## Осознанные упрощения

- Outbox-релей живёт внутри процесса API, а не отдельным сервисом: при нескольких репликах API
  `FOR UPDATE SKIP LOCKED` не даст им публиковать одни и те же события.
- Эмуляция шлюза выполняется под блокировкой строки платежа. При одном consumer'е (как в задании)
  это дёшево и защищает от дублей; при масштабировании consumer'ов эмуляцию нужно выносить
  за транзакцию, оставив в ней только смену статуса.
- Webhook отправляется без подписи запроса - в задании этого нет.
