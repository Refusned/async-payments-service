"""Топология RabbitMQ: основная очередь, лестница retry-очередей, DLQ.

    payments (direct)
      |-- payments.new        --(reject/ttl)--> payments.dlx --> payments.dlq
      |-- payments.retry.1    x-message-ttl=2s  --dlx--> payments / payments.new
      |-- payments.retry.2    x-message-ttl=4s  --dlx--> payments / payments.new
      |-- payments.retry.3    x-message-ttl=8s  --dlx--> payments / payments.new

Задержка перед повтором делается временем жизни сообщения в retry-очереди:
у этих очередей нет потребителей, по истечении TTL брокер сам возвращает
сообщение в payments.new. Отдельная очередь на каждую задержку, а не общая с
per-message TTL, потому что в общей очереди сообщение с длинной задержкой
задержало бы все следующие за ним (head-of-line blocking).
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import settings

EXCHANGE_NAME = "payments"
DLX_NAME = "payments.dlx"
NEW_ROUTING_KEY = "payments.new"
DEAD_ROUTING_KEY = "payments.dead"

exchange = RabbitExchange(EXCHANGE_NAME, type=ExchangeType.DIRECT, durable=True)
dlx = RabbitExchange(DLX_NAME, type=ExchangeType.DIRECT, durable=True)

new_queue = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DLX_NAME,
        "x-dead-letter-routing-key": DEAD_ROUTING_KEY,
    },
)

dead_queue = RabbitQueue("payments.dlq", durable=True, routing_key=DEAD_ROUTING_KEY)


def retry_routing_key(attempt: int) -> str:
    return f"payments.retry.{attempt}"


retry_queues = [
    RabbitQueue(
        retry_routing_key(attempt),
        durable=True,
        routing_key=retry_routing_key(attempt),
        arguments={
            "x-message-ttl": delay * 1000,
            "x-dead-letter-exchange": EXCHANGE_NAME,
            "x-dead-letter-routing-key": NEW_ROUTING_KEY,
        },
    )
    for attempt, delay in enumerate(settings.retry_delays_seconds, start=1)
]


async def declare_topology(broker: RabbitBroker) -> None:
    """Объявить обменники, очереди и привязки. Идемпотентно: вызывается и API, и consumer."""
    payments_exchange = await broker.declare_exchange(exchange)
    dead_exchange = await broker.declare_exchange(dlx)

    for queue in (new_queue, *retry_queues):
        declared = await broker.declare_queue(queue)
        await declared.bind(payments_exchange, routing_key=queue.routing_key)

    declared_dlq = await broker.declare_queue(dead_queue)
    await declared_dlq.bind(dead_exchange, routing_key=DEAD_ROUTING_KEY)
