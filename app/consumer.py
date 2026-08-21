import logging

from faststream import AckPolicy, FastStream, Logger
from faststream.exceptions import RejectMessage
from faststream.rabbit import RabbitBroker, RabbitMessage

from app.broker import declare_topology, exchange, new_queue, retry_queues
from app.config import settings
from app.db import session_factory
from app.processing import process_payment
from app.retry import ATTEMPT_HEADER, Dead, current_retry, next_route
from app.schemas import PaymentCreatedEvent

logging.basicConfig(level=logging.INFO)

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)


@app.after_startup
async def _declare() -> None:
    await declare_topology(broker)


@broker.subscriber(new_queue, exchange, ack_policy=AckPolicy.REJECT_ON_ERROR)
async def handle_payment_created(event: PaymentCreatedEvent, msg: RabbitMessage, logger: Logger) -> None:
    try:
        await process_payment(event.payment_id, session_factory)
    except Exception:
        logger.exception("payment %s processing failed", event.payment_id)
        await _reschedule(event, msg, logger)


async def _reschedule(event: PaymentCreatedEvent, msg: RabbitMessage, logger: Logger) -> None:
    route = next_route(current_retry(msg.headers), settings.max_retries)

    if isinstance(route, Dead):
        logger.error("payment %s: retries exhausted, moving to DLQ", event.payment_id)
        # У payments.new настроен x-dead-letter-exchange, поэтому reject отправляет
        # сообщение в payments.dlq вместе с историей отказов в заголовке x-death.
        raise RejectMessage() from None

    delay = settings.retry_delays_seconds[route.attempt - 1]
    logger.warning("payment %s: retry %s in %ss", event.payment_id, route.attempt, delay)
    await broker.publish(
        event.model_dump(mode="json"),
        queue=retry_queues[route.attempt - 1],
        exchange=exchange,
        headers={ATTEMPT_HEADER: str(route.attempt)},
        persist=True,
    )
