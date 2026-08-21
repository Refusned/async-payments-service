import asyncio
import logging
from datetime import datetime, timezone

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.broker import exchange, new_queue
from app.config import settings
from app.models import OutboxEvent
from app.retry import ATTEMPT_HEADER

logger = logging.getLogger(__name__)


async def publish_pending(session: AsyncSession, broker: RabbitBroker) -> int:
    """Опубликовать порцию неотправленных событий. Возвращает число отправленных."""
    events = (
        (
            await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(settings.outbox_batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )

    for event in events:
        await broker.publish(
            event.payload,
            queue=new_queue,
            exchange=exchange,
            headers={ATTEMPT_HEADER: "0"},
            message_id=str(event.id),
            persist=True,
        )
        event.published_at = datetime.now(timezone.utc)

    await session.commit()
    return len(events)


async def relay_loop(broker: RabbitBroker, session_factory) -> None:
    """Фоновый релей outbox -> RabbitMQ.

    Гарантия at-least-once: publish идёт до коммита published_at, поэтому падение
    между ними приведёт к повторной публикации. Дубли безвредны, consumer идемпотентен.
    """
    while True:
        published = 0
        try:
            async with session_factory() as session:
                published = await publish_pending(session, broker)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("outbox relay iteration failed")
            await asyncio.sleep(settings.outbox_poll_interval)
        if published == 0:
            await asyncio.sleep(settings.outbox_poll_interval)
