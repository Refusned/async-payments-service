import asyncio
import logging
import random
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

from app.config import settings
from app.models import Payment, PaymentStatus

logger = logging.getLogger(__name__)


async def _locked_payment(session, payment_id: uuid.UUID) -> Payment | None:
    return (
        await session.execute(select(Payment).where(Payment.id == payment_id).with_for_update())
    ).scalar_one_or_none()


async def process_payment(payment_id: uuid.UUID, session_factory) -> None:
    """Обработать платёж и уведомить клиента.

    Две фазы, каждая под собственной блокировкой строки платежа, коммит между
    ними - чекпойнт: результат списания сохранён до попытки доставить webhook,
    и повтор после сбоя webhook'а не перерешивает судьбу платежа заново.
    Повторная доставка сообщения (outbox гарантирует лишь at-least-once) не
    спишет деньги дважды и не отправит webhook дважды: дубль ждёт на блокировке
    и перечитывает состояние уже после коммита первого исполнителя.
    Исключение наружу означает "повторить попытку позже" (см. consumer).
    """
    # Фаза 1: списание.
    async with session_factory() as session:
        # ponytail: блокировка держится на время эмуляции (2-5 c). При одном
        # consumer'е это дёшево; при нескольких эмуляцию нужно выносить за
        # транзакцию, оставив под блокировкой только смену статуса.
        payment = await _locked_payment(session, payment_id)
        if payment is None:
            logger.warning("payment %s not found, skipping", payment_id)
            return

        if payment.status is PaymentStatus.pending:
            await _charge(payment)
            await session.commit()
            logger.info("payment %s processed: %s", payment.id, payment.status.value)

    # Фаза 2: webhook. Проверка "уже доставлен?" и отметка о доставке идут
    # в одной транзакции под блокировкой, иначе дубль сообщения в зазоре
    # между коммитами отправил бы webhook второй раз.
    async with session_factory() as session:
        payment = await _locked_payment(session, payment_id)
        if payment is None:
            return

        if payment.webhook_url and payment.webhook_delivered_at is None:
            await _deliver_webhook(payment)
            payment.webhook_delivered_at = datetime.now(timezone.utc)
            await session.commit()
            logger.info("webhook for payment %s delivered", payment.id)


async def _charge(payment: Payment) -> None:
    """Эмуляция внешнего платёжного шлюза."""
    await asyncio.sleep(random.uniform(settings.processing_min_seconds, settings.processing_max_seconds))

    if random.random() < settings.failure_rate:
        payment.status = PaymentStatus.failed
        payment.failure_reason = "gateway declined the payment"
    else:
        payment.status = PaymentStatus.succeeded
    payment.processed_at = datetime.now(timezone.utc)


async def _deliver_webhook(payment: Payment) -> None:
    payload = {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata": payment.payment_metadata,
        "failure_reason": payment.failure_reason,
        "processed_at": payment.processed_at.isoformat() if payment.processed_at else None,
    }
    async with httpx.AsyncClient(timeout=settings.webhook_timeout) as client:
        response = await client.post(
            payment.webhook_url,
            json=payload,
            headers={"X-Payment-Id": str(payment.id)},
        )
        response.raise_for_status()
