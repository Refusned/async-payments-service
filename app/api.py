import hashlib
import json
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_session
from app.models import OutboxEvent, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentCreatedEvent, PaymentOut

PAYMENT_CREATED = "payment.created"


async def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    # Отсутствующий и неверный ключ -> 401, а не 422 от валидатора заголовков.
    if x_api_key != settings.api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")


router = APIRouter(prefix="/api/v1", dependencies=[Depends(require_api_key)])


def _request_hash(body: PaymentCreate) -> str:
    # sort_keys: повтор запроса с переставленными ключами в metadata - тот же
    # запрос, а не ложный 409.
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def _by_idempotency_key(session: AsyncSession, key: str) -> Payment | None:
    return (await session.execute(select(Payment).where(Payment.idempotency_key == key))).scalar_one_or_none()


@router.post("/payments", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    response: Response,
    idempotency_key: str = Header(alias="Idempotency-Key", max_length=255),
    session: AsyncSession = Depends(get_session),
) -> PaymentAccepted:
    request_hash = _request_hash(body)

    existing = await _by_idempotency_key(session, idempotency_key)
    if existing is not None:
        return _replay(existing, request_hash, response)

    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        payment_metadata=body.metadata,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        webhook_url=str(body.webhook_url) if body.webhook_url else None,
    )
    session.add(payment)
    await session.flush()
    # Событие пишется той же транзакцией, что и платёж: outbox-релей опубликует
    # его в RabbitMQ только после коммита, потерять событие нельзя.
    session.add(
        OutboxEvent(
            aggregate_id=payment.id,
            event_type=PAYMENT_CREATED,
            payload=PaymentCreatedEvent(
                payment_id=payment.id, idempotency_key=idempotency_key
            ).model_dump(mode="json"),
        )
    )

    try:
        await session.commit()
    except IntegrityError:
        # Два одновременных запроса с одним Idempotency-Key: побеждает первый.
        await session.rollback()
        existing = await _by_idempotency_key(session, idempotency_key)
        if existing is None:
            raise
        return _replay(existing, request_hash, response)

    return PaymentAccepted(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


def _replay(existing: Payment, request_hash: str, response: Response) -> PaymentAccepted:
    if existing.request_hash != request_hash:
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "Idempotency-Key already used with a different request body",
        )
    response.headers["Idempotent-Replay"] = "true"
    return PaymentAccepted(
        payment_id=existing.id, status=existing.status, created_at=existing.created_at
    )


@router.get("/payments/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: uuid.UUID, session: AsyncSession = Depends(get_session)) -> Payment:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return payment
