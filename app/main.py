import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator

from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

from app.api import router
from app.broker import declare_topology
from app.config import settings
from app.db import session_factory
from app.outbox import relay_loop

logging.basicConfig(level=logging.INFO)

broker = RabbitBroker(settings.rabbitmq_url)


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    await broker.connect()
    await declare_topology(broker)
    relay = asyncio.create_task(relay_loop(broker, session_factory))
    try:
        yield
    finally:
        relay.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await relay
        await broker.stop()


app = FastAPI(title="Async Payments Service", version="1.0.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
