"""End-to-end проверка поднятого окружения.

Запуск (после `docker compose --profile test up -d`):

    docker compose run --rm api python scripts/smoke.py

Скрипт работает изнутри docker-сети, поэтому на хосте ничего ставить не нужно.
"""

import asyncio
import os
import sys
import uuid

import httpx

API = "http://api:8000"
SINK = "http://webhook-echo:9000"
RABBIT = "http://rabbitmq:15672/api"
HEADERS = {"X-API-Key": os.getenv("API_KEY", "dev-secret-key")}

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'PASS' if ok else 'FAIL'}  {name}{f' :: {detail}' if detail else ''}", flush=True)
    if not ok:
        failures.append(name)


def body(amount: str = "199.99", webhook: str | None = None) -> dict:
    return {
        "amount": amount,
        "currency": "RUB",
        "description": "smoke test payment",
        "metadata": {"order_id": "A-1"},
        "webhook_url": webhook,
    }


async def dlq_depth(client: httpx.AsyncClient) -> int:
    response = await client.get(f"{RABBIT}/queues/%2F/payments.dlq", auth=("guest", "guest"))
    response.raise_for_status()
    return response.json()["messages"]


async def wait_for(predicate, timeout: float, interval: float = 1.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = await predicate()
        if result:
            return result
        await asyncio.sleep(interval)
    return None


async def main() -> int:
    async with httpx.AsyncClient(timeout=10) as client:
        # 1. Аутентификация
        response = await client.get(f"{API}/api/v1/payments/{uuid.uuid4()}")
        check("запрос без X-API-Key отклонён", response.status_code == 401, f"got {response.status_code}")

        response = await client.get(f"{API}/api/v1/payments/{uuid.uuid4()}", headers=HEADERS)
        check("несуществующий платёж -> 404", response.status_code == 404, f"got {response.status_code}")

        # 2. Создание платежа
        key = f"smoke-{uuid.uuid4()}"
        headers = {**HEADERS, "Idempotency-Key": key}
        response = await client.post(
            f"{API}/api/v1/payments", json=body(webhook=f"{SINK}/hook"), headers=headers
        )
        check("создание платежа -> 202", response.status_code == 202, f"got {response.status_code} {response.text}")
        payment_id = response.json()["payment_id"]
        check("ответ содержит статус pending", response.json()["status"] == "pending")

        # 3. Идемпотентность
        repeat = await client.post(
            f"{API}/api/v1/payments", json=body(webhook=f"{SINK}/hook"), headers=headers
        )
        check(
            "повтор с тем же Idempotency-Key -> тот же платёж",
            repeat.status_code == 202 and repeat.json()["payment_id"] == payment_id,
            f"got {repeat.status_code} {repeat.json().get('payment_id')}",
        )

        conflict = await client.post(
            f"{API}/api/v1/payments", json=body(amount="1.00", webhook=f"{SINK}/hook"), headers=headers
        )
        check(
            "тот же ключ с другим телом -> 409",
            conflict.status_code == 409,
            f"got {conflict.status_code}",
        )

        # 4. Асинхронная обработка и доставка webhook
        async def processed():
            data = (await client.get(f"{API}/api/v1/payments/{payment_id}", headers=HEADERS)).json()
            return data if data["status"] in ("succeeded", "failed") and data["webhook_delivered_at"] else None

        payment = await wait_for(processed, timeout=60)
        check("платёж обработан и webhook доставлен", payment is not None, "timeout 60s")
        if payment:
            check("метаданные сохранены", payment["metadata"] == {"order_id": "A-1"})

            delivered = (await client.get(f"{SINK}/received")).json()
            mine = [item for item in delivered if item["payment_id"] == payment_id]
            check("webhook доставлен ровно один раз", len(mine) == 1, f"got {len(mine)}")
            if mine:
                check("webhook несёт финальный статус", mine[0]["status"] == payment["status"])

        # 5. Retry + DLQ: webhook отвечает 404, сообщение должно осесть в DLQ
        depth_before = await dlq_depth(client)
        broken_key = f"smoke-dlq-{uuid.uuid4()}"
        response = await client.post(
            f"{API}/api/v1/payments",
            json=body(webhook=f"{SINK}/does-not-exist"),
            headers={**HEADERS, "Idempotency-Key": broken_key},
        )
        check("создан платёж с недоступным webhook", response.status_code == 202)

        async def in_dlq():
            return await dlq_depth(client) > depth_before

        check(
            "после исчерпания повторов сообщение в payments.dlq",
            await wait_for(in_dlq, timeout=120, interval=2) is not None,
            "timeout 120s",
        )

    print()
    if failures:
        print(f"FAILED: {len(failures)} -> {', '.join(failures)}")
        return 1
    print("ALL CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
