"""Приёмник webhook'ов для smoke-теста. Поднимается профилем `test`."""

from fastapi import FastAPI, Request

app = FastAPI(title="Webhook echo")
received: list[dict] = []


@app.post("/hook")
async def hook(request: Request) -> dict[str, bool]:
    received.append(await request.json())
    return {"ok": True}


@app.get("/received")
async def list_received() -> list[dict]:
    return received
