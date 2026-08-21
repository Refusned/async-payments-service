from dataclasses import dataclass

ATTEMPT_HEADER = "x-attempt"


@dataclass(frozen=True)
class Retry:
    """Повторить обработку через retry-очередь номер `attempt`."""

    attempt: int


@dataclass(frozen=True)
class Dead:
    """Повторы исчерпаны, сообщение уходит в DLQ."""


def next_route(retries_done: int, max_retries: int) -> Retry | Dead:
    """retries_done - сколько повторов уже было сделано (0 у нового сообщения)."""
    if retries_done < max_retries:
        return Retry(attempt=retries_done + 1)
    return Dead()


def current_retry(headers: dict) -> int:
    try:
        return int(headers.get(ATTEMPT_HEADER, 0))
    except (TypeError, ValueError):
        return 0
