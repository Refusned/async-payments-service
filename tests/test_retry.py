from app.retry import ATTEMPT_HEADER, Dead, Retry, current_retry, next_route


def test_retry_ladder_then_dlq() -> None:
    max_retries = 3
    assert next_route(0, max_retries) == Retry(attempt=1)
    assert next_route(1, max_retries) == Retry(attempt=2)
    assert next_route(2, max_retries) == Retry(attempt=3)
    assert next_route(3, max_retries) == Dead()
    assert next_route(9, max_retries) == Dead()


def test_current_retry_reads_header() -> None:
    assert current_retry({}) == 0
    assert current_retry({ATTEMPT_HEADER: "2"}) == 2
    assert current_retry({ATTEMPT_HEADER: 2}) == 2
    assert current_retry({ATTEMPT_HEADER: "broken"}) == 0


def test_retry_queues_cover_every_retry() -> None:
    from app.broker import retry_queues
    from app.config import settings

    assert len(retry_queues) == settings.max_retries
    assert settings.retry_delays_seconds == sorted(settings.retry_delays_seconds)
