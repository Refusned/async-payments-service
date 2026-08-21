from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@postgres:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"

    api_key: str = "dev-secret-key"

    # Outbox relay
    outbox_poll_interval: float = 0.5
    outbox_batch_size: int = 100

    # Эмуляция платёжного шлюза
    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    failure_rate: float = 0.1

    # Доставка webhook и повторные попытки.
    # Первичная обработка + по одному повтору на каждую задержку из списка,
    # после последнего провала сообщение уходит в DLQ.
    webhook_timeout: float = 5.0
    retry_delays_seconds: list[int] = [2, 4, 8]

    @property
    def max_retries(self) -> int:
        return len(self.retry_delays_seconds)


settings = Settings()
