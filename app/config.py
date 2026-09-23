from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str = "postgresql://adwaiteklavya@localhost:5432/queue_system"
    # async DSN derived; psycopg handles both
    lease_seconds: int = 8  # short lease for 7s reclaim SLA with heartbeat
    reaper_interval_seconds: float = 2.0
    poll_interval_seconds: float = 0.2
    worker_id: str = "worker-1"
    max_pool_size: int = 20


settings = Settings()
