from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://sms_gateway:sms_gateway@127.0.0.1:5432/sms_gateway"
    db_pool_min_size: int = 2
    db_pool_max_size: int = 25
    redis_url: str = "redis://127.0.0.1:6379/0"
    dlr_secret: str = "dcc-shared-dlr-hmac-secret-2026"

    carrier_primary_url: str = "http://127.0.0.1:8001"
    carrier_secondary_url: str = "http://127.0.0.1:8002"

    gateway_global_rps_limit: int = 48
    dispatch_concurrency: int = 32
    max_submit_attempts: int = 4
    retry_base_delay_sec: float = 0.5
    carrier_timeout_sec: float = 5.0
    lease_ttl_sec: int = 15
    sweeper_interval_sec: float = 0.5
    webhook_poll_interval_sec: float = 0.5

    retryable_status_codes: tuple[int, ...] = (429, 503, 502, 504, 408)

    run_embedded_workers: bool = False
    run_migrations_on_startup: bool = True

    # Known seeded API keys for internal ops dashboard test-send convenience
    acme_api_key: str = "sk_live_acme_otp_key_01"
    shopfast_api_key: str = "sk_live_shopfast_bulk_key_01"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
