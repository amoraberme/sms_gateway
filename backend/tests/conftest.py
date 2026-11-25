from __future__ import annotations

import asyncio
from decimal import Decimal
import hashlib
import hmac
import json
import os
import sys
from pathlib import Path

# Configure test DB & Redis BEFORE importing app modules
os.environ["DATABASE_URL"] = os.getenv(
    "TEST_DATABASE_URL", "postgresql://postgres:postgres@127.0.0.1:5433/postgres"
)
os.environ["REDIS_URL"] = os.getenv("TEST_REDIS_URL", "redis://127.0.0.1:6379/1")
os.environ["DB_POOL_MIN_SIZE"] = "1"
os.environ["DB_POOL_MAX_SIZE"] = "1"
os.environ["RUN_EMBEDDED_WORKERS"] = "false"
os.environ["RUN_MIGRATIONS_ON_STARTUP"] = "true"
os.environ["RETRY_BASE_DELAY_SEC"] = "0.05"
os.environ["SWEEPER_INTERVAL_SEC"] = "0.1"
os.environ["WEBHOOK_POLL_INTERVAL_SEC"] = "0.1"
os.environ["DLR_SECRET"] = "dcc-shared-dlr-hmac-secret-2026"

from app.config import get_settings

get_settings.cache_clear()

import asyncpg
import httpx
import pytest
import pytest_asyncio

from app.db.migrate import run_migrations
from app.db.pool import create_db_pool
from app.main import app
from app.queue.redis_scheduler import RedisScheduler

ACME_KEY = "sk_live_acme_otp_key_01"
SHOPFAST_KEY = "sk_live_shopfast_bulk_key_01"
DLR_SECRET = "dcc-shared-dlr-hmac-secret-2026"


def sign_dlr(payload: dict, secret: str = DLR_SECRET) -> tuple[bytes, str]:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
    return raw, sig


@pytest_asyncio.fixture
async def db_and_app():
    settings = get_settings()
    await run_migrations(settings.database_url)

    pool = await create_db_pool(settings.database_url, min_size=1, max_size=1)
    scheduler = RedisScheduler(settings.redis_url)
    await scheduler.init_scripts()
    await scheduler.redis.flushdb()

    # Reset mutable state between tests
    async with pool.acquire() as conn:
        await conn.execute(
            """
            TRUNCATE webhook_deliveries, dlr_receipts, ledger_entries,
                     message_events, messages, idempotency_keys RESTART IDENTITY CASCADE
            """
        )
        await conn.execute(
            """
            UPDATE tenants
            SET balance = CASE slug WHEN 'acme-bank' THEN 500.0000 ELSE 1000.0000 END,
                opening_balance = CASE slug WHEN 'acme-bank' THEN 500.0000 ELSE 1000.0000 END,
                webhook_url = NULL,
                is_active = TRUE
            """
        )
        await conn.execute(
            """
            UPDATE carriers SET base_url = CASE id
                WHEN 'carrier-primary' THEN 'http://127.0.0.1:8001'
                ELSE 'http://127.0.0.1:8002'
            END, is_active = TRUE
            """
        )

    app.state.db_pool = pool
    app.state.scheduler = scheduler

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client, pool, scheduler

    await scheduler.close()
    pool.terminate()
