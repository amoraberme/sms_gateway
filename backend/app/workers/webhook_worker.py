from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json

import asyncpg
import httpx

from app.config import get_settings
from app.observability.metrics import WEBHOOK_DELIVERIES_TOTAL


async def process_pending_webhooks_once(
    pool: asyncpg.Pool,
    http_client: httpx.AsyncClient,
) -> int:
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                SELECT w.id, w.target_url, w.payload, w.attempt_count, w.max_attempts,
                       t.webhook_secret
                FROM webhook_deliveries w
                JOIN tenants t ON t.id = w.tenant_id
                WHERE w.status = 'PENDING' AND w.next_attempt_at <= $1
                ORDER BY w.next_attempt_at ASC
                LIMIT 20
                FOR UPDATE OF w SKIP LOCKED
                """,
                now,
            )

    if not rows:
        return 0

    for r in rows:
        raw_body = r["payload"] if isinstance(r["payload"], str) else json.dumps(r["payload"])
        raw_bytes = raw_body.encode("utf-8")
        secret = r["webhook_secret"] or ""
        sig = hmac.new(secret.encode("utf-8"), raw_bytes, hashlib.sha256).hexdigest()

        attempt = int(r["attempt_count"]) + 1
        success = False
        status_code: int | None = None
        err_msg: str | None = None

        try:
            resp = await http_client.post(
                r["target_url"],
                content=raw_bytes,
                headers={"Content-Type": "application/json", "X-Signature": sig},
            )
            status_code = resp.status_code
            success = 200 <= resp.status_code < 300
            if not success:
                err_msg = f"HTTP_{resp.status_code}"
        except Exception as exc:
            err_msg = type(exc).__name__

        done_at = datetime.now(timezone.utc)
        async with pool.acquire() as conn:
            if success:
                await conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET status = 'DELIVERED',
                        attempt_count = $1,
                        last_status_code = $2,
                        last_error = NULL,
                        updated_at = $3
                    WHERE id = $4
                    """,
                    attempt,
                    status_code,
                    done_at,
                    r["id"],
                )
                WEBHOOK_DELIVERIES_TOTAL.labels(status="delivered").inc()
            elif attempt >= int(r["max_attempts"]):
                await conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET status = 'FAILED',
                        attempt_count = $1,
                        last_status_code = $2,
                        last_error = $3,
                        updated_at = $4
                    WHERE id = $5
                    """,
                    attempt,
                    status_code,
                    err_msg,
                    done_at,
                    r["id"],
                )
                WEBHOOK_DELIVERIES_TOTAL.labels(status="failed").inc()
            else:
                backoff_sec = min(60, 2 ** (attempt - 1))
                next_at = done_at + timedelta(seconds=backoff_sec)
                await conn.execute(
                    """
                    UPDATE webhook_deliveries
                    SET attempt_count = $1,
                        next_attempt_at = $2,
                        last_status_code = $3,
                        last_error = $4,
                        updated_at = $5
                    WHERE id = $6
                    """,
                    attempt,
                    next_at,
                    status_code,
                    err_msg,
                    done_at,
                    r["id"],
                )
                WEBHOOK_DELIVERIES_TOTAL.labels(status="retry").inc()

    return len(rows)


async def webhook_worker_loop(
    pool: asyncpg.Pool,
    stop_event: asyncio.Event,
) -> None:
    settings = get_settings()
    async with httpx.AsyncClient(timeout=5.0) as client:
        while not stop_event.is_set():
            try:
                processed = await process_pending_webhooks_once(pool, client)
                if processed == 0:
                    await asyncio.sleep(settings.webhook_poll_interval_sec)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.5)
