from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import httpx

from app.carriers.client import CarrierClient
from app.carriers.router import CarrierRouter
from app.config import get_settings
from app.observability.metrics import CARRIER_SUBMITS_TOTAL
from app.queue.redis_scheduler import RedisScheduler
from app.services.dispatch_service import (
    handle_pre_acceptance_failure,
    mark_submitted_and_correlate_early_dlr,
)


async def process_one_message(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    router: CarrierRouter,
    carrier_client: CarrierClient,
    msg_id_str: str,
) -> None:
    settings = get_settings()
    msg_id = UUID(msg_id_str)
    now = datetime.now(timezone.utc)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, to_number, from_sender, body, encoding::text AS encoding,
                   status_rank, attempt_count, next_attempt_at
            FROM messages
            WHERE id = $1
            """,
            msg_id,
        )
        if row is None or row["status_rank"] >= 30:
            await scheduler.ack_inflight(msg_id_str)
            return

        if row["next_attempt_at"] > now:
            # Not due for retry yet; release lease so sweeper re-enqueues when due
            await scheduler.ack_inflight(msg_id_str)
            return

        lease_until = now + timedelta(seconds=settings.lease_ttl_sec)
        await conn.execute(
            "UPDATE messages SET lease_expires_at = $1 WHERE id = $2 AND status_rank < 30",
            lease_until,
            msg_id,
        )

    candidates = await router.select_candidates(row["to_number"])
    if not candidates:
        await handle_pre_acceptance_failure(
            pool, scheduler, msg_id, "none", "NO_CARRIER_ROUTE", permanent=True
        )
        await scheduler.ack_inflight(msg_id_str)
        return

    # On retry after transient error, prefer healthy secondary candidate if available
    attempt_idx = int(row["attempt_count"])
    target = candidates[attempt_idx % len(candidates)] if len(candidates) > 1 and attempt_idx > 0 else candidates[0]

    # Enforce global 50 RPS cap via distributed Redis Lua sliding window
    while True:
        wait_ms = await scheduler.acquire_rate_limit_slot(
            carrier_id=target.id,
            global_limit=settings.gateway_global_rps_limit,
            carrier_limit=min(target.rate_limit_rps, settings.gateway_global_rps_limit),
        )
        if wait_ms <= 0:
            break
        await asyncio.sleep(wait_ms / 1000.0)

    try:
        resp = await carrier_client.submit(
            base_url=target.base_url,
            message_id=msg_id_str,
            to_number=row["to_number"],
            from_sender=row["from_sender"],
            text=row["body"],
            encoding=row["encoding"],
        )
    except httpx.TimeoutException:
        CARRIER_SUBMITS_TOTAL.labels(carrier=target.id, status_code="timeout").inc()
        await router.record_failure(target.id)
        await handle_pre_acceptance_failure(
            pool, scheduler, msg_id, target.id, "CARRIER_TIMEOUT", permanent=False
        )
        await scheduler.ack_inflight(msg_id_str)
        return
    except httpx.RequestError as exc:
        CARRIER_SUBMITS_TOTAL.labels(carrier=target.id, status_code="network_error").inc()
        await router.record_failure(target.id)
        await handle_pre_acceptance_failure(
            pool, scheduler, msg_id, target.id, f"CARRIER_CONN_ERROR: {type(exc).__name__}", permanent=False
        )
        await scheduler.ack_inflight(msg_id_str)
        return

    CARRIER_SUBMITS_TOTAL.labels(carrier=target.id, status_code=str(resp.status_code)).inc()

    if resp.status_code == 200 and resp.carrier_id:
        await router.record_success(target.id)
        await mark_submitted_and_correlate_early_dlr(
            pool, scheduler, msg_id, target.id, resp.carrier_id
        )
        await scheduler.ack_inflight(msg_id_str)
        return

    if resp.status_code == 400:
        await handle_pre_acceptance_failure(
            pool,
            scheduler,
            msg_id,
            target.id,
            resp.error or "INVALID_DESTINATION",
            permanent=True,
        )
        await scheduler.ack_inflight(msg_id_str)
        return

    if resp.status_code in settings.retryable_status_codes:
        if resp.status_code >= 500:
            await router.record_failure(target.id)
        await handle_pre_acceptance_failure(
            pool,
            scheduler,
            msg_id,
            target.id,
            f"HTTP_{resp.status_code}:{resp.error or ''}",
            permanent=False,
        )
        await scheduler.ack_inflight(msg_id_str)
        return

    # Unexpected non-retryable 4xx
    await handle_pre_acceptance_failure(
        pool, scheduler, msg_id, target.id, f"HTTP_{resp.status_code}", permanent=True
    )
    await scheduler.ack_inflight(msg_id_str)


async def dispatch_worker_loop(
    worker_idx: int,
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    router: CarrierRouter,
    carrier_client: CarrierClient,
    stop_event: asyncio.Event,
) -> None:
    settings = get_settings()
    lease_ttl_ms = settings.lease_ttl_sec * 1000
    while not stop_event.is_set():
        try:
            msg_id_str = await scheduler.pop_fair(lease_ttl_ms=lease_ttl_ms)
            if msg_id_str is None:
                await asyncio.sleep(0.02)
                continue
            await process_one_message(pool, scheduler, router, carrier_client, msg_id_str)
        except asyncio.CancelledError:
            break
        except Exception:
            await asyncio.sleep(0.1)
