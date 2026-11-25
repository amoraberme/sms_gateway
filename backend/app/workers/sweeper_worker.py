from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg

from app.config import get_settings
from app.observability.metrics import QUEUE_DEPTH
from app.queue.redis_scheduler import RedisScheduler


async def run_sweeper_once(pool: asyncpg.Pool, scheduler: RedisScheduler) -> dict[str, int]:
    """
    1. Promote due ACCEPTED messages (including scheduled_at <= now()) -> QUEUED.
    2. Reclaim expired Redis inflight leases.
    3. Re-enqueue due QUEUED messages whose lease expired or retry backoff elapsed.
    """
    now = datetime.now(timezone.utc)
    promoted = 0
    recovered = 0

    await scheduler.reclaim_expired_inflight()

    async with pool.acquire() as conn:
        async with conn.transaction():
            due_accepted = await conn.fetch(
                """
                SELECT id, tenant_id, priority::text AS priority
                FROM messages
                WHERE status = 'ACCEPTED'::message_status
                  AND COALESCE(scheduled_at, accepted_at) <= $1
                ORDER BY accepted_at ASC
                LIMIT 500
                FOR UPDATE SKIP LOCKED
                """,
                now,
            )
            if due_accepted:
                ids = [r["id"] for r in due_accepted]
                await conn.execute(
                    """
                    UPDATE messages
                    SET status = 'QUEUED'::message_status,
                        status_rank = 20,
                        queued_at = COALESCE(queued_at, $2),
                        updated_at = $2
                    WHERE id = ANY($1::uuid[]) AND status_rank < 20
                    """,
                    ids,
                    now,
                )
                await conn.execute(
                    """
                    INSERT INTO message_events (message_id, status, detail, occurred_at)
                    SELECT u.id, 'QUEUED'::message_status, 'Promoted to queue by sweeper', $2
                    FROM UNNEST($1::uuid[]) AS u(id)
                    """,
                    ids,
                    now,
                )
                promoted = len(due_accepted)

        due_queued = await conn.fetch(
            """
            SELECT id, tenant_id, priority::text AS priority
            FROM messages
            WHERE status = 'QUEUED'::message_status
              AND next_attempt_at <= $1
              AND (lease_expires_at IS NULL OR lease_expires_at < $1)
            ORDER BY priority ASC, next_attempt_at ASC
            LIMIT 500
            """,
            now,
        )

    if due_accepted:
        await scheduler.enqueue_many(
            [(str(r["id"]), str(r["tenant_id"]), r["priority"]) for r in due_accepted]
        )

    if due_queued:
        await scheduler.enqueue_many(
            [(str(r["id"]), str(r["tenant_id"]), r["priority"]) for r in due_queued]
        )
        recovered = len(due_queued)

    depths = await scheduler.queue_depths()
    QUEUE_DEPTH.labels(priority="high").set(depths["high"])
    QUEUE_DEPTH.labels(priority="normal").set(depths["normal"])

    return {"promoted": promoted, "recovered": recovered}


async def sweeper_worker_loop(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    stop_event: asyncio.Event,
) -> None:
    settings = get_settings()
    while not stop_event.is_set():
        try:
            await run_sweeper_once(pool, scheduler)
        except asyncio.CancelledError:
            break
        except Exception:
            pass
        await asyncio.sleep(settings.sweeper_interval_sec)
