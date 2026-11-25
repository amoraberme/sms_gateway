from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import random
from uuid import UUID

import asyncpg

from app.config import get_settings
from app.observability.metrics import DLR_DELAY_SECONDS
from app.queue.redis_scheduler import RedisScheduler


def compute_backoff_seconds(attempt_number: int, base_delay_sec: float) -> float:
    """Exponential backoff with jitter for 429/503/timeout retries."""
    return base_delay_sec * (2 ** max(0, attempt_number - 1)) + random.uniform(0.0, 0.25)


async def mark_submitted_and_correlate_early_dlr(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    message_id: UUID,
    carrier_used: str,
    carrier_id: str,
) -> None:
    """
    Persist carrier_id + SUBMITTED state under pg_advisory_xact_lock('dlr:' || carrier_id),
    and immediately correlate any early DLR already buffered in dlr_receipts.
    """
    now = datetime.now(timezone.utc)
    final_status: str | None = None
    tenant_id_str: str | None = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"dlr:{carrier_id}")

            msg = await conn.fetchrow(
                """
                SELECT m.id, m.tenant_id, m.status::text AS status, m.status_rank,
                       m.to_number, m.from_sender, m.client_ref, t.webhook_url
                FROM messages m
                JOIN tenants t ON t.id = m.tenant_id
                WHERE m.id = $1
                FOR UPDATE OF m
                """,
                message_id,
            )
            if msg is None:
                return

            tenant_id_str = str(msg["tenant_id"])

            if msg["status_rank"] < 30:
                await conn.execute(
                    """
                    UPDATE messages
                    SET status = 'SUBMITTED'::message_status,
                        status_rank = 30,
                        carrier_used = $1,
                        carrier_id = $2,
                        attempt_count = attempt_count + 1,
                        submitted_at = COALESCE(submitted_at, $3),
                        lease_expires_at = NULL,
                        last_error = NULL,
                        updated_at = $3
                    WHERE id = $4 AND status_rank < 30
                    """,
                    carrier_used,
                    carrier_id,
                    now,
                    message_id,
                )
                await conn.execute(
                    """
                    INSERT INTO message_events (message_id, status, detail, metadata, occurred_at)
                    VALUES ($1, 'SUBMITTED'::message_status, $2, $3::jsonb, $4)
                    """,
                    message_id,
                    f"Submitted to {carrier_used}",
                    json.dumps({"carrier_used": carrier_used, "carrier_id": carrier_id}),
                    now,
                )
                final_status = "SUBMITTED"

            # Check if an out-of-order DLR (~5%) already arrived while /submit was in flight
            early_dlr = await conn.fetchrow(
                """
                SELECT id, raw_status, mapped_status::text AS mapped_status
                FROM dlr_receipts
                WHERE carrier_id = $1 AND processed_at IS NULL
                ORDER BY id ASC
                LIMIT 1
                FOR UPDATE
                """,
                carrier_id,
            )
            if early_dlr is not None:
                mapped = early_dlr["mapped_status"]
                await conn.execute(
                    """
                    UPDATE messages
                    SET status = $1::message_status,
                        status_rank = 40,
                        finalized_at = $2,
                        updated_at = $2
                    WHERE id = $3 AND status_rank < 40
                    """,
                    mapped,
                    now,
                    message_id,
                )
                await conn.execute(
                    """
                    INSERT INTO message_events (message_id, status, detail, metadata, occurred_at)
                    VALUES ($1, $2::message_status, $3, $4::jsonb, $5)
                    """,
                    message_id,
                    mapped,
                    f"Correlated early DLR: {early_dlr['raw_status']}",
                    json.dumps({"carrier_id": carrier_id, "raw_status": early_dlr["raw_status"], "early": True}),
                    now,
                )
                await conn.execute(
                    """
                    UPDATE dlr_receipts
                    SET message_id = $1, processed_at = $2
                    WHERE id = $3
                    """,
                    message_id,
                    now,
                    early_dlr["id"],
                )
                if msg["webhook_url"]:
                    wh_payload = {
                        "message_id": str(message_id),
                        "status": mapped,
                        "to": msg["to_number"],
                        "from": msg["from_sender"],
                        "client_ref": msg["client_ref"],
                        "carrier_id": carrier_id,
                        "finalized_at": now.isoformat(),
                    }
                    await conn.execute(
                        """
                        INSERT INTO webhook_deliveries (tenant_id, message_id, target_url, payload)
                        VALUES ($1, $2, $3, $4::jsonb)
                        ON CONFLICT (message_id) DO NOTHING
                        """,
                        msg["tenant_id"],
                        message_id,
                        msg["webhook_url"],
                        json.dumps(wh_payload),
                    )
                final_status = mapped
                DLR_DELAY_SECONDS.observe(0.0)

    if final_status and tenant_id_str:
        await scheduler.publish_event(
            {
                "type": "message_updated",
                "message_id": str(message_id),
                "tenant_id": tenant_id_str,
                "status": final_status,
            }
        )


async def handle_pre_acceptance_failure(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    message_id: UUID,
    carrier_used: str,
    error_reason: str,
    permanent: bool,
) -> bool:
    """
    Either schedule retry (if not permanent and attempts < max_attempts)
    or atomically mark FAILED (status_rank=40) and issue single REFUND ledger entry.
    Returns True if finalized as FAILED, False if scheduled for retry.
    """
    settings = get_settings()
    now = datetime.now(timezone.utc)
    finalized = False
    tenant_id_str: str | None = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            msg = await conn.fetchrow(
                """
                SELECT m.id, m.tenant_id, m.status_rank, m.attempt_count, m.max_attempts,
                       m.total_cost, m.to_number, m.from_sender, m.client_ref, t.webhook_url
                FROM messages m
                JOIN tenants t ON t.id = m.tenant_id
                WHERE m.id = $1
                FOR UPDATE OF m
                """,
                message_id,
            )
            if msg is None or msg["status_rank"] >= 30:
                return False

            tenant_id_str = str(msg["tenant_id"])
            new_attempt = int(msg["attempt_count"]) + 1
            should_fail = permanent or new_attempt >= int(msg["max_attempts"])

            if not should_fail:
                delay_sec = compute_backoff_seconds(new_attempt, settings.retry_base_delay_sec)
                next_at = now + timedelta(seconds=delay_sec)
                await conn.execute(
                    """
                    UPDATE messages
                    SET attempt_count = $1,
                        last_error = $2,
                        carrier_used = $3,
                        next_attempt_at = $4,
                        lease_expires_at = NULL,
                        updated_at = $5
                    WHERE id = $6 AND status_rank < 30
                    """,
                    new_attempt,
                    error_reason,
                    carrier_used,
                    next_at,
                    now,
                    message_id,
                )
                await conn.execute(
                    """
                    INSERT INTO message_events (message_id, status, detail, metadata, occurred_at)
                    VALUES ($1, 'QUEUED'::message_status, $2, $3::jsonb, $4)
                    """,
                    message_id,
                    f"Retry {new_attempt}/{msg['max_attempts']} scheduled after {error_reason}",
                    json.dumps({"attempt": new_attempt, "carrier": carrier_used, "delay_sec": round(delay_sec, 3)}),
                    now,
                )
                return False

            # Permanent failure or exhausted retries before carrier acceptance -> FAILED + REFUND
            tenant_row = await conn.fetchrow(
                "SELECT balance FROM tenants WHERE id = $1 FOR UPDATE",
                msg["tenant_id"],
            )
            current_balance = Decimal(tenant_row["balance"])
            refund_cost = Decimal(msg["total_cost"]).quantize(Decimal("0.0001"))
            new_balance = (current_balance + refund_cost).quantize(Decimal("0.0001"))

            await conn.execute(
                """
                UPDATE messages
                SET status = 'FAILED'::message_status,
                    status_rank = 40,
                    attempt_count = $1,
                    carrier_used = $2,
                    last_error = $3,
                    lease_expires_at = NULL,
                    finalized_at = $4,
                    updated_at = $4
                WHERE id = $5 AND status_rank < 30
                """,
                new_attempt,
                carrier_used,
                error_reason,
                now,
                message_id,
            )
            await conn.execute(
                """
                INSERT INTO message_events (message_id, status, detail, metadata, occurred_at)
                VALUES ($1, 'FAILED'::message_status, $2, $3::jsonb, $4)
                """,
                message_id,
                f"Failed pre-acceptance ({error_reason}); balance refunded",
                json.dumps({"attempt": new_attempt, "carrier": carrier_used, "permanent": permanent}),
                now,
            )

            refund_row = await conn.fetchrow(
                """
                INSERT INTO ledger_entries (
                    tenant_id, message_id, entry_type, amount, balance_after, description, created_at
                )
                VALUES ($1, $2, 'REFUND'::ledger_entry_type, $3, $4, $5, $6)
                ON CONFLICT (message_id, entry_type) DO NOTHING
                RETURNING id
                """,
                msg["tenant_id"],
                message_id,
                -refund_cost,
                new_balance,
                f"Refund for pre-acceptance failure: {error_reason}",
                now,
            )
            if refund_row is not None:
                await conn.execute(
                    "UPDATE tenants SET balance = $1, updated_at = $2 WHERE id = $3",
                    new_balance,
                    now,
                    msg["tenant_id"],
                )

            if msg["webhook_url"]:
                wh_payload = {
                    "message_id": str(message_id),
                    "status": "FAILED",
                    "to": msg["to_number"],
                    "from": msg["from_sender"],
                    "client_ref": msg["client_ref"],
                    "error": error_reason,
                    "finalized_at": now.isoformat(),
                }
                await conn.execute(
                    """
                    INSERT INTO webhook_deliveries (tenant_id, message_id, target_url, payload)
                    VALUES ($1, $2, $3, $4::jsonb)
                    ON CONFLICT (message_id) DO NOTHING
                    """,
                    msg["tenant_id"],
                    message_id,
                    msg["webhook_url"],
                    json.dumps(wh_payload),
                )

            finalized = True

    if finalized and tenant_id_str:
        await scheduler.publish_event(
            {
                "type": "message_updated",
                "message_id": str(message_id),
                "tenant_id": tenant_id_str,
                "status": "FAILED",
            }
        )

    return finalized
