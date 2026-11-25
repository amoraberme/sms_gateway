from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json

import asyncpg
from fastapi import HTTPException, status

from app.domain.state_machine import map_dlr_status
from app.observability.metrics import DLR_DELAY_SECONDS
from app.queue.redis_scheduler import RedisScheduler


def verify_dlr_signature(raw_body: bytes, provided_signature: str | None, secret: str) -> None:
    if not provided_signature:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing X-Signature header")
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    sig_clean = provided_signature.strip()
    if sig_clean.startswith("sha256="):
        sig_clean = sig_clean[7:]
    if not hmac.compare_digest(sig_clean.lower(), expected.lower()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid X-Signature")


async def process_carrier_dlr(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    carrier_id: str,
    raw_status: str,
    carrier_timestamp: datetime,
    raw_payload: dict,
) -> dict[str, str]:
    try:
        mapped_status = map_dlr_status(raw_status)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    if carrier_timestamp.tzinfo is None:
        carrier_timestamp = carrier_timestamp.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    updated_message_id: str | None = None
    updated_tenant_id: str | None = None
    submitted_at: datetime | None = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            # Coordinate with dispatch worker's carrier_id persistence via identical advisory lock
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"dlr:{carrier_id}")

            receipt_row = await conn.fetchrow(
                """
                INSERT INTO dlr_receipts (
                    carrier_id, raw_status, mapped_status, carrier_timestamp, raw_payload
                )
                VALUES ($1, $2, $3::message_status, $4, $5::jsonb)
                ON CONFLICT (carrier_id, raw_status) DO NOTHING
                RETURNING id
                """,
                carrier_id,
                raw_status,
                mapped_status,
                carrier_timestamp,
                json.dumps(raw_payload),
            )
            if receipt_row is None:
                return {"status": "duplicate_ignored"}

            receipt_id = receipt_row["id"]

            msg = await conn.fetchrow(
                """
                SELECT m.id, m.tenant_id, m.status::text AS status, m.status_rank,
                       m.submitted_at, m.to_number, m.from_sender, m.client_ref,
                       t.webhook_url
                FROM messages m
                JOIN tenants t ON t.id = m.tenant_id
                WHERE m.carrier_id = $1
                FOR UPDATE OF m
                """,
                carrier_id,
            )

            if msg is None:
                # Out-of-order DLR (~5%): arrived before carrier /submit response returned.
                # Buffered safely in dlr_receipts with processed_at = NULL.
                return {"status": "buffered_early_dlr"}

            updated_message_id = str(msg["id"])
            updated_tenant_id = str(msg["tenant_id"])
            submitted_at = msg["submitted_at"]

            if msg["status_rank"] < 40:
                await conn.execute(
                    """
                    UPDATE messages
                    SET status = $1::message_status,
                        status_rank = 40,
                        finalized_at = $2,
                        updated_at = $2
                    WHERE id = $3 AND status_rank < 40
                    """,
                    mapped_status,
                    now,
                    msg["id"],
                )
                await conn.execute(
                    """
                    INSERT INTO message_events (message_id, status, detail, metadata, occurred_at)
                    VALUES ($1, $2::message_status, $3, $4::jsonb, $5)
                    """,
                    msg["id"],
                    mapped_status,
                    f"Carrier DLR received: {raw_status}",
                    json.dumps({"carrier_id": carrier_id, "raw_status": raw_status}),
                    now,
                )
                if msg["webhook_url"]:
                    wh_payload = {
                        "message_id": str(msg["id"]),
                        "status": mapped_status,
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
                        msg["id"],
                        msg["webhook_url"],
                        json.dumps(wh_payload),
                    )

            await conn.execute(
                """
                UPDATE dlr_receipts
                SET message_id = $1, processed_at = $2
                WHERE id = $3
                """,
                msg["id"],
                now,
                receipt_id,
            )

    if submitted_at is not None:
        delay = max(0.0, (now - submitted_at).total_seconds())
        DLR_DELAY_SECONDS.observe(delay)

    if updated_message_id is not None:
        await scheduler.publish_event(
            {
                "type": "message_updated",
                "message_id": updated_message_id,
                "tenant_id": updated_tenant_id,
                "status": mapped_status,
            }
        )

    return {"status": "processed", "mapped_status": mapped_status}
