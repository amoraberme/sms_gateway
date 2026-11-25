from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from typing import Any
from uuid import UUID, uuid4

import asyncpg
from fastapi import HTTPException, status

from app.domain.encoding import analyze_encoding
from app.domain.pricing import UnsupportedDestinationError, resolve_price
from app.domain.validation import validate_e164, validate_message_text, validate_sender_id
from app.queue.redis_scheduler import RedisScheduler
from app.services.auth import AuthenticatedTenant


@dataclass
class PreparedMessage:
    message_id: UUID
    to_number: str
    from_sender: str
    body: str
    client_ref: str | None
    encoding: str
    char_units: int
    segments: int
    price_prefix: str
    unit_price: Decimal
    total_cost: Decimal
    scheduled_at: datetime | None


def _canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _normalize_scheduled_at(raw_dt: datetime | None) -> datetime | None:
    if raw_dt is None:
        return None
    if raw_dt.tzinfo is None:
        return raw_dt.replace(tzinfo=timezone.utc)
    return raw_dt.astimezone(timezone.utc)


async def _load_price_table(conn: asyncpg.Connection) -> dict[str, Decimal]:
    rows = await conn.fetch("SELECT prefix, price_per_segment FROM prices")
    return {r["prefix"]: Decimal(r["price_per_segment"]) for r in rows}


async def accept_messages(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    tenant: AuthenticatedTenant,
    items: list[dict[str, Any]],
    endpoint: str,
    idempotency_key: str | None,
    raw_request_payload: Any,
    max_attempts: int,
) -> tuple[int, dict[str, Any]]:
    """
    Atomically validate, price, check idempotency, lock tenant balance,
    debit prepaid funds, and insert ACCEPTED messages + ledger entries.
    """
    if not items:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="At least 1 message is required")
    if len(items) > 1000:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Batch size cannot exceed 1,000 messages")

    req_hash = _canonical_hash(raw_request_payload)

    async with pool.acquire() as conn:
        price_table = await _load_price_table(conn)

        prepared: list[PreparedMessage] = []
        total_batch_cost = Decimal("0.0000")

        for idx, item in enumerate(items):
            try:
                to_num = validate_e164(item["to"])
                from_sender = validate_sender_id(item["from"])
                text = validate_message_text(item["text"])
                enc = analyze_encoding(text)
                quote = resolve_price(to_num, enc.segments, price_table)
                sched = _normalize_scheduled_at(item.get("scheduled_at"))
            except UnsupportedDestinationError as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Item {idx}: {exc}",
                ) from exc

            client_ref = item.get("client_ref")
            if client_ref is not None:
                client_ref = str(client_ref)[:255]

            msg_id = uuid4()
            total_batch_cost += quote.total_cost
            prepared.append(
                PreparedMessage(
                    message_id=msg_id,
                    to_number=to_num,
                    from_sender=from_sender,
                    body=text,
                    client_ref=client_ref,
                    encoding=enc.encoding,
                    char_units=enc.units,
                    segments=enc.segments,
                    price_prefix=quote.prefix,
                    unit_price=quote.unit_price,
                    total_cost=quote.total_cost,
                    scheduled_at=sched,
                )
            )

        now = datetime.now(timezone.utc)

        async with conn.transaction():
            if idempotency_key:
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtext($1))",
                    f"{tenant.id}:idem:{idempotency_key}",
                )
                existing = await conn.fetchrow(
                    """
                    SELECT request_hash, response_status, response_body
                    FROM idempotency_keys
                    WHERE tenant_id = $1 AND idempotency_key = $2
                    """,
                    tenant.id,
                    idempotency_key,
                )
                if existing is not None:
                    if existing["request_hash"] != req_hash:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="Idempotency-Key reused with a different request payload",
                        )
                    return int(existing["response_status"]), json.loads(existing["response_body"])

            # Row-level lock on tenant guarantees balance never drops below zero under concurrent bursts
            tenant_row = await conn.fetchrow(
                "SELECT balance, default_priority::text AS default_priority FROM tenants WHERE id = $1 FOR UPDATE",
                tenant.id,
            )
            if tenant_row is None:
                raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Tenant not found")

            current_balance = Decimal(tenant_row["balance"])
            if current_balance < total_batch_cost:
                raise HTTPException(
                    status_code=status.HTTP_402_PAYMENT_REQUIRED,
                    detail={
                        "error": "INSUFFICIENT_BALANCE",
                        "required": str(total_batch_cost),
                        "available": str(current_balance),
                    },
                )

            new_balance = (current_balance - total_batch_cost).quantize(Decimal("0.0001"))
            await conn.execute(
                "UPDATE tenants SET balance = $1, updated_at = now() WHERE id = $2",
                new_balance,
                tenant.id,
            )

            # Prepare bulk UNNEST arrays
            msg_ids = [m.message_id for m in prepared]
            tenant_ids = [tenant.id] * len(prepared)
            client_refs = [m.client_ref for m in prepared]
            to_numbers = [m.to_number for m in prepared]
            from_senders = [m.from_sender for m in prepared]
            bodies = [m.body for m in prepared]
            encodings = [m.encoding for m in prepared]
            char_units = [m.char_units for m in prepared]
            segments = [m.segments for m in prepared]
            prefixes = [m.price_prefix for m in prepared]
            unit_prices = [m.unit_price for m in prepared]
            total_costs = [m.total_cost for m in prepared]
            priorities = [tenant_row["default_priority"]] * len(prepared)
            max_attempts_arr = [max_attempts] * len(prepared)
            scheduled_ats = [m.scheduled_at for m in prepared]

            await conn.execute(
                """
                INSERT INTO messages (
                    id, tenant_id, client_ref, to_number, from_sender, body,
                    encoding, char_units, segments, price_prefix, unit_price, total_cost,
                    priority, status, status_rank, max_attempts, scheduled_at,
                    next_attempt_at, accepted_at, updated_at
                )
                SELECT
                    u.id, u.tenant_id, u.client_ref, u.to_number, u.from_sender, u.body,
                    u.encoding::encoding_type, u.char_units, u.segments, u.price_prefix,
                    u.unit_price, u.total_cost, u.priority::priority_tier,
                    'ACCEPTED'::message_status, 10, u.max_attempts, u.scheduled_at,
                    COALESCE(u.scheduled_at, $16), $16, $16
                FROM UNNEST(
                    $1::uuid[], $2::uuid[], $3::varchar[], $4::varchar[], $5::varchar[], $6::text[],
                    $7::text[], $8::int[], $9::int[], $10::varchar[], $11::numeric[], $12::numeric[],
                    $13::text[], $14::int[], $15::timestamptz[]
                ) AS u(
                    id, tenant_id, client_ref, to_number, from_sender, body,
                    encoding, char_units, segments, price_prefix, unit_price, total_cost,
                    priority, max_attempts, scheduled_at
                )
                """,
                msg_ids, tenant_ids, client_refs, to_numbers, from_senders, bodies,
                encodings, char_units, segments, prefixes, unit_prices, total_costs,
                priorities, max_attempts_arr, scheduled_ats, now,
            )

            event_details = ["Message accepted" for _ in prepared]
            await conn.execute(
                """
                INSERT INTO message_events (message_id, status, detail, occurred_at)
                SELECT u.message_id, 'ACCEPTED'::message_status, u.detail, $3
                FROM UNNEST($1::uuid[], $2::text[]) AS u(message_id, detail)
                """,
                msg_ids,
                event_details,
                now,
            )

            running_balances: list[Decimal] = []
            running = current_balance
            descriptions: list[str] = []
            for m in prepared:
                running = (running - m.total_cost).quantize(Decimal("0.0001"))
                running_balances.append(running)
                descriptions.append(f"Prepaid debit for {m.segments} {m.encoding} segment(s) to {m.price_prefix}")

            await conn.execute(
                """
                INSERT INTO ledger_entries (
                    tenant_id, message_id, entry_type, amount, balance_after, description, created_at
                )
                SELECT
                    u.tenant_id, u.message_id, 'DEBIT'::ledger_entry_type,
                    u.amount, u.balance_after, u.description, $6
                FROM UNNEST(
                    $1::uuid[], $2::uuid[], $3::numeric[], $4::numeric[], $5::text[]
                ) AS u(tenant_id, message_id, amount, balance_after, description)
                """,
                tenant_ids,
                msg_ids,
                total_costs,
                running_balances,
                descriptions,
                now,
            )

            if endpoint == "single":
                m0 = prepared[0]
                response_body: dict[str, Any] = {
                    "message_id": str(m0.message_id),
                    "status": "ACCEPTED",
                    "to": m0.to_number,
                    "from": m0.from_sender,
                    "encoding": m0.encoding,
                    "units": m0.char_units,
                    "segments": m0.segments,
                    "price_prefix": m0.price_prefix,
                    "cost": str(m0.total_cost),
                    "balance_remaining": str(new_balance),
                    "scheduled_at": m0.scheduled_at.isoformat() if m0.scheduled_at else None,
                }
            else:
                response_body = {
                    "accepted_count": len(prepared),
                    "total_cost": str(total_batch_cost),
                    "balance_remaining": str(new_balance),
                    "messages": [
                        {
                            "message_id": str(m.message_id),
                            "status": "ACCEPTED",
                            "to": m.to_number,
                            "encoding": m.encoding,
                            "segments": m.segments,
                            "cost": str(m.total_cost),
                            "client_ref": m.client_ref,
                        }
                        for m in prepared
                    ],
                }

            status_code = 202
            if idempotency_key:
                await conn.execute(
                    """
                    INSERT INTO idempotency_keys (
                        tenant_id, idempotency_key, endpoint, request_hash, response_status, response_body
                    ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                    """,
                    tenant.id,
                    idempotency_key,
                    endpoint,
                    req_hash,
                    status_code,
                    json.dumps(response_body),
                )

    # Fast-path promotion for immediate messages (scheduled_at is None or <= now)
    ready_ids = [m.message_id for m in prepared if m.scheduled_at is None or m.scheduled_at <= now]
    if ready_ids:
        await promote_and_enqueue_messages(pool, scheduler, ready_ids, str(tenant.id), tenant.default_priority)

    # Publish SSE event for dashboard updates
    await scheduler.publish_event(
        {
            "type": "messages_accepted",
            "tenant_id": str(tenant.id),
            "count": len(prepared),
        }
    )

    return status_code, response_body


async def promote_and_enqueue_messages(
    pool: asyncpg.Pool,
    scheduler: RedisScheduler,
    message_ids: list[UUID],
    tenant_id: str,
    priority: str,
) -> None:
    """Transition immediate ACCEPTED messages -> QUEUED and push to Redis fair queues."""
    now = datetime.now(timezone.utc)
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                """
                UPDATE messages
                SET status = 'QUEUED'::message_status,
                    status_rank = 20,
                    queued_at = COALESCE(queued_at, $2),
                    updated_at = $2
                WHERE id = ANY($1::uuid[]) AND status_rank < 20
                RETURNING id
                """,
                message_ids,
                now,
            )
            promoted_ids = [r["id"] for r in rows]
            if promoted_ids:
                await conn.execute(
                    """
                    INSERT INTO message_events (message_id, status, detail, occurred_at)
                    SELECT u.id, 'QUEUED'::message_status, 'Enqueued for carrier dispatch', $2
                    FROM UNNEST($1::uuid[]) AS u(id)
                    """,
                    promoted_ids,
                    now,
                )

    if promoted_ids:
        await scheduler.enqueue_many(
            [(str(mid), tenant_id, priority) for mid in promoted_ids]
        )
