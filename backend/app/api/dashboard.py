from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
import json
import math
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from app.config import get_settings
from app.services.auth import AuthenticatedTenant
from app.services.intake import accept_messages

router = APIRouter(prefix="/internal/dashboard", tags=["dashboard"])


class DashboardSendRequest(BaseModel):
    tenant_slug: str
    to: str
    from_: str = Field(alias="from")
    text: str
    client_ref: str | None = None
    idempotency_key: str | None = None
    scheduled_at: datetime | None = None

    model_config = {"populate_by_name": True}


@router.get("/overview")
async def get_overview(request: Request) -> dict[str, Any]:
    pool = request.app.state.db_pool
    tenants = await pool.fetch(
        """
        SELECT t.id, t.name, t.slug, t.default_priority::text AS default_priority,
               t.opening_balance, t.balance,
               COALESCE((SELECT SUM(l.amount) FROM ledger_entries l WHERE l.tenant_id = t.id), 0) AS ledger_net
        FROM tenants t
        ORDER BY t.name ASC
        """
    )

    counts_rows = await pool.fetch(
        """
        SELECT tenant_id, status::text AS status, COUNT(*) AS cnt
        FROM messages
        GROUP BY tenant_id, status
        """
    )
    status_map: dict[str, dict[str, int]] = {}
    for r in counts_rows:
        tid = str(r["tenant_id"])
        status_map.setdefault(tid, {})[r["status"]] = int(r["cnt"])

    prices = await pool.fetch("SELECT prefix, country_name, price_per_segment FROM prices ORDER BY prefix")

    return {
        "tenants": [
            {
                "id": str(t["id"]),
                "name": t["name"],
                "slug": t["slug"],
                "default_priority": t["default_priority"],
                "opening_balance": str(Decimal(t["opening_balance"]).quantize(Decimal("0.0001"))),
                "balance": str(Decimal(t["balance"]).quantize(Decimal("0.0001"))),
                "ledger_net": str(Decimal(t["ledger_net"]).quantize(Decimal("0.0001"))),
                "invariant_ok": (
                    Decimal(t["opening_balance"]).quantize(Decimal("0.0001"))
                    - Decimal(t["ledger_net"]).quantize(Decimal("0.0001"))
                )
                == Decimal(t["balance"]).quantize(Decimal("0.0001")),
                "status_counts": status_map.get(str(t["id"]), {}),
            }
            for t in tenants
        ],
        "prices": [
            {
                "prefix": p["prefix"],
                "country_name": p["country_name"],
                "price_per_segment": str(Decimal(p["price_per_segment"]).quantize(Decimal("0.0001"))),
            }
            for p in prices
        ],
    }


@router.get("/messages")
async def list_all_messages(
    request: Request,
    tenant_id: UUID | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    client_ref: str | None = Query(default=None),
    start_date: datetime | None = Query(default=None),
    end_date: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    pool = request.app.state.db_pool
    conditions = ["1=1"]
    params: list[Any] = []
    idx = 1

    if tenant_id:
        conditions.append(f"m.tenant_id = ${idx}")
        params.append(tenant_id)
        idx += 1
    if status_filter:
        conditions.append(f"m.status = ${idx}::message_status")
        params.append(status_filter.upper())
        idx += 1
    if client_ref:
        conditions.append(f"m.client_ref ILIKE ${idx}")
        params.append(f"%{client_ref}%")
        idx += 1
    if start_date:
        conditions.append(f"m.accepted_at >= ${idx}")
        params.append(start_date)
        idx += 1
    if end_date:
        conditions.append(f"m.accepted_at <= ${idx}")
        params.append(end_date)
        idx += 1

    where_sql = " AND ".join(conditions)
    rows = await pool.fetch(
        f"""
        SELECT m.id, m.tenant_id, t.name AS tenant_name, t.slug AS tenant_slug,
               m.client_ref, m.to_number, m.from_sender, m.body,
               m.encoding::text AS encoding, m.segments, m.price_prefix,
               m.total_cost, m.status::text AS status, m.carrier_used,
               m.carrier_id, m.attempt_count, m.scheduled_at,
               m.accepted_at, m.submitted_at, m.finalized_at
        FROM messages m
        JOIN tenants t ON t.id = m.tenant_id
        WHERE {where_sql}
        ORDER BY m.accepted_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
        """,
        *params,
        limit,
        offset,
    )
    total = await pool.fetchval(f"SELECT COUNT(*) FROM messages m WHERE {where_sql}", *params)

    return {
        "total": int(total),
        "messages": [
            {
                "id": str(r["id"]),
                "tenant_id": str(r["tenant_id"]),
                "tenant_name": r["tenant_name"],
                "tenant_slug": r["tenant_slug"],
                "client_ref": r["client_ref"],
                "to": r["to_number"],
                "from": r["from_sender"],
                "text": r["body"],
                "encoding": r["encoding"],
                "segments": r["segments"],
                "price_prefix": r["price_prefix"],
                "cost": str(r["total_cost"]),
                "status": r["status"],
                "carrier_used": r["carrier_used"],
                "carrier_id": r["carrier_id"],
                "attempt_count": r["attempt_count"],
                "scheduled_at": r["scheduled_at"].isoformat() if r["scheduled_at"] else None,
                "accepted_at": r["accepted_at"].isoformat(),
                "submitted_at": r["submitted_at"].isoformat() if r["submitted_at"] else None,
                "finalized_at": r["finalized_at"].isoformat() if r["finalized_at"] else None,
            }
            for r in rows
        ],
    }


@router.get("/messages/{message_id}")
async def get_dashboard_message_detail(message_id: UUID, request: Request) -> dict[str, Any]:
    pool = request.app.state.db_pool
    row = await pool.fetchrow(
        """
        SELECT m.*, m.encoding::text AS enc_text, m.priority::text AS prio_text,
               m.status::text AS status_text, t.name AS tenant_name, t.slug AS tenant_slug
        FROM messages m
        JOIN tenants t ON t.id = m.tenant_id
        WHERE m.id = $1
        """,
        message_id,
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Message not found")

    events = await pool.fetch(
        """
        SELECT id, status::text AS status, detail, metadata, occurred_at
        FROM message_events
        WHERE message_id = $1
        ORDER BY id ASC
        """,
        message_id,
    )
    ledger = await pool.fetch(
        """
        SELECT id, entry_type::text AS entry_type, amount, balance_after, description, created_at
        FROM ledger_entries
        WHERE message_id = $1
        ORDER BY id ASC
        """,
        message_id,
    )

    return {
        "id": str(row["id"]),
        "tenant_id": str(row["tenant_id"]),
        "tenant_name": row["tenant_name"],
        "tenant_slug": row["tenant_slug"],
        "client_ref": row["client_ref"],
        "to": row["to_number"],
        "from": row["from_sender"],
        "text": row["body"],
        "encoding": row["enc_text"],
        "units": row["char_units"],
        "segments": row["segments"],
        "price_prefix": row["price_prefix"],
        "unit_price": str(row["unit_price"]),
        "cost": str(row["total_cost"]),
        "priority": row["prio_text"],
        "status": row["status_text"],
        "carrier_used": row["carrier_used"],
        "carrier_id": row["carrier_id"],
        "attempt_count": row["attempt_count"],
        "max_attempts": row["max_attempts"],
        "last_error": row["last_error"],
        "scheduled_at": row["scheduled_at"].isoformat() if row["scheduled_at"] else None,
        "accepted_at": row["accepted_at"].isoformat(),
        "queued_at": row["queued_at"].isoformat() if row["queued_at"] else None,
        "submitted_at": row["submitted_at"].isoformat() if row["submitted_at"] else None,
        "finalized_at": row["finalized_at"].isoformat() if row["finalized_at"] else None,
        "events": [
            {
                "id": e["id"],
                "status": e["status"],
                "detail": e["detail"],
                "metadata": e["metadata"],
                "occurred_at": e["occurred_at"].isoformat(),
            }
            for e in events
        ],
        "ledger_entries": [
            {
                "id": l["id"],
                "entry_type": l["entry_type"],
                "amount": str(l["amount"]),
                "balance_after": str(l["balance_after"]),
                "description": l["description"],
                "created_at": l["created_at"].isoformat(),
            }
            for l in ledger
        ],
    }


@router.post("/send")
async def dashboard_send_test(request: Request, payload: DashboardSendRequest) -> JSONResponse:
    pool = request.app.state.db_pool
    settings = get_settings()
    row = await pool.fetchrow(
        "SELECT id, name, slug, default_priority::text AS default_priority FROM tenants WHERE slug = $1 AND is_active = TRUE",
        payload.tenant_slug,
    )
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Tenant not found")

    tenant = AuthenticatedTenant(
        id=row["id"],
        name=row["name"],
        slug=row["slug"],
        default_priority=row["default_priority"],
    )
    item = {
        "to": payload.to,
        "from": payload.from_,
        "text": payload.text,
        "client_ref": payload.client_ref,
        "scheduled_at": payload.scheduled_at,
    }
    code, body = await accept_messages(
        pool=pool,
        scheduler=request.app.state.scheduler,
        tenant=tenant,
        items=[item],
        endpoint="single",
        idempotency_key=payload.idempotency_key or None,
        raw_request_payload=item,
        max_attempts=settings.max_submit_attempts,
    )
    return JSONResponse(status_code=code, content=body)


@router.get("/events")
async def sse_events(request: Request) -> StreamingResponse:
    scheduler = request.app.state.scheduler

    async def event_stream():
        pubsub = scheduler.redis.pubsub()
        await pubsub.subscribe(scheduler.EVENTS_CHANNEL)
        try:
            yield f"data: {json.dumps({'type': 'connected'})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if msg and msg.get("data"):
                    yield f"data: {msg['data']}\n\n"
                else:
                    yield ": keepalive\n\n"
                    await asyncio.sleep(0.5)
        finally:
            await pubsub.unsubscribe(scheduler.EVENTS_CHANNEL)
            await pubsub.aclose()

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


@router.get("/audit")
async def audit_run(
    request: Request,
    run_prefix: str | None = Query(default=None),
) -> dict[str, Any]:
    """Summary & strict billing invariant audit endpoint for load_test.py."""
    pool = request.app.state.db_pool
    where_clause = "WHERE client_ref LIKE $1" if run_prefix else ""
    args: list[Any] = [f"{run_prefix}%"] if run_prefix else []

    status_rows = await pool.fetch(
        f"SELECT status::text AS status, COUNT(*) AS cnt FROM messages {where_clause} GROUP BY status",
        *args,
    )
    status_counts = {r["status"]: int(r["cnt"]) for r in status_rows}
    total_messages = sum(status_counts.values())
    non_terminal = sum(
        status_counts.get(s, 0) for s in ("ACCEPTED", "QUEUED", "SUBMITTED")
    )

    otp_lat_rows = await pool.fetch(
        f"""
        SELECT EXTRACT(EPOCH FROM (m.submitted_at - m.accepted_at)) AS submit_latency_sec
        FROM messages m
        JOIN tenants t ON t.id = m.tenant_id
        {where_clause + ' AND ' if where_clause else 'WHERE '}
          t.slug = 'acme-bank' AND m.submitted_at IS NOT NULL
        """,
        *args,
    )
    otp_latencies = sorted(float(r["submit_latency_sec"]) for r in otp_lat_rows)

    total_attempts = await pool.fetchval(
        f"SELECT COALESCE(SUM(attempt_count), 0) FROM messages {where_clause}",
        *args,
    )

    tenants = await pool.fetch(
        """
        SELECT t.id, t.name, t.slug, t.opening_balance, t.balance,
               COALESCE((SELECT SUM(l.amount) FROM ledger_entries l WHERE l.tenant_id = t.id), 0) AS ledger_net
        FROM tenants t
        ORDER BY t.slug
        """
    )

    dup_debits = await pool.fetchval(
        """
        SELECT COUNT(*) FROM (
            SELECT message_id FROM ledger_entries WHERE entry_type = 'DEBIT'
            GROUP BY message_id HAVING COUNT(*) > 1
        ) x
        """
    )
    unrefunded_failed = await pool.fetchval(
        """
        SELECT COUNT(*) FROM messages m
        WHERE m.status = 'FAILED'
          AND NOT EXISTS (
              SELECT 1 FROM ledger_entries l WHERE l.message_id = m.id AND l.entry_type = 'REFUND'
          )
        """
    )
    refunded_post_submit = await pool.fetchval(
        """
        SELECT COUNT(*) FROM messages m
        WHERE m.status IN ('DELIVERED', 'UNDELIVERED', 'EXPIRED', 'SUBMITTED')
          AND EXISTS (
              SELECT 1 FROM ledger_entries l WHERE l.message_id = m.id AND l.entry_type = 'REFUND'
          )
        """
    )

    tenant_audits = []
    for t in tenants:
        opening = Decimal(t["opening_balance"]).quantize(Decimal("0.0001"))
        balance = Decimal(t["balance"]).quantize(Decimal("0.0001"))
        ledger_net = Decimal(t["ledger_net"]).quantize(Decimal("0.0001"))
        tenant_audits.append(
            {
                "slug": t["slug"],
                "name": t["name"],
                "opening_balance": str(opening),
                "balance": str(balance),
                "ledger_net": str(ledger_net),
                "invariant_ok": (opening - ledger_net) == balance,
            }
        )

    return {
        "total_messages": total_messages,
        "non_terminal_count": non_terminal,
        "status_counts": status_counts,
        "total_carrier_attempts": int(total_attempts),
        "otp_count": len(otp_latencies),
        "otp_p50_sec": round(_percentile(otp_latencies, 50), 4),
        "otp_p95_sec": round(_percentile(otp_latencies, 95), 4),
        "duplicate_debits_count": int(dup_debits),
        "unrefunded_failed_count": int(unrefunded_failed),
        "refunded_post_submit_count": int(refunded_post_submit),
        "tenants": tenant_audits,
    }
