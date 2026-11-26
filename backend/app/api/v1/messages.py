from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import get_settings
from app.domain.state_machine import STATUS_RANKS
from app.services.auth import AuthenticatedTenant, get_current_tenant
from app.services.intake import accept_messages

router = APIRouter(prefix="/v1/messages", tags=["messages"])


class SendMessageRequest(BaseModel):
    to: str
    from_: str = Field(alias="from")
    text: str
    client_ref: str | None = None
    scheduled_at: datetime | None = None

    model_config = {"populate_by_name": True}


class SendBatchRequest(BaseModel):
    messages: list[SendMessageRequest]


@router.post("", status_code=status.HTTP_202_ACCEPTED)
async def send_message(
    request: Request,
    payload: SendMessageRequest,
    tenant: AuthenticatedTenant = Depends(get_current_tenant),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JSONResponse:
    settings = get_settings()
    item = {
        "to": payload.to,
        "from": payload.from_,
        "text": payload.text,
        "client_ref": payload.client_ref,
        "scheduled_at": payload.scheduled_at,
    }
    code, body = await accept_messages(
        pool=request.app.state.db_pool,
        scheduler=request.app.state.scheduler,
        tenant=tenant,
        items=[item],
        endpoint="single",
        idempotency_key=idempotency_key,
        raw_request_payload=item,
        max_attempts=settings.max_submit_attempts,
    )
    return JSONResponse(status_code=code, content=body)


@router.post("/batch", status_code=status.HTTP_202_ACCEPTED)
async def send_batch(
    request: Request,
    payload: SendBatchRequest,
    tenant: AuthenticatedTenant = Depends(get_current_tenant),
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JSONResponse:
    settings = get_settings()
    items = [
        {
            "to": m.to,
            "from": m.from_,
            "text": m.text,
            "client_ref": m.client_ref,
            "scheduled_at": m.scheduled_at,
        }
        for m in payload.messages
    ]
    code, body = await accept_messages(
        pool=request.app.state.db_pool,
        scheduler=request.app.state.scheduler,
        tenant=tenant,
        items=items,
        endpoint="batch",
        idempotency_key=idempotency_key,
        raw_request_payload={"messages": items},
        max_attempts=settings.max_submit_attempts,
    )
    return JSONResponse(status_code=code, content=body)


@router.get("")
async def list_messages(
    request: Request,
    tenant: AuthenticatedTenant = Depends(get_current_tenant),
    status_filter: str | None = Query(default=None, alias="status"),
    client_ref: str | None = Query(default=None),
    start_date: datetime | None = Query(default=None),
    end_date: datetime | None = Query(default=None),
    from_date: datetime | None = Query(default=None),
    to_date: datetime | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    pool = request.app.state.db_pool
    conditions = ["tenant_id = $1"]
    params: list[Any] = [tenant.id]
    idx = 2

    if status_filter:
        normalized_status = status_filter.strip().upper()
        if normalized_status not in STATUS_RANKS:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid status filter '{status_filter}'. Allowed: {sorted(STATUS_RANKS.keys())}",
            )
        conditions.append(f"status = ${idx}::message_status")
        params.append(normalized_status)
        idx += 1
    if client_ref:
        conditions.append(f"client_ref = ${idx}")
        params.append(client_ref)
        idx += 1
    eff_start = start_date or from_date
    eff_end = end_date or to_date
    if eff_start:
        conditions.append(f"accepted_at >= ${idx}")
        params.append(eff_start)
        idx += 1
    if eff_end:
        conditions.append(f"accepted_at <= ${idx}")
        params.append(eff_end)
        idx += 1

    where_sql = " AND ".join(conditions)
    rows = await pool.fetch(
        f"""
        SELECT id, client_ref, to_number, from_sender, encoding::text AS encoding,
               segments, price_prefix, total_cost, status::text AS status,
               carrier_used, carrier_id, attempt_count, scheduled_at,
               accepted_at, queued_at, submitted_at, finalized_at
        FROM messages
        WHERE {where_sql}
        ORDER BY accepted_at DESC
        LIMIT ${idx} OFFSET ${idx + 1}
        """,
        *params,
        limit,
        offset,
    )
    total = await pool.fetchval(f"SELECT COUNT(*) FROM messages WHERE {where_sql}", *params)

    return {
        "total": int(total),
        "limit": limit,
        "offset": offset,
        "messages": [
            {
                "id": str(r["id"]),
                "client_ref": r["client_ref"],
                "to": r["to_number"],
                "from": r["from_sender"],
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
                "queued_at": r["queued_at"].isoformat() if r["queued_at"] else None,
                "submitted_at": r["submitted_at"].isoformat() if r["submitted_at"] else None,
                "finalized_at": r["finalized_at"].isoformat() if r["finalized_at"] else None,
            }
            for r in rows
        ],
    }


@router.get("/{message_id}")
async def get_message_detail(
    message_id: UUID,
    request: Request,
    tenant: AuthenticatedTenant = Depends(get_current_tenant),
) -> dict[str, Any]:
    pool = request.app.state.db_pool
    row = await pool.fetchrow(
        """
        SELECT id, tenant_id, client_ref, to_number, from_sender, body,
               encoding::text AS encoding, char_units, segments, price_prefix,
               unit_price, total_cost, priority::text AS priority,
               status::text AS status, carrier_used, carrier_id,
               attempt_count, max_attempts, last_error, scheduled_at,
               accepted_at, queued_at, submitted_at, finalized_at
        FROM messages
        WHERE id = $1 AND tenant_id = $2
        """,
        message_id,
        tenant.id,
    )
    if row is None:
        # Strict tenant isolation: cross-tenant access returns 404
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

    return {
        "id": str(row["id"]),
        "tenant_id": str(row["tenant_id"]),
        "client_ref": row["client_ref"],
        "to": row["to_number"],
        "from": row["from_sender"],
        "text": row["body"],
        "encoding": row["encoding"],
        "units": row["char_units"],
        "segments": row["segments"],
        "price_prefix": row["price_prefix"],
        "unit_price": str(row["unit_price"]),
        "cost": str(row["total_cost"]),
        "priority": row["priority"],
        "status": row["status"],
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
    }
