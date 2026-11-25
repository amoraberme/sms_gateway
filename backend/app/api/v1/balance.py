from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, Request

from app.services.auth import AuthenticatedTenant, get_current_tenant

router = APIRouter(prefix="/v1/balance", tags=["balance"])


@router.get("")
async def get_balance(
    request: Request,
    tenant: AuthenticatedTenant = Depends(get_current_tenant),
) -> dict[str, Any]:
    pool = request.app.state.db_pool
    row = await pool.fetchrow(
        """
        SELECT t.opening_balance, t.balance,
               COALESCE(SUM(l.amount), 0) AS ledger_net
        FROM tenants t
        LEFT JOIN ledger_entries l ON l.tenant_id = t.id
        WHERE t.id = $1
        GROUP BY t.id
        """,
        tenant.id,
    )
    opening = Decimal(row["opening_balance"]).quantize(Decimal("0.0001"))
    balance = Decimal(row["balance"]).quantize(Decimal("0.0001"))
    ledger_net = Decimal(row["ledger_net"]).quantize(Decimal("0.0001"))

    recent_ledger = await pool.fetch(
        """
        SELECT id, message_id, entry_type::text AS entry_type, amount, balance_after, description, created_at
        FROM ledger_entries
        WHERE tenant_id = $1
        ORDER BY id DESC
        LIMIT 25
        """,
        tenant.id,
    )

    return {
        "tenant_id": str(tenant.id),
        "tenant_name": tenant.name,
        "opening_balance": str(opening),
        "balance": str(balance),
        "ledger_net": str(ledger_net),
        "invariant_ok": (opening - ledger_net) == balance,
        "recent_ledger": [
            {
                "id": e["id"],
                "message_id": str(e["message_id"]),
                "entry_type": e["entry_type"],
                "amount": str(e["amount"]),
                "balance_after": str(e["balance_after"]),
                "description": e["description"],
                "created_at": e["created_at"].isoformat(),
            }
            for e in recent_ledger
        ],
    }
