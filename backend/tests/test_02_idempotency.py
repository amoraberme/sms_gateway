from __future__ import annotations

import asyncio
from decimal import Decimal
import pytest

from tests.conftest import ACME_KEY


@pytest.mark.asyncio
async def test_idempotency_same_and_different_body(db_and_app):
    client, pool, _ = db_and_app

    payload = {"to": "+8801711000001", "from": "ACME", "text": "Your OTP is 4321"}
    headers = {"X-API-Key": ACME_KEY, "Idempotency-Key": "idem-key-001"}

    # Fire two concurrent identical requests
    r1, r2 = await asyncio.gather(
        client.post("/v1/messages", headers=headers, json=payload),
        client.post("/v1/messages", headers=headers, json=payload),
    )
    assert r1.status_code == 202
    assert r2.status_code == 202
    assert r1.json()["message_id"] == r2.json()["message_id"]

    # Same key with different body -> 409 Conflict
    r3 = await client.post(
        "/v1/messages",
        headers=headers,
        json={"to": "+8801711000001", "from": "ACME", "text": "Different OTP 9999"},
    )
    assert r3.status_code == 409

    # Verify only 1 message and 1 ledger debit exist
    async with pool.acquire() as conn:
        msg_cnt = await conn.fetchval("SELECT COUNT(*) FROM messages")
        ledger_cnt = await conn.fetchval("SELECT COUNT(*) FROM ledger_entries")
        bal = await conn.fetchval("SELECT balance FROM tenants WHERE slug = 'acme-bank'")
        assert msg_cnt == 1
        assert ledger_cnt == 1
        assert Decimal(bal) == Decimal("499.9800")
