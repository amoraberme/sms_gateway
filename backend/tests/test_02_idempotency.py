from __future__ import annotations

import asyncio
from decimal import Decimal
import pytest

from tests.conftest import ACME_KEY, SHOPFAST_KEY


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

    # Different tenant using the same Idempotency-Key string -> independent!
    r_other = await client.post(
        "/v1/messages",
        headers={"X-API-Key": SHOPFAST_KEY, "Idempotency-Key": "idem-key-001"},
        json={"to": "+8801711000002", "from": "SHOP", "text": "Shop promo"},
    )
    assert r_other.status_code == 202
    assert r_other.json()["message_id"] != r1.json()["message_id"]

    async with pool.acquire() as conn:
        msg_cnt = await conn.fetchval("SELECT COUNT(*) FROM messages")
        ledger_cnt = await conn.fetchval("SELECT COUNT(*) FROM ledger_entries")
        bal = await conn.fetchval("SELECT balance FROM tenants WHERE slug = 'acme-bank'")
        assert msg_cnt == 2
        assert ledger_cnt == 2
        assert Decimal(bal) == Decimal("499.9800")


@pytest.mark.asyncio
async def test_batch_idempotency_and_atomic_rollback(db_and_app):
    client, pool, _ = db_and_app

    batch_payload = {
        "messages": [
            {"to": "+8801711000001", "from": "SHOP", "text": "Promo A", "client_ref": "b1"},
            {"to": "+971501000001", "from": "SHOP", "text": "Promo B", "client_ref": "b2"},
        ]
    }
    headers = {"X-API-Key": SHOPFAST_KEY, "Idempotency-Key": "batch-idem-01"}

    r1 = await client.post("/v1/messages/batch", headers=headers, json=batch_payload)
    r2 = await client.post("/v1/messages/batch", headers=headers, json=batch_payload)
    assert r1.status_code == 202 and r2.status_code == 202
    assert r1.json() == r2.json()

    # Altered item in batch with same key -> 409
    r_conflict = await client.post(
        "/v1/messages/batch",
        headers=headers,
        json={"messages": [{"to": "+8801711000001", "from": "SHOP", "text": "Altered"}]},
    )
    assert r_conflict.status_code == 409

    # All-or-nothing rollback when second item has invalid E.164
    r_bad = await client.post(
        "/v1/messages/batch",
        headers={"X-API-Key": SHOPFAST_KEY},
        json={
            "messages": [
                {"to": "+8801711000003", "from": "SHOP", "text": "Valid item"},
                {"to": "invalid-phone", "from": "SHOP", "text": "Bad item"},
            ]
        },
    )
    assert r_bad.status_code == 400

    async with pool.acquire() as conn:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM messages")
        assert cnt == 2  # Only the first 2-item batch persisted
