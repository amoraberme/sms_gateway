from __future__ import annotations

import asyncio
from decimal import Decimal
import pytest

from tests.conftest import ACME_KEY


@pytest.mark.asyncio
async def test_concurrent_sends_never_overdraw_balance(db_and_app):
    client, pool, _ = db_and_app

    # Set Acme Bank balance to $0.1000 (enough for exactly 5 x $0.0200 SMS)
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tenants SET opening_balance = 0.1000, balance = 0.1000 WHERE slug = 'acme-bank'"
        )

    async def send_one(idx: int):
        return await client.post(
            "/v1/messages",
            headers={"X-API-Key": ACME_KEY},
            json={"to": "+8801711000001", "from": "ACME", "text": f"Burst {idx}"},
        )

    responses = await asyncio.gather(*(send_one(i) for i in range(30)))
    accepted = [r for r in responses if r.status_code == 202]
    rejected = [r for r in responses if r.status_code == 402]

    assert len(accepted) == 5
    assert len(rejected) == 25

    bal_resp = await client.get("/v1/balance", headers={"X-API-Key": ACME_KEY})
    assert bal_resp.status_code == 200
    bal_data = bal_resp.json()
    assert Decimal(bal_data["balance"]) == Decimal("0.0000")
    assert bal_data["invariant_ok"] is True


@pytest.mark.asyncio
async def test_concurrent_mixed_single_and_batch_never_overdraw(db_and_app):
    client, pool, _ = db_and_app

    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE tenants SET opening_balance = 0.0800, balance = 0.0800 WHERE slug = 'acme-bank'"
        )

    # Fire 2 batches of 3 ($0.06 each) + 5 singles ($0.02 each) concurrently against $0.08
    tasks = [
        client.post(
            "/v1/messages/batch",
            headers={"X-API-Key": ACME_KEY},
            json={"messages": [{"to": "+8801711000001", "from": "ACME", "text": "B"} for _ in range(3)]},
        ),
        client.post(
            "/v1/messages/batch",
            headers={"X-API-Key": ACME_KEY},
            json={"messages": [{"to": "+8801711000001", "from": "ACME", "text": "B"} for _ in range(3)]},
        ),
        *(
            client.post(
                "/v1/messages",
                headers={"X-API-Key": ACME_KEY},
                json={"to": "+8801711000001", "from": "ACME", "text": f"S{i}"},
            )
            for i in range(5)
        ),
    ]
    resps = await asyncio.gather(*tasks)
    assert all(r.status_code in (202, 402) for r in resps)

    bal_data = (await client.get("/v1/balance", headers={"X-API-Key": ACME_KEY})).json()
    assert Decimal(bal_data["balance"]) >= Decimal("0.0000")
    assert bal_data["invariant_ok"] is True
