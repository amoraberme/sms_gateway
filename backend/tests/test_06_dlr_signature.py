from __future__ import annotations

from datetime import datetime, timezone
import json
import pytest

from tests.conftest import sign_dlr


@pytest.mark.asyncio
async def test_dlr_missing_or_wrong_signature_returns_401(db_and_app):
    client, pool, _ = db_and_app
    payload = {
        "carrier_id": "c-unauth-001",
        "status": "DELIVRD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Missing X-Signature
    r1 = await client.post("/carrier/dlr", json=payload)
    assert r1.status_code == 401

    # Wrong secret
    raw, wrong_sig = sign_dlr(payload, secret="wrong-secret")
    r2 = await client.post(
        "/carrier/dlr",
        content=raw,
        headers={"Content-Type": "application/json", "X-Signature": wrong_sig},
    )
    assert r2.status_code == 401

    # Tampered body with original signature
    _, valid_sig = sign_dlr(payload)
    tampered_raw = json.dumps({**payload, "status": "UNDELIV"}).encode("utf-8")
    r3 = await client.post(
        "/carrier/dlr",
        content=tampered_raw,
        headers={"Content-Type": "application/json", "X-Signature": valid_sig},
    )
    assert r3.status_code == 401

    async with pool.acquire() as conn:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM dlr_receipts")
        assert cnt == 0
