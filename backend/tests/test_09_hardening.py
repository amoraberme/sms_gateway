from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import httpx
import pytest

from app.carriers.client import CarrierClient, CarrierSubmitResponse
from app.carriers.router import CarrierRouter
from app.workers.dispatch_worker import process_one_message
from app.workers.sweeper_worker import run_sweeper_once
from tests.conftest import ACME_KEY


@pytest.mark.asyncio
async def test_status_filter_validation_date_aliases_and_id_symmetry(db_and_app):
    client, _, _ = db_and_app

    r_send = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "Symmetry OTP", "client_ref": "sym-1"},
    )
    assert r_send.status_code == 202
    body = r_send.json()
    assert body["id"] == body["message_id"]

    # Invalid status filter must return 400 (never 500)
    r_bad = await client.get("/v1/messages?status=INVALID_STATUS", headers={"X-API-Key": ACME_KEY})
    assert r_bad.status_code == 400

    # Valid status + from_date/to_date aliases
    now = datetime.now(timezone.utc)
    r_ok = await client.get(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        params={
            "status": "QUEUED",
            "from_date": (now - timedelta(minutes=5)).isoformat(),
            "to_date": (now + timedelta(minutes=5)).isoformat(),
        },
    )
    assert r_ok.status_code == 200
    assert r_ok.json()["total"] == 1


@pytest.mark.asyncio
async def test_read_timeout_retries_same_carrier_then_fails_over_on_repeat(db_and_app):
    client, pool, scheduler = db_and_app
    router = CarrierRouter(pool, scheduler.redis)

    r_send = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "Timeout affinity"},
    )
    msg_id = r_send.json()["message_id"]
    await scheduler.pop_fair()

    called_urls: list[str] = []

    class TwoTimeoutsThenSecondary(CarrierClient):
        async def submit(self, base_url, message_id, to_number, from_sender, text, encoding):
            called_urls.append(base_url)
            if len(called_urls) <= 2:
                raise httpx.ReadTimeout("simulated 5s read timeout")
            return CarrierSubmitResponse(status_code=200, carrier_id="c-failover-ok")

    carrier = TwoTimeoutsThenSecondary()
    for _ in range(3):
        async with pool.acquire() as conn:
            await conn.execute("UPDATE messages SET next_attempt_at = now() - interval '1 second' WHERE id = $1", msg_id)
        await process_one_message(pool, scheduler, router, carrier, msg_id)

    # Attempt 1 -> 8001, Attempt 2 (ReadTimeout affinity) -> 8001, Attempt 3 (consecutive timeout) -> 8002
    assert called_urls == [
        "http://127.0.0.1:8001",
        "http://127.0.0.1:8001",
        "http://127.0.0.1:8002",
    ]


@pytest.mark.asyncio
async def test_worker_crash_mid_lease_recovered_by_sweeper(db_and_app):
    client, pool, scheduler = db_and_app

    r_send = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "Crash recovery"},
    )
    msg_id = r_send.json()["message_id"]

    # Worker pops with 100ms lease and crashes without acking
    popped = await scheduler.pop_fair(lease_ttl_ms=100)
    assert popped == msg_id
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE messages SET lease_expires_at = now() - interval '1 second' WHERE id = $1",
            msg_id,
        )
    await asyncio.sleep(0.15)
    sweep = await run_sweeper_once(pool, scheduler)
    assert sweep["recovered"] >= 1
    assert await scheduler.pop_fair() == msg_id
