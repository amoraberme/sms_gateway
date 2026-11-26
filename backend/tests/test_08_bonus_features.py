from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import httpx
import pytest

from app.carriers.client import CarrierClient, CarrierSubmitResponse
from app.carriers.router import CarrierRouter
from app.workers.dispatch_worker import process_one_message
from app.workers.sweeper_worker import run_sweeper_once
from app.workers.webhook_worker import process_pending_webhooks_once
from tests.conftest import ACME_KEY, sign_dlr


class FailoverCarrierClient(CarrierClient):
    async def submit(self, base_url, message_id, to_number, from_sender, text, encoding):
        if "8001" in base_url:
            return CarrierSubmitResponse(status_code=503, error="PRIMARY_DOWN")
        return CarrierSubmitResponse(status_code=200, carrier_id="c-sec-ok")


@pytest.mark.asyncio
async def test_bonus_failover_routing_scheduling_webhooks_and_metrics(db_and_app):
    client, pool, scheduler = db_and_app
    router = CarrierRouter(pool, scheduler.redis)

    # 0. Verify +971 UAE prefers carrier-secondary first
    uae_candidates = await router.select_candidates("+971501234567")
    assert uae_candidates[0].id == "carrier-secondary"

    # 1. Scheduled message (+1.5s in future): stays ACCEPTED until due
    future_at = (datetime.now(timezone.utc) + timedelta(seconds=1.5)).isoformat()
    r_sched = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={
            "to": "+8801711000001",
            "from": "ACME",
            "text": "Scheduled OTP",
            "scheduled_at": future_at,
        },
    )
    assert r_sched.status_code == 202
    sched_id = r_sched.json()["message_id"]

    await run_sweeper_once(pool, scheduler)
    det_early = (await client.get(f"/v1/messages/{sched_id}", headers={"X-API-Key": ACME_KEY})).json()
    assert det_early["status"] == "ACCEPTED"

    await asyncio.sleep(1.7)
    res_sweep = await run_sweeper_once(pool, scheduler)
    assert res_sweep["promoted"] >= 1
    det_due = (await client.get(f"/v1/messages/{sched_id}", headers={"X-API-Key": ACME_KEY})).json()
    assert det_due["status"] == "QUEUED"

    # 2. Primary carrier down -> failover to secondary carrier
    failover_client = FailoverCarrierClient()
    await process_one_message(pool, scheduler, router, failover_client, sched_id)
    async with pool.acquire() as conn:
        await conn.execute("UPDATE messages SET next_attempt_at = now() - interval '1 second' WHERE id = $1", sched_id)
    await process_one_message(pool, scheduler, router, failover_client, sched_id)

    det_sub = (await client.get(f"/v1/messages/{sched_id}", headers={"X-API-Key": ACME_KEY})).json()
    assert det_sub["status"] == "SUBMITTED"
    assert det_sub["carrier_used"] == "carrier-secondary"

    # 3. Tenant webhook: 500 on attempt 1 -> backoff retry -> 200 DELIVERED on attempt 2, HMAC verified
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tenants SET webhook_url = 'http://tenant.local/hook' WHERE slug = 'acme-bank'")

    raw_dlr, sig_dlr = sign_dlr(
        {"carrier_id": "c-sec-ok", "status": "DELIVRD", "timestamp": datetime.now(timezone.utc).isoformat()}
    )
    await client.post("/carrier/dlr", content=raw_dlr, headers={"X-Signature": sig_dlr})

    captured: list[tuple[bytes, str]] = []
    call_count = 0

    class FlakyThenOkTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            nonlocal call_count
            call_count += 1
            captured.append((request.content, request.headers.get("X-Signature", "")))
            if call_count == 1:
                return httpx.Response(500, json={"error": "temporary"})
            return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(transport=FlakyThenOkTransport()) as wh_client:
        assert await process_pending_webhooks_once(pool, wh_client) == 1
        async with pool.acquire() as conn:
            await conn.execute("UPDATE webhook_deliveries SET next_attempt_at = now() - interval '1 second'")
        assert await process_pending_webhooks_once(pool, wh_client) == 1

    async with pool.acquire() as conn:
        wh_status = await conn.fetchrow(
            "SELECT status, attempt_count, last_status_code FROM webhook_deliveries WHERE message_id = $1",
            sched_id,
        )
    assert wh_status["status"] == "DELIVERED"
    assert wh_status["attempt_count"] == 2
    assert wh_status["last_status_code"] == 200

    wh_body, wh_sig = captured[1]
    expected_wh_sig = hmac.new(b"whsec_acme_secret_01", wh_body, hashlib.sha256).hexdigest()
    assert wh_sig == expected_wh_sig

    # 4. Prometheus /metrics endpoint
    r_metrics = await client.get("/metrics")
    assert r_metrics.status_code == 200
    assert "sms_queue_depth" in r_metrics.text
