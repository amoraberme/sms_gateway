from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
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
async def test_bonus_failover_scheduling_webhooks_and_metrics(db_and_app):
    client, pool, scheduler = db_and_app
    router = CarrierRouter(pool, scheduler.redis)

    # 1. Scheduled message (+2s in future): stays ACCEPTED until due
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

    # 3. Tenant webhook signed with X-Signature HMAC-SHA256
    async with pool.acquire() as conn:
        await conn.execute("UPDATE tenants SET webhook_url = 'http://tenant.local/hook' WHERE slug = 'acme-bank'")

    raw_dlr, sig_dlr = sign_dlr(
        {"carrier_id": "c-sec-ok", "status": "DELIVRD", "timestamp": datetime.now(timezone.utc).isoformat()}
    )
    await client.post("/carrier/dlr", content=raw_dlr, headers={"X-Signature": sig_dlr})

    captured: list[tuple[bytes, str]] = []

    class CaptureTransport(pytest.importorskip("httpx").AsyncBaseTransport):
        async def handle_async_request(self, request):
            captured.append((request.content, request.headers.get("X-Signature", "")))
            return pytest.importorskip("httpx").Response(200, json={"ok": True})

    import httpx

    async with httpx.AsyncClient(transport=CaptureTransport()) as wh_client:
        processed = await process_pending_webhooks_once(pool, wh_client)
        assert processed == 1

    assert len(captured) == 1
    wh_body, wh_sig = captured[0]
    expected_wh_sig = hmac.new(b"whsec_acme_secret_01", wh_body, hashlib.sha256).hexdigest()
    assert wh_sig == expected_wh_sig

    # 4. Prometheus /metrics endpoint
    r_metrics = await client.get("/metrics")
    assert r_metrics.status_code == 200
    assert "sms_queue_depth" in r_metrics.text
