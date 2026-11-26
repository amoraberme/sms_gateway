from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib.util
from pathlib import Path
import time
import uuid

import httpx
import pytest

from app.carriers.client import CarrierClient, CarrierSubmitResponse
from app.carriers.router import CarrierRouter
from app.workers.dispatch_worker import dispatch_worker_loop
from tests.conftest import ACME_KEY, SHOPFAST_KEY


def _load_mock_carrier_module():
    import sys
    path = Path(__file__).resolve().parents[2] / "mock_carrier" / "app.py"
    spec = importlib.util.spec_from_file_location("mock_carrier_app", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules["mock_carrier_app"] = mod
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.asyncio
async def test_mock_carrier_contract_429_503_400_early_dup_and_hmac():
    mc = _load_mock_carrier_module()
    mc.rps_window.clear()
    mc.accepted_cache.clear()
    mc.cfg.min_latency_ms = 1
    mc.cfg.max_latency_ms = 2
    mc.cfg.dlr_min_delay_sec = 0.01
    mc.cfg.dlr_max_delay_sec = 0.02

    captured_dlrs: list[tuple[bytes, str]] = []

    class DlrCaptureTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            captured_dlrs.append((request.content, request.headers.get("X-Signature", "")))
            return httpx.Response(200, json={"status": "ok"})

    mc.http_client = httpx.AsyncClient(transport=DlrCaptureTransport())
    transport = httpx.ASGITransport(app=mc.app)

    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://carrier") as c:
            # 1. Invalid encoding rejected with 400
            r_bad = await c.post(
                "/submit",
                json={"message_id": "m0", "to": "+8801711000001", "from": "ACME", "text": "Hi", "encoding": "UTF8"},
            )
            assert r_bad.status_code == 400

            # 2. Early + Duplicate DLR with valid X-Signature HMAC-SHA256
            mc.cfg.rate_400 = 0.0
            mc.cfg.rate_503 = 0.0
            mc.cfg.rate_timeout = 0.0
            mc.cfg.rate_early_dlr = 1.0
            mc.cfg.rate_duplicate_dlr = 1.0
            r_ok = await c.post(
                "/submit",
                json={"message_id": "m1", "to": "+8801711000001", "from": "ACME", "text": "Hi", "encoding": "GSM7"},
            )
            assert r_ok.status_code == 200
            cid = r_ok.json()["carrier_id"]
            assert cid.startswith("c-")
            assert len(captured_dlrs) == 2
            for raw_body, sig in captured_dlrs:
                expected = hmac.new(mc.cfg.dlr_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
                assert sig == expected

            # 3. message_id deduplication returns identical carrier_id
            r_dup = await c.post(
                "/submit",
                json={"message_id": "m1", "to": "+8801711000001", "from": "ACME", "text": "Hi", "encoding": "GSM7"},
            )
            assert r_dup.status_code == 200
            assert r_dup.json()["carrier_id"] == cid

            # 4. 400 & 503 injection
            mc.cfg.force_status = 400
            assert (await c.post("/submit", json={"message_id": "m2", "to": "+8801711000001", "from": "ACME", "text": "Hi", "encoding": "GSM7"})).status_code == 400
            mc.cfg.force_status = 503
            assert (await c.post("/submit", json={"message_id": "m3", "to": "+8801711000001", "from": "ACME", "text": "Hi", "encoding": "GSM7"})).status_code == 503

            # 5. Sliding-window > MAX_RPS returns 429
            mc.cfg.force_status = None
            mc.cfg.rate_early_dlr = 0.0
            mc.cfg.rate_duplicate_dlr = 0.0
            mc.rps_window.clear()
            mc.cfg.max_rps = 5
            codes = []
            for i in range(8):
                resp = await c.post(
                    "/submit",
                    json={"message_id": f"rps-{i}", "to": "+8801711000001", "from": "ACME", "text": "Hi", "encoding": "GSM7"},
                )
                codes.append(resp.status_code)
            assert codes[:5] == [200] * 5
            assert codes[5:] == [429] * 3
    finally:
        await mc.http_client.aclose()


@pytest.mark.asyncio
async def test_global_50_rps_rate_limit_across_workers(db_and_app):
    client, pool, scheduler = db_and_app
    router = CarrierRouter(pool, scheduler.redis)

    r_batch = await client.post(
        "/v1/messages/batch",
        headers={"X-API-Key": SHOPFAST_KEY},
        json={
            "messages": [
                {"to": "+8801711000001", "from": "SHOP", "text": f"RPS {i}"}
                for i in range(100)
            ]
        },
    )
    assert r_batch.status_code == 202

    timestamps: list[float] = []

    class FastCarrier(CarrierClient):
        async def submit(self, base_url, message_id, to_number, from_sender, text, encoding):
            timestamps.append(time.monotonic())
            await asyncio.sleep(0.002)
            return CarrierSubmitResponse(status_code=200, carrier_id=f"c-{uuid.uuid4().hex[:10]}")

    stop_evt = asyncio.Event()
    fast_carrier = FastCarrier()
    workers = [
        asyncio.create_task(dispatch_worker_loop(i, pool, scheduler, router, fast_carrier, stop_evt))
        for i in range(16)
    ]
    deadline = time.monotonic() + 20.0
    while len(timestamps) < 100 and time.monotonic() < deadline:
        await asyncio.sleep(0.05)
    stop_evt.set()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    timestamps.sort()
    max_in_1s = 0
    j = 0
    for i in range(len(timestamps)):
        while timestamps[i] - timestamps[j] >= 1.0:
            j += 1
        max_in_1s = max(max_in_1s, i - j + 1)

    assert len(timestamps) == 100
    assert max_in_1s <= 50


@pytest.mark.asyncio
async def test_priority_and_50_tenant_fair_round_robin(db_and_app):
    client, _, scheduler = db_and_app

    # 1. 2-tenant priority: 5 Acme OTPs jump ahead of 300 ShopFast bulk messages
    await client.post(
        "/v1/messages/batch",
        headers={"X-API-Key": SHOPFAST_KEY},
        json={"messages": [{"to": "+8801711000001", "from": "SHOP", "text": f"B{i}"} for i in range(300)]},
    )
    otp_ids = set()
    for i in range(5):
        r = await client.post(
            "/v1/messages",
            headers={"X-API-Key": ACME_KEY},
            json={"to": "+8801711999999", "from": "ACME", "text": f"OTP {i}"},
        )
        otp_ids.add(r.json()["message_id"])

    first_10 = [await scheduler.pop_fair() for _ in range(10)]
    assert sum(1 for m in first_10 if m in otp_ids) == 5

    # 2. 50-tenant fairness: flush and enqueue 50 tenants in normal tier
    await scheduler.redis.flushdb()
    items = []
    # Tenant 0 sends 100 messages; Tenants 1..49 send 3 each
    for i in range(100):
        items.append((f"t0-{i}", "tenant-00", "normal"))
    for t in range(1, 50):
        for i in range(3):
            items.append((f"t{t}-{i}", f"tenant-{t:02d}", "normal"))
    await scheduler.enqueue_many(items)

    popped_tenants = set()
    for _ in range(50):
        mid = await scheduler.pop_fair()
        assert mid is not None
        popped_tenants.add(mid.split("-")[0])

    # Every single one of the 50 tenants is served once before any tenant gets a 2nd turn
    assert len(popped_tenants) == 50
