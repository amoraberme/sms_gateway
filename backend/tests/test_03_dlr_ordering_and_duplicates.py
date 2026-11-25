from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import UUID
import pytest

from app.carriers.client import CarrierClient, CarrierSubmitResponse
from app.carriers.router import CarrierRouter
from app.workers.dispatch_worker import process_one_message
from tests.conftest import ACME_KEY, sign_dlr


class EarlyDlrCarrierClient(CarrierClient):
    """Simulates the ~5% race where /carrier/dlr arrives BEFORE /submit returns."""

    def __init__(self, asgi_client, carrier_id: str) -> None:
        self.asgi_client = asgi_client
        self.carrier_id = carrier_id

    async def submit(self, base_url, message_id, to_number, from_sender, text, encoding):
        dlr_payload = {
            "carrier_id": self.carrier_id,
            "status": "DELIVRD",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        raw, sig = sign_dlr(dlr_payload)
        # Early DLR arrives while /submit is still in-flight!
        r_dlr = await self.asgi_client.post(
            "/carrier/dlr",
            content=raw,
            headers={"Content-Type": "application/json", "X-Signature": sig},
        )
        assert r_dlr.status_code == 200
        assert r_dlr.json()["status"] == "buffered_early_dlr"

        await asyncio.sleep(0.02)
        return CarrierSubmitResponse(status_code=200, carrier_id=self.carrier_id)


@pytest.mark.asyncio
async def test_out_of_order_and_duplicate_dlr(db_and_app):
    client, pool, scheduler = db_and_app

    send_resp = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "Race test OTP"},
    )
    assert send_resp.status_code == 202
    msg_id = send_resp.json()["message_id"]

    popped = await scheduler.pop_fair()
    assert popped == msg_id

    carrier_id = "c-early-race-001"
    router = CarrierRouter(pool, scheduler.redis)
    fake_carrier = EarlyDlrCarrierClient(client, carrier_id)

    await process_one_message(pool, scheduler, router, fake_carrier, msg_id)

    # Duplicate DLR arrives after submission is already DELIVERED
    dup_payload = {
        "carrier_id": carrier_id,
        "status": "DELIVRD",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    raw, sig = sign_dlr(dup_payload)
    r_dup = await client.post(
        "/carrier/dlr",
        content=raw,
        headers={"Content-Type": "application/json", "X-Signature": sig},
    )
    assert r_dup.status_code == 200
    assert r_dup.json()["status"] == "duplicate_ignored"

    # Verify final state is DELIVERED (not overwritten back to SUBMITTED) & events are clean
    detail = await client.get(f"/v1/messages/{msg_id}", headers={"X-API-Key": ACME_KEY})
    assert detail.status_code == 200
    body = detail.json()
    assert body["status"] == "DELIVERED"
    statuses = [e["status"] for e in body["events"]]
    assert statuses == ["ACCEPTED", "QUEUED", "SUBMITTED", "DELIVERED"]
