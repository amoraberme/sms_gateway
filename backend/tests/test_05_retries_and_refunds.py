from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import pytest

from app.carriers.client import CarrierClient, CarrierSubmitResponse
from app.carriers.router import CarrierRouter
from app.workers.dispatch_worker import process_one_message
from tests.conftest import ACME_KEY, sign_dlr


class SequenceCarrierClient(CarrierClient):
    def __init__(self, responses: list[CarrierSubmitResponse]) -> None:
        self._responses = list(responses)
        self.calls = 0

    async def submit(self, base_url, message_id, to_number, from_sender, text, encoding):
        idx = min(self.calls, len(self._responses) - 1)
        self.calls += 1
        return self._responses[idx]


@pytest.mark.asyncio
async def test_429_503_retried_400_not_retried_and_undeliv_not_refunded(db_and_app):
    client, pool, scheduler = db_and_app
    router = CarrierRouter(pool, scheduler.redis)

    # Case A: 429 -> 503 -> 200 (retried and succeeds; 0 refunds)
    r_a = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "Retry success"},
    )
    msg_a = r_a.json()["message_id"]
    carrier_a = SequenceCarrierClient(
        [
            CarrierSubmitResponse(status_code=429, error="TOO_MANY_REQUESTS"),
            CarrierSubmitResponse(status_code=503, error="TEMPORARY_FAILURE"),
            CarrierSubmitResponse(status_code=200, carrier_id="c-retry-ok"),
        ]
    )
    for _ in range(3):
        async with pool.acquire() as conn:
            await conn.execute("UPDATE messages SET next_attempt_at = now() - interval '1 second' WHERE id = $1", msg_a)
        await process_one_message(pool, scheduler, router, carrier_a, msg_a)

    assert carrier_a.calls == 3
    det_a = (await client.get(f"/v1/messages/{msg_a}", headers={"X-API-Key": ACME_KEY})).json()
    assert det_a["status"] == "SUBMITTED"
    assert det_a["attempt_count"] == 3

    # Deliver UNDELIV DLR -> terminal UNDELIVERED, must NOT refund!
    raw_u, sig_u = sign_dlr(
        {"carrier_id": "c-retry-ok", "status": "UNDELIV", "timestamp": datetime.now(timezone.utc).isoformat()}
    )
    await client.post("/carrier/dlr", content=raw_u, headers={"X-Signature": sig_u})

    # Case B: 400 INVALID_DESTINATION -> immediate FAILED, 0 retries, 1 REFUND
    r_b = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000002", "from": "ACME", "text": "Bad dest"},
    )
    msg_b = r_b.json()["message_id"]
    carrier_b = SequenceCarrierClient([CarrierSubmitResponse(status_code=400, error="INVALID_DESTINATION")])
    await process_one_message(pool, scheduler, router, carrier_b, msg_b)

    assert carrier_b.calls == 1
    det_b = (await client.get(f"/v1/messages/{msg_b}", headers={"X-API-Key": ACME_KEY})).json()
    assert det_b["status"] == "FAILED"
    assert det_b["attempt_count"] == 1

    # Verify balance: Case A ($0.02) stays debited (UNDELIVERED), Case B ($0.02) refunded -> $499.9800
    bal = (await client.get("/v1/balance", headers={"X-API-Key": ACME_KEY})).json()
    assert Decimal(bal["balance"]) == Decimal("499.9800")
    assert bal["invariant_ok"] is True
