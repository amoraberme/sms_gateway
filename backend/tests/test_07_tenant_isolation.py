from __future__ import annotations

import pytest

from tests.conftest import ACME_KEY, SHOPFAST_KEY


@pytest.mark.asyncio
async def test_tenant_isolation_returns_404_cross_tenant(db_and_app):
    client, _, _ = db_and_app

    r = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "Confidential OTP 7788"},
    )
    assert r.status_code == 202
    acme_msg_id = r.json()["message_id"]

    # ShopFast requesting Acme's message must get 404
    r_cross = await client.get(
        f"/v1/messages/{acme_msg_id}",
        headers={"X-API-Key": SHOPFAST_KEY},
    )
    assert r_cross.status_code == 404

    # ShopFast's list must not contain Acme's message
    r_list = await client.get("/v1/messages", headers={"X-API-Key": SHOPFAST_KEY})
    assert r_list.status_code == 200
    assert r_list.json()["total"] == 0
