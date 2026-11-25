from __future__ import annotations

from decimal import Decimal
import pytest

from app.domain.encoding import analyze_encoding
from app.domain.pricing import UnsupportedDestinationError, resolve_price
from app.domain.validation import validate_e164
from tests.conftest import ACME_KEY


def test_gsm7_and_extension_chars_and_ucs2_unit():
    # 160 basic GSM-7 chars -> 1 segment
    a160 = analyze_encoding("A" * 160)
    assert a160.encoding == "GSM7"
    assert a160.units == 160
    assert a160.segments == 1

    # 161 basic GSM-7 chars -> 2 segments (153 per segment)
    a161 = analyze_encoding("A" * 161)
    assert a161.encoding == "GSM7"
    assert a161.units == 161
    assert a161.segments == 2

    # Extension characters ({ } [ ] ~ \ ^ | €) count as 2 septets each
    ext80 = analyze_encoding("€" * 80)
    assert ext80.encoding == "GSM7"
    assert ext80.units == 160
    assert ext80.segments == 1

    ext81 = analyze_encoding("€" * 81)
    assert ext81.encoding == "GSM7"
    assert ext81.units == 162
    assert ext81.segments == 2

    # Mixed extension chars
    mix = analyze_encoding("Price: {100} [USD] ~ €50 | ^test\\")
    assert mix.encoding == "GSM7"

    # Bangla text -> UCS-2 (70 / 67)
    bangla = analyze_encoding("আপনার ওটিপি কোড হলো ১২৩৪৫৬")
    assert bangla.encoding == "UCS2"
    assert bangla.segments == 1

    bangla_long = analyze_encoding("ব" * 71)
    assert bangla_long.encoding == "UCS2"
    assert bangla_long.units == 71
    assert bangla_long.segments == 2

    # Emoji (astral plane) -> UCS-2 (2 UTF-16 code units)
    emoji = analyze_encoding("Your OTP is 1234 🚀")
    assert emoji.encoding == "UCS2"
    assert emoji.units == 19  # 17 ASCII + 2 UTF-16 units for 🚀
    assert emoji.segments == 1


def test_pricing_and_e164_validation():
    prices = {"+880": Decimal("0.0200"), "+971": Decimal("0.0500")}
    q = resolve_price("+8801711000001", 2, prices)
    assert q.prefix == "+880"
    assert q.total_cost == Decimal("0.0400")

    with pytest.raises(UnsupportedDestinationError):
        resolve_price("+819012345678", 1, prices)

    with pytest.raises(ValueError):
        validate_e164("01711000001")
    with pytest.raises(ValueError):
        validate_e164("+880 1711000001")


@pytest.mark.asyncio
async def test_api_rejects_unpriced_and_bad_e164(db_and_app):
    client, pool, _ = db_and_app

    # Unpriced destination (+81 Japan) -> 400
    r1 = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+819012345678", "from": "ACME", "text": "OTP 1234"},
    )
    assert r1.status_code == 400

    # Malformed E.164 -> 400
    r2 = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "01711000001", "from": "ACME", "text": "OTP 1234"},
    )
    assert r2.status_code == 400

    # Multi-segment UCS-2 Bangla message -> 2 segments * $0.0200 = $0.0400
    r3 = await client.post(
        "/v1/messages",
        headers={"X-API-Key": ACME_KEY},
        json={"to": "+8801711000001", "from": "ACME", "text": "ব" * 71},
    )
    assert r3.status_code == 202
    data = r3.json()
    assert data["encoding"] == "UCS2"
    assert data["segments"] == 2
    assert Decimal(data["cost"]) == Decimal("0.0400")
