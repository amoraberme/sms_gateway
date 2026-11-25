from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping


class UnsupportedDestinationError(ValueError):
    """Raised when no country prefix in the price table matches the destination."""


@dataclass(frozen=True)
class PriceQuote:
    prefix: str
    unit_price: Decimal
    segments: int
    total_cost: Decimal


def resolve_price(
    to_number: str,
    segments: int,
    price_table: Mapping[str, Decimal],
) -> PriceQuote:
    """Longest-prefix match against the seeded price table."""
    matched_prefix: str | None = None
    for prefix in sorted(price_table.keys(), key=len, reverse=True):
        if to_number.startswith(prefix):
            matched_prefix = prefix
            break

    if matched_prefix is None:
        raise UnsupportedDestinationError(
            f"Destination '{to_number}' is not covered by any configured country prefix price."
        )

    unit_price = Decimal(price_table[matched_prefix]).quantize(Decimal("0.0001"))
    total_cost = (unit_price * Decimal(segments)).quantize(Decimal("0.0001"))
    return PriceQuote(
        prefix=matched_prefix,
        unit_price=unit_price,
        segments=segments,
        total_cost=total_cost,
    )
