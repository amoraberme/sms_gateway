from __future__ import annotations

from typing import Literal

MessageStatus = Literal[
    "ACCEPTED",
    "QUEUED",
    "SUBMITTED",
    "DELIVERED",
    "UNDELIVERED",
    "EXPIRED",
    "FAILED",
]

STATUS_RANKS: dict[str, int] = {
    "ACCEPTED": 10,
    "QUEUED": 20,
    "SUBMITTED": 30,
    "DELIVERED": 40,
    "UNDELIVERED": 40,
    "EXPIRED": 40,
    "FAILED": 40,
}

TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"DELIVERED", "UNDELIVERED", "EXPIRED", "FAILED"}
)

DLR_STATUS_MAP: dict[str, str] = {
    "DELIVRD": "DELIVERED",
    "UNDELIV": "UNDELIVERED",
    "EXPIRED": "EXPIRED",
}


def can_transition(current: str, target: str) -> bool:
    """Enforce strictly monotonic forward-only transitions."""
    if current in TERMINAL_STATUSES:
        return False
    return STATUS_RANKS[target] > STATUS_RANKS[current]


def map_dlr_status(raw_status: str) -> str:
    mapped = DLR_STATUS_MAP.get(raw_status)
    if mapped is None:
        raise ValueError(f"Unsupported DLR status: {raw_status}")
    return mapped
