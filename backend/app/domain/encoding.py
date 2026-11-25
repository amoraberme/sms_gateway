from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Literal

# Full GSM 03.38 basic character set (1 septet each)
GSM7_BASIC: frozenset[str] = frozenset(
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ"
    "ÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§"
    "¿abcdefghijklmnopqrstuvwxyzäöñüà"
)

# GSM 03.38 extension table (each requires ESC 0x1B + char = 2 septets)
GSM7_EXT: frozenset[str] = frozenset("^{}\\[~]|€\f")


@dataclass(frozen=True)
class EncodingAnalysis:
    encoding: Literal["GSM7", "UCS2"]
    units: int
    segments: int


def analyze_encoding(text: str) -> EncodingAnalysis:
    """Detect GSM-7 vs UCS-2 encoding and compute exact segment count."""
    if not text:
        raise ValueError("Message text must not be empty")

    is_gsm7 = True
    septets = 0
    for ch in text:
        if ch in GSM7_BASIC:
            septets += 1
        elif ch in GSM7_EXT:
            septets += 2
        else:
            is_gsm7 = False
            break

    if is_gsm7:
        segments = 1 if septets <= 160 else math.ceil(septets / 153)
        return EncodingAnalysis(encoding="GSM7", units=septets, segments=segments)

    # UCS-2 / UTF-16 code units: BMP chars (incl. Bangla) = 1 unit, astral emojis = 2 units
    utf16_units = len(text.encode("utf-16-be")) // 2
    segments = 1 if utf16_units <= 70 else math.ceil(utf16_units / 67)
    return EncodingAnalysis(encoding="UCS2", units=utf16_units, segments=segments)
