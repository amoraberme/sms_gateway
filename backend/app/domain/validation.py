from __future__ import annotations

import re

# Strict E.164: '+' followed by 7 to 15 digits, first digit 1-9. Never guesses.
E164_REGEX = re.compile(r"^\+[1-9]\d{6,14}$")


def validate_e164(phone_number: str) -> str:
    if not isinstance(phone_number, str) or not E164_REGEX.fullmatch(phone_number):
        raise ValueError(
            f"Invalid E.164 phone number '{phone_number}'. Must match ^\\+[1-9]\\d{{6,14}}$ without spaces or punctuation."
        )
    return phone_number


def validate_sender_id(sender: str) -> str:
    if not isinstance(sender, str):
        raise ValueError("Sender ID must be a string")
    cleaned = sender.strip()
    if not cleaned or len(cleaned) > 20:
        raise ValueError("Sender ID ('from') must be between 1 and 20 characters")
    return cleaned


def validate_message_text(text: str) -> str:
    if not isinstance(text, str) or len(text) == 0:
        raise ValueError("Message 'text' must not be empty")
    if len(text) > 1600:
        raise ValueError("Message 'text' exceeds maximum length of 1600 characters")
    return text
