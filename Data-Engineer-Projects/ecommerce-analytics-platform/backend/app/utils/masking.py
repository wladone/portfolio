"""Utilities for masking personally identifiable information."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def mask_phone(phone: str | None) -> str | None:
    """Return a masked representation of a phone number."""
    if phone is None:
        return None

    digits = "".join(ch for ch in phone if ch.isdigit())
    if not digits:
        return "*" * len(phone)

    visible = min(4, len(digits))
    masked_digits = f"{'*' * (len(digits) - visible)}{digits[-visible:]}"
    return masked_digits


def mask_email(email: str | None) -> str | None:
    """Return a masked representation of an email address."""
    if email is None:
        return None

    if "@" not in email:
        return "***"

    local, domain = email.split("@", 1)
    return f"***@{domain}"


def safe_customer_payload(customer: Mapping[str, Any]) -> dict[str, Any]:
    """Return a payload without direct PII fields."""
    allowed_keys = {
        "customer_nk",
        "email_hash",
        "country_code",
        "first_name",
        "last_name",
    }
    sanitized = {key: customer.get(key) for key in allowed_keys if key in customer}
    sanitized["phone_masked"] = mask_phone(customer.get("phone"))
    return sanitized
