"""Utilities for hashing and verifying passwords with bcrypt."""

from __future__ import annotations

import bcrypt


def hash_password(password: str) -> str:
    """Hash the provided password using bcrypt."""
    password_bytes = password.encode("utf-8")
    hashed = bcrypt.hashpw(password_bytes, bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Verify that the provided password matches the stored hash."""
    password_bytes = password.encode("utf-8")
    stored_bytes = password_hash.encode("utf-8")
    return bcrypt.checkpw(password_bytes, stored_bytes)
