"""Security helpers for JWT-based authentication."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import jwt
from jwt import InvalidTokenError


class JWTService:
    """JWT encode/decode helpers."""

    def __init__(self, secret: str, algorithm: str) -> None:
        self._secret = secret
        self._algorithm = algorithm

    def encode(
        self, payload: dict[str, Any], expires_in: timedelta | None = None
    ) -> str:
        """Encode a JWT payload."""
        to_encode = payload.copy()
        now = datetime.now(UTC)
        to_encode.setdefault("iat", now)
        if expires_in is not None:
            to_encode["exp"] = now + expires_in
        return jwt.encode(to_encode, self._secret, algorithm=self._algorithm)

    def decode(self, token: str) -> dict[str, Any]:
        """Decode and validate a JWT token."""
        try:
            payload = jwt.decode(
                token,
                self._secret,
                algorithms=[self._algorithm],
                options={"require_iat": True},
            )
        except InvalidTokenError as exc:  # pragma: no cover - mapped in dependencies
            raise exc
        return payload
