"""JWT keyring with key rotation support."""

import json
from datetime import datetime
from typing import Any

import jwt
from pydantic import BaseModel

from backend.app.core.exceptions import AuthenticationError


class JWTKey(BaseModel):
    """Single JWT key in the keyset."""

    kid: str
    kty: str = "oct"  # Only HS256 supported for now
    alg: str = "HS256"
    k: str  # Base64URL-encoded secret
    active: bool = False
    created_at: datetime


class JWTKeySet(BaseModel):
    """Collection of JWT keys."""

    keys: list[JWTKey]

    def get_active_key(self, override_kid: str | None = None) -> JWTKey:
        """Get the active key or a specific one by kid."""
        if override_kid:
            key = next((k for k in self.keys if k.kid == override_kid), None)
            if not key:
                raise AuthenticationError(f"Key ID {override_kid} not found")
            return key

        active_keys = [k for k in self.keys if k.active]
        if not active_keys:
            raise RuntimeError("No active key found in keyset")
        if len(active_keys) > 1:
            raise RuntimeError("Multiple active keys found")
        return active_keys[0]

    def get_key_by_kid(self, kid: str) -> JWTKey | None:
        """Get a key by its ID."""
        return next((k for k in self.keys if k.kid == kid), None)


def load_keyset(path: str) -> JWTKeySet:
    """Load and validate a keyset from file."""
    try:
        with open(path) as f:
            data = json.load(f)
        keyset = JWTKeySet.model_validate(data)

        # Validate only one active key
        active_keys = [k for k in keyset.keys if k.active]
        if len(active_keys) > 1:
            raise ValueError("Multiple active keys found")

        return keyset
    except Exception as e:
        raise RuntimeError(f"Failed to load JWT keyset: {e}")


def sign(payload: dict[str, Any], key: JWTKey) -> tuple[str, str]:
    """Sign a payload with the given key.

    Returns:
        Tuple of (jwt_token, kid)
    """
    headers = {"kid": key.kid, "alg": "HS256"}
    secret = key.k.encode()  # Base64URL-decoded by PyJWT
    token = jwt.encode(payload, secret, algorithm="HS256", headers=headers)
    return token, key.kid


def verify(token: str, keyset: JWTKeySet) -> dict[str, Any]:
    """Verify a token against the keyset.

    Args:
        token: JWT token to verify
        keyset: Current keyset of valid keys

    Returns:
        Decoded claims if valid

    Raises:
        AuthenticationError: If token is invalid or expired
    """
    try:
        # Try to decode headers without verification
        headers = jwt.get_unverified_header(token)
        kid = headers.get("kid")

        if kid:
            # If we have a key ID, try that key first
            key = keyset.get_key_by_kid(kid)
            if key:
                return jwt.decode(token, key.k.encode(), algorithms=["HS256"])

        # Otherwise (or if specified key failed), try all keys
        for key in keyset.keys:
            try:
                return jwt.decode(token, key.k.encode(), algorithms=["HS256"])
            except jwt.InvalidTokenError:
                continue

        raise AuthenticationError("No key could verify the token")

    except jwt.InvalidTokenError as e:
        raise AuthenticationError(f"Invalid token: {e}")


def to_jwks(keyset: JWTKeySet) -> dict[str, list[dict[str, str]]]:
    """Convert keyset to JWKS format."""
    return {
        "keys": [
            {"kid": key.kid, "kty": key.kty, "alg": key.alg, "k": key.k, "use": "sig"}
            for key in keyset.keys
        ]
    }
