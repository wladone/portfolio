"""Tests for JWT key rotation."""

import json
from datetime import UTC, datetime

import jwt
import pytest
from fastapi.testclient import TestClient

from backend.app.core.jwt_keyring import load_keyset, sign, verify
from scripts.rotate_jwt_keys import generate_key, rotate


@pytest.fixture
def test_keyset(tmp_path):
    """Create a test keyset."""
    keys = {
        "keys": [
            {
                "kid": "test-1",
                "kty": "oct",
                "alg": "HS256",
                "k": generate_key(),
                "active": True,
                "created_at": datetime.now(UTC).isoformat(),
            }
        ]
    }
    path = tmp_path / "test_keys.json"
    with open(path, "w") as f:
        json.dump(keys, f)
    return str(path)


def test_sign_verify_with_kid(test_keyset):
    """Test signing and verifying with key ID."""
    keyset = load_keyset(test_keyset)
    active_key = keyset.get_active_key()
    payload = {"sub": "test", "exp": 1735689600}  # 2025-01-01

    token, kid = sign(payload, active_key)
    assert kid == active_key.kid

    # Verify token has kid in header
    header = jwt.get_unverified_header(token)
    assert header["kid"] == active_key.kid

    # Verify claims
    claims = verify(token, keyset)
    assert claims["sub"] == "test"


def test_rotate_switches_active(test_keyset):
    """Test rotating active key."""
    # Add new key
    rotate(test_keyset, "test-2")

    # Load new keyset
    keyset = load_keyset(test_keyset)

    # Check new key is active
    active_key = keyset.get_active_key()
    assert active_key.kid == "test-2"
    assert active_key.active

    # Check old key still works for verification
    old_key = keyset.get_key_by_kid("test-1")
    assert not old_key.active

    # Create tokens with both keys
    payload = {"sub": "test"}
    token_new, _ = sign(payload, active_key)
    token_old, _ = sign(payload, old_key)

    # Both should verify
    verify(token_new, keyset)
    verify(token_old, keyset)


def test_jwks_exposes_keys_oct(test_keyset, client: TestClient):
    """Test JWKS endpoint returns correct format."""
    response = client.get("/.well-known/jwks.json")
    assert response.status_code == 200

    data = response.json()
    assert "keys" in data

    for key in data["keys"]:
        assert key["kty"] == "oct"
        assert "kid" in key
        assert "k" in key
        assert key["alg"] == "HS256"
        assert key["use"] == "sig"
