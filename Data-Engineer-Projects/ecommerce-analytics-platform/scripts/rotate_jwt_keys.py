#!/usr/bin/env python3
"""Script for managing JWT keys."""

import argparse
import base64
import json
import os
import secrets
import shutil
from datetime import UTC, datetime

from backend.app.core.jwt_keyring import JWTKeySet


def generate_key() -> str:
    """Generate a random 32-byte key as base64url."""
    key_bytes = secrets.token_bytes(32)
    return base64.urlsafe_b64encode(key_bytes).decode("ascii")


def load_keys(path: str) -> dict:
    """Load keys from file."""
    if not os.path.exists(path):
        return {"keys": []}
    with open(path) as f:
        return json.load(f)


def save_keys(path: str, data: dict) -> None:
    """Save keys to file with backup."""
    # Create backup
    if os.path.exists(path):
        backup = f"{path}.bak.{int(datetime.now().timestamp())}"
        shutil.copy2(path, backup)

    # Save new data
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def mask_key(key: str) -> str:
    """Mask key value for display."""
    if len(key) <= 8:
        return "****"
    return f"{key[:4]}...{key[-4:]}"


def list_keys(path: str) -> None:
    """List all keys in the keyset."""
    data = load_keys(path)
    print("\nCurrent keys:")
    print("-" * 80)
    print(f"{'KID':<20} {'Status':<10} {'Created':<25} {'Key':<20}")
    print("-" * 80)

    for key in data.get("keys", []):
        status = "ACTIVE" if key.get("active") else "inactive"
        created = key.get("created_at", "unknown")
        masked = mask_key(key.get("k", ""))
        print(
            f"{key.get('kid', 'unknown'):<20} {status:<10} {created:<25} {masked:<20}"
        )
    print()


def rotate(path: str, kid: str) -> None:
    """Add a new active key and deactivate the old one."""
    data = load_keys(path)

    # Deactivate all existing keys
    for key in data.get("keys", []):
        key["active"] = False

    # Add new active key
    new_key = {
        "kid": kid,
        "kty": "oct",
        "alg": "HS256",
        "k": generate_key(),
        "active": True,
        "created_at": datetime.now(UTC).isoformat(),
    }
    data["keys"] = data.get("keys", []) + [new_key]

    # Validate with pydantic
    JWTKeySet.model_validate(data)

    # Save changes
    save_keys(path, data)
    print(f"\nRotated to new key {kid}")
    list_keys(path)


def revoke(path: str, kid: str) -> None:
    """Revoke a specific key by kid."""
    data = load_keys(path)
    found = False

    for key in data.get("keys", []):
        if key.get("kid") == kid:
            key["active"] = False
            found = True
            break

    if not found:
        print(f"Key {kid} not found")
        return

    save_keys(path, data)
    print(f"\nRevoked key {kid}")
    list_keys(path)


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="JWT key management")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # List command
    subparsers.add_parser("list", help="List all keys")

    # Rotate command
    rotate_parser = subparsers.add_parser("rotate", help="Add new active key")
    rotate_parser.add_argument("--kid", required=True, help="Key ID for new key")

    # Revoke command
    revoke_parser = subparsers.add_parser("revoke", help="Revoke a key")
    revoke_parser.add_argument("--kid", required=True, help="Key ID to revoke")

    args = parser.parse_args()

    # Default path from settings
    path = "infra/secrets/jwt_keys.json"

    if args.command == "list":
        list_keys(path)
    elif args.command == "rotate":
        rotate(path, args.kid)
    elif args.command == "revoke":
        revoke(path, args.kid)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
