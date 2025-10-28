"""CLI utility to create application users for development/demo scenarios."""

from __future__ import annotations

import argparse
import sys

from sqlalchemy.exc import IntegrityError

from backend.app.config import get_settings
from backend.app.core.db import SessionLocal
from backend.app.core.security import JWTService
from backend.app.models.app_user import VALID_ROLES
from backend.app.services.user_service import UserService


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Create an application user with a role and hashed password."
    )
    parser.add_argument("--username", required=True, help="Username for the new user.")
    parser.add_argument(
        "--password",
        required=True,
        help="Password for the new user (stored as bcrypt hash).",
    )
    parser.add_argument(
        "--role",
        choices=VALID_ROLES,
        default="analyst",
        help="Role assigned to the user.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the CLI utility."""
    args = parse_args(argv)
    settings = get_settings()

    session = SessionLocal()
    try:
        jwt_service = JWTService(settings.jwt_secret, settings.jwt_algorithm)
        service = UserService(session, jwt_service)
        user = service.create_user(args.username, args.password, args.role)
    except IntegrityError:
        message = f"User '{args.username}' already exists."
        print(message, file=sys.stderr)
        return 1
    except ValueError as err:
        print(str(err), file=sys.stderr)
        return 1
    finally:
        session.close()

    print(
        f"User created: id={user.user_id}, username={user.username}, role={user.role}",
        file=sys.stdout,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    sys.exit(main())
