"""Service layer encapsulating user management and authentication logic."""

from __future__ import annotations

from datetime import timedelta

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.core.security import JWTService
from backend.app.models.app_user import VALID_ROLES, AppUser
from backend.app.repositories.user_repo import UserRepository
from backend.app.security.passwords import hash_password, verify_password


class UserService:
    """Provides higher-level operations for managing application users."""

    def __init__(self, session: Session, jwt_service: JWTService) -> None:
        self._session = session
        self._repo = UserRepository(session)
        self._jwt_service = jwt_service

    def create_user(self, username: str, password: str, role: str) -> AppUser:
        """Create a new user with the provided credentials."""
        if role not in VALID_ROLES:
            raise ValueError(f"Invalid role '{role}'. Expected one of {VALID_ROLES}.")
        if not username:
            raise ValueError("Username must be provided.")
        if not password:
            raise ValueError("Password must be provided.")

        password_hash_value = hash_password(password)

        try:
            user = self._repo.create(username, password_hash_value, role)
            self._session.commit()
            self._session.refresh(user)
            return user
        except IntegrityError as exc:
            self._session.rollback()
            raise exc

    def authenticate(self, username: str, password: str) -> AppUser | None:
        """Authenticate a user by username and password."""
        user = self._repo.get_by_username(username)
        if user is None or not user.is_active:
            return None

        if not verify_password(password, user.password_hash):
            return None

        return user

    def issue_access_token(
        self,
        user: AppUser,
        expires_in: timedelta,
        audience: str | None = None,
    ) -> str:
        """Issue a signed JWT access token for the given user."""
        payload = {
            "sub": user.username,
            "role": user.role,
        }
        if audience is not None:
            payload["aud"] = audience

        return self._jwt_service.encode(payload, expires_in=expires_in)
