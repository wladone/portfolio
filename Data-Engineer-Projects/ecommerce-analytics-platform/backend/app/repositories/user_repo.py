"""Repository for managing application users."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.app.models.app_user import AppUser


class UserRepository:
    """Data access layer for `AppUser` records."""

    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_username(self, username: str) -> AppUser | None:
        """Return a user matching the given username."""
        stmt = select(AppUser).where(AppUser.username == username)
        return self._session.execute(stmt).scalar_one_or_none()

    def create(self, username: str, password_hash: str, role: str) -> AppUser:
        """Persist a new user."""
        user = AppUser(
            username=username,
            password_hash=password_hash,
            role=role,
        )
        self._session.add(user)
        try:
            self._session.flush()
        except IntegrityError as exc:
            self._session.rollback()
            raise exc
        return user

    def save(self, user: AppUser) -> AppUser:
        """Flush the current session ensuring latest state."""
        self._session.add(user)
        self._session.flush()
        return user
