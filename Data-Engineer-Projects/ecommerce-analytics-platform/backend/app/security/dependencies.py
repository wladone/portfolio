"""FastAPI dependencies for authentication and role-based access control."""

from __future__ import annotations

from collections.abc import Callable
from typing import cast

from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jwt import InvalidTokenError
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.core.db import get_db
from backend.app.core.security import JWTService
from backend.app.models.app_user import AppUser
from backend.app.repositories.user_repo import UserRepository

oauth2_scheme = OAuth2PasswordBearer(
    tokenUrl="/auth/login",
    scheme_name="JWT",
    auto_error=False,
)


async def get_optional_token() -> str | None:
    """Retrieve the bearer token if presented."""
    token = await oauth2_scheme()
    if not token:
        return None
    return cast(str, token)


def get_jwt_service() -> JWTService:
    """Provide a configured JWT service."""
    settings = get_settings()
    return JWTService(settings.jwt_secret, settings.jwt_algorithm)


async def get_current_user(
    token: str | None = Depends(get_optional_token),
    session: Session = Depends(get_db),
) -> AppUser | None:
    """Return the authenticated user based on the bearer token."""
    settings = get_settings()
    if not settings.auth_require_auth:
        return None

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication credentials were not provided.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    jwt_service = get_jwt_service()
    try:
        payload = jwt_service.decode(token)
    except InvalidTokenError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    username = payload.get("sub")
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    repo = UserRepository(session)
    user = repo.get_by_username(username)
    if user is None or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return user


def require_roles(*roles: str) -> Callable[[AppUser | None], AppUser | None]:
    """Return a dependency enforcing that the current user has one of the roles."""

    def dependency(
        current_user: AppUser | None = Depends(get_current_user),
    ) -> AppUser | None:
        settings = get_settings()
        if not settings.auth_require_auth:
            return None

        if current_user is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Authentication required.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if roles and current_user.role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Insufficient permissions.",
            )

        return current_user

    return dependency
