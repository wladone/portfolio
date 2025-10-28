"""Authentication endpoints for issuing access tokens."""

from __future__ import annotations

from datetime import timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from backend.app.config import get_settings
from backend.app.core.db import get_db
from backend.app.core.security import JWTService
from backend.app.schemas.auth import TokenResponse
from backend.app.services.user_service import UserService

router = APIRouter(prefix="/auth", tags=["auth"])


def get_user_service(session: Annotated[Session, Depends(get_db)]) -> UserService:
    """Provide a user service instance for dependency injection."""
    settings = get_settings()
    jwt_service = JWTService(settings.jwt_secret, settings.jwt_algorithm)
    return UserService(session, jwt_service)


@router.post(
    "/login",
    response_model=TokenResponse,
    status_code=status.HTTP_200_OK,
    summary="Issue an access token",
)
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
    user_service: Annotated[UserService, Depends(get_user_service)],
) -> TokenResponse:
    """Authenticate a user and issue an access token."""
    settings = get_settings()
    if not settings.auth_dev_users_enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Endpoint not available.",
        )

    user = user_service.authenticate(form_data.username, form_data.password)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expires_seconds = settings.auth_access_token_expires_seconds
    token = user_service.issue_access_token(
        user, expires_in=timedelta(seconds=expires_seconds)
    )

    return TokenResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_seconds,
    )
