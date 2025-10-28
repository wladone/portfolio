"""Authentication and RBAC tests for the API."""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete

from backend.app.config import get_settings
from backend.app.core.db import SessionLocal
from backend.app.core.security import JWTService
from backend.app.main import app
from backend.app.models.app_user import AppUser
from backend.app.services.recs_service import RecsService
from backend.app.services.user_service import UserService


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Provide a FastAPI test client."""
    with TestClient(app) as test_client:
        yield test_client


def _create_user(username: str, password: str, role: str) -> None:
    """Helper to create a user via the service layer."""
    settings = get_settings()
    session = SessionLocal()
    try:
        session.execute(delete(AppUser).where(AppUser.username == username))
        session.commit()
        service = UserService(
            session, JWTService(settings.jwt_secret, settings.jwt_algorithm)
        )
        service.create_user(username, password, role)
    finally:
        session.close()


def _delete_user(username: str) -> None:
    """Cleanup helper to remove a user by username."""
    session = SessionLocal()
    try:
        session.execute(delete(AppUser).where(AppUser.username == username))
        session.commit()
    finally:
        session.close()


def _login(client: TestClient, username: str, password: str) -> str:
    """Login helper returning the bearer token."""
    response = client.post(
        "/auth/login",
        data={"username": username, "password": password},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 200, response.text
    body = response.json()
    return body["access_token"]


def test_login_and_call_sales_with_bearer(client: TestClient) -> None:
    """User with role analyst can login and access sales summary."""
    settings = get_settings()
    original_require = settings.auth_require_auth
    original_dev = settings.auth_dev_users_enabled
    settings.auth_require_auth = True
    settings.auth_dev_users_enabled = True

    username = f"analyst_{uuid4().hex[:8]}"
    try:
        _create_user(username, "secret", "analyst")
        token = _login(client, username, "secret")
        response = client.get(
            "/api/v1/sales/summary",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 200
    finally:
        settings.auth_require_auth = original_require
        settings.auth_dev_users_enabled = original_dev
        _delete_user(username)


def test_forbidden_without_role(client: TestClient) -> None:
    """User with role app cannot access admin endpoint when auth is enforced."""
    settings = get_settings()
    original_require = settings.auth_require_auth
    original_dev = settings.auth_dev_users_enabled
    settings.auth_require_auth = True
    settings.auth_dev_users_enabled = True

    username = f"app_{uuid4().hex[:8]}"
    try:
        _create_user(username, "secret", "app")
        token = _login(client, username, "secret")
        response = client.get(
            "/admin/ping",
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 403
    finally:
        settings.auth_require_auth = original_require
        settings.auth_dev_users_enabled = original_dev
        _delete_user(username)


def test_auth_bypass_in_dev(client: TestClient) -> None:
    """When auth is disabled the analytics routes remain accessible without tokens."""
    settings = get_settings()
    original_require = settings.auth_require_auth
    settings.auth_require_auth = False
    try:
        response = client.get("/api/v1/sales/summary")
        assert response.status_code == 200
    finally:
        settings.auth_require_auth = original_require


def test_refresh_requires_admin(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recommendation refresh requires admin role even when endpoint is enabled."""
    settings = get_settings()
    original_require = settings.auth_require_auth
    original_dev = settings.auth_dev_users_enabled
    original_refresh = settings.recs_allow_refresh_endpoint
    settings.auth_require_auth = True
    settings.auth_dev_users_enabled = True
    settings.recs_allow_refresh_endpoint = True

    async def _fake_refresh(self: RecsService) -> tuple[None, str]:
        return None, "mock-model"

    monkeypatch.setattr(RecsService, "refresh", _fake_refresh)

    analyst_username = f"analyst_{uuid4().hex[:8]}"
    admin_username = f"admin_{uuid4().hex[:8]}"

    try:
        _create_user(analyst_username, "secret", "analyst")
        _create_user(admin_username, "secret", "admin")

        analyst_token = _login(client, analyst_username, "secret")
        admin_token = _login(client, admin_username, "secret")

        forbidden = client.post(
            "/api/v1/recs/_refresh",
            headers={"Authorization": f"Bearer {analyst_token}"},
        )
        assert forbidden.status_code == 403

        allowed = client.post(
            "/api/v1/recs/_refresh",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert allowed.status_code == 200
        assert allowed.json()["status"] == "reloaded"
    finally:
        settings.auth_require_auth = original_require
        settings.auth_dev_users_enabled = original_dev
        settings.recs_allow_refresh_endpoint = original_refresh
        _delete_user(analyst_username)
        _delete_user(admin_username)
