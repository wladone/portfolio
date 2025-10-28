"""Smoke tests for the health endpoint."""

from fastapi.testclient import TestClient

from backend.app.main import app


def test_health_endpoint_returns_ok() -> None:
    """Ensure `/health` responds with 200 and payload."""
    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert "X-Correlation-ID" in response.headers
