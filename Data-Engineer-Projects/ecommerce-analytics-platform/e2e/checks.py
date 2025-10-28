"""HTTP checks for the end-to-end pipeline."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import httpx


@dataclass
class CheckOutcome:
    ok: bool
    detail: str
    data: Any = None
    latency: float = 0.0


def _request_json(url: str, timeout: float) -> tuple[Mapping[str, Any], float]:
    with httpx.Client(timeout=timeout) as client:
        start = time.perf_counter()
        response = client.get(url)
        latency = time.perf_counter() - start
        response.raise_for_status()
        return response.json(), latency


def wait_for_api_health(base_url: str, timeout_s: int) -> CheckOutcome:
    deadline = time.time() + timeout_s
    last_error = ""
    last_latency = 0.0
    while time.time() < deadline:
        try:
            payload, latency = _request_json(f"{base_url}/health", timeout=5.0)
            last_latency = latency
            if payload.get("status") == "ok":
                return CheckOutcome(True, "health endpoint OK", payload, latency)
            last_error = f"unexpected payload: {payload}"
        except Exception as exc:  # pragma: no cover - retry logic
            last_error = str(exc)
        time.sleep(1)
    return CheckOutcome(
        False,
        f"health check failed after {timeout_s}s: {last_error}",
        latency=last_latency,
    )


def check_readyz(base_url: str, timeout: float = 10.0) -> CheckOutcome:
    try:
        payload, latency = _request_json(f"{base_url}/readyz", timeout=timeout)
    except Exception as exc:
        return CheckOutcome(False, f"readyz request failed: {exc}")

    ready = bool(payload.get("ready", False))
    detail = "ready" if ready else "not ready"
    return CheckOutcome(ready, detail, payload, latency)


def check_sales_summary(
    base_url: str,
    dt_from: str,
    dt_to: str,
    *,
    granularity: str = "day",
    channel: str | None = None,
    timeout: float = 10.0,
) -> CheckOutcome:
    params = {"start": dt_from, "end": dt_to, "granularity": granularity}
    if channel:
        params["channel"] = channel
    try:
        with httpx.Client(timeout=timeout) as client:
            start = time.perf_counter()
            response = client.get(f"{base_url}/api/v1/sales/summary", params=params)
            latency = time.perf_counter() - start
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        return CheckOutcome(False, f"sales summary request failed: {exc}")

    if "data" not in payload or not isinstance(payload["data"], list):
        return CheckOutcome(False, "summary payload missing data list", payload)
    return CheckOutcome(True, "sales summary OK", payload, latency)


def check_top_products(
    base_url: str,
    dt_from: str,
    dt_to: str,
    *,
    metric: str = "net",
    limit: int = 10,
    timeout: float = 10.0,
) -> CheckOutcome:
    params = {"start": dt_from, "end": dt_to, "metric": metric, "limit": limit}
    try:
        with httpx.Client(timeout=timeout) as client:
            start = time.perf_counter()
            response = client.get(
                f"{base_url}/api/v1/sales/top-products", params=params
            )
            latency = time.perf_counter() - start
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        return CheckOutcome(False, f"top-products request failed: {exc}")

    items = payload.get("data")
    if not isinstance(items, list) or not items:
        return CheckOutcome(False, "no top products returned", payload)
    if metric not in items[0]:
        return CheckOutcome(False, f"metric {metric} missing in response", payload)
    values = [item.get(metric, 0) for item in items]
    if values != sorted(values, reverse=True):
        return CheckOutcome(False, "metric values not sorted desc", payload)
    return CheckOutcome(True, "top products OK", payload, latency)


def check_user_recs(
    base_url: str, user_id: int, k: int, timeout: float = 10.0
) -> CheckOutcome:
    try:
        with httpx.Client(timeout=timeout) as client:
            start = time.perf_counter()
            response = client.get(
                f"{base_url}/api/v1/recs/user/{user_id}", params={"k": k}
            )
            latency = time.perf_counter() - start
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        return CheckOutcome(False, f"user recs request failed: {exc}")

    items = payload.get("items")
    reason = payload.get("reason")
    if not isinstance(items, list):
        return CheckOutcome(False, "items missing in recs response", payload)
    if reason not in {"als", "popular"}:
        return CheckOutcome(False, "unexpected recommendation reason", payload)
    return CheckOutcome(True, "user recs OK", payload, latency)


def check_similar(
    base_url: str, sku: str, k: int, timeout: float = 10.0
) -> CheckOutcome:
    try:
        with httpx.Client(timeout=timeout) as client:
            start = time.perf_counter()
            response = client.get(
                f"{base_url}/api/v1/recs/similar-products/{sku}", params={"k": k}
            )
            latency = time.perf_counter() - start
            response.raise_for_status()
            payload = response.json()
    except Exception as exc:
        return CheckOutcome(False, f"similar products request failed: {exc}")

    if not isinstance(payload.get("items"), list):
        return CheckOutcome(False, "similar products payload invalid", payload)
    return CheckOutcome(True, "similar products OK", payload, latency)


def check_metrics_expose(base_url: str, timeout: float = 10.0) -> CheckOutcome:
    try:
        with httpx.Client(timeout=timeout) as client:
            start = time.perf_counter()
            response = client.get(f"{base_url}/metrics")
            latency = time.perf_counter() - start
            response.raise_for_status()
            body = response.text
    except Exception as exc:
        return CheckOutcome(False, f"metrics endpoint failed: {exc}")

    required_metrics = ["ecom_requests_total", "ecom_request_latency_ms_bucket"]
    if not all(metric in body for metric in required_metrics):
        return CheckOutcome(False, "required metrics missing", body)
    return CheckOutcome(True, "metrics endpoint OK", body, latency)
