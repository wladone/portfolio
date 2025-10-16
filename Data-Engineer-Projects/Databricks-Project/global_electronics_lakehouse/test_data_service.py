#!/usr/bin/env python3
"""Test script for data service retry and alerting."""

import logging
import time
from unittest.mock import MagicMock, patch

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def test_retry_decorator():
    """Test retry decorator with simulated failures."""
    print("Testing retry decorator...")

    from dashboard.core.data_service import retry_with_backoff

    call_count = 0

    # short backoff for test
    @retry_with_backoff(max_retries=2, backoff_factor=0.1)
    def failing_function():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception(f"Simulated failure #{call_count}")
        return "success"

    start_time = time.time()
    try:
        result = failing_function()
        end_time = time.time()
        print(
            f"[OK] Retry succeeded after {call_count} attempts in {end_time - start_time:.2f}s")
        print(f"  Result: {result}")
    except Exception as e:
        print(f"[FAIL] Retry failed: {e}")


def test_send_alert():
    """Test send_alert function."""
    print("\nTesting send_alert function...")

    from dashboard.core.data_service import send_alert

    # This will log alerts
    send_alert("Test alert message", "WARNING")
    print("[OK] Alert sent (logged)")


if __name__ == "__main__":
    test_retry_decorator()
    test_send_alert()
    print("\nData service tests completed.")
