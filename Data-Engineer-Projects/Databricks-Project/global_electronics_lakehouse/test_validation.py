#!/usr/bin/env python3
"""Test script for config validation and retry logic."""

import logging
import time
from unittest.mock import MagicMock, patch

# Setup logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Mock spark for testing
mock_spark = MagicMock()


def test_validate_config():
    """Test config validation with good and bad configs."""
    print("Testing config validation...")

    # Mock good configs
    def good_config(key):
        configs = {
            "pipelines.electronics.source_inventory": "/mnt/landing/electronics/inventory",
            "pipelines.electronics.source_suppliers": "/mnt/landing/electronics/suppliers",
            "pipelines.electronics.kafka_bootstrap": "localhost:9092",
            "pipelines.electronics.kafka_topic": "global_electronics_sales"
        }
        return configs.get(key, "")

    mock_spark.conf.get.side_effect = good_config

    # Import after mocking
    with patch('src.dlt.electronics_dlt.spark', mock_spark), patch('src.dlt.electronics_dlt.dlt', MagicMock()):
        from src.dlt.electronics_dlt import validate_config
        try:
            validate_config()
            print("✓ Config validation passed with good configs")
        except Exception as e:
            print(f"✗ Config validation failed unexpectedly: {e}")

    # Mock bad configs
    def bad_config(key):
        configs = {
            "pipelines.electronics.source_inventory": "",  # empty
            "pipelines.electronics.source_suppliers": "/mnt/landing/electronics/suppliers",
            "pipelines.electronics.kafka_bootstrap": "localhost:9092",
            "pipelines.electronics.kafka_topic": "global_electronics_sales"
        }
        return configs.get(key, "")

    mock_spark.conf.get.side_effect = bad_config

    with patch('src.dlt.electronics_dlt.spark', mock_spark), patch('src.dlt.electronics_dlt.dlt', MagicMock()):
        try:
            validate_config()
            print("✗ Config validation should have failed with bad configs")
        except ValueError as e:
            print(f"✓ Config validation correctly failed: {e}")
        except Exception as e:
            print(f"✗ Unexpected error: {e}")


def test_retry_decorator():
    """Test retry decorator with simulated failures."""
    print("\nTesting retry decorator...")

    with patch('src.dlt.electronics_dlt.dlt', MagicMock()):
        from src.dlt.electronics_dlt import retry_on_failure

    call_count = 0

    # short backoff for test
    @retry_on_failure(max_retries=2, backoff_factor=0.1)
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
            f"✓ Retry succeeded after {call_count} attempts in {end_time - start_time:.2f}s")
        print(f"  Result: {result}")
    except Exception as e:
        print(f"✗ Retry failed: {e}")


def test_alert_function():
    """Test alert function."""
    print("\nTesting alert function...")

    with patch('src.dlt.electronics_dlt.dlt', MagicMock()):
        from src.dlt.electronics_dlt import alert_on_expect_violation

    # This will log alerts
    alert_on_expect_violation("test_table", "test_expectation", 5)
    print("✓ Alert logged for expectation violation")


if __name__ == "__main__":
    test_validate_config()
    test_retry_decorator()
    test_alert_function()
    print("\nAll tests completed.")
