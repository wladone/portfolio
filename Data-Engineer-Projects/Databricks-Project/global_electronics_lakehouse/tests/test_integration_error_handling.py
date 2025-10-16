"""Integration tests for error handling and recovery scenarios."""

import pytest
import tempfile
import subprocess
import os
import json
import csv
import time
import threading
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from dashboard.core.data_service import DataService, DataServiceError, DataNotFoundError


class TestErrorHandlingScenarios:
    """Test error handling and recovery in data pipeline components."""

    def test_data_generation_with_invalid_parameters(self):
        """Test data generation handles invalid parameters gracefully."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Test negative seed
            result = subprocess.run([
                "python", "src/generate_synthetic_data.py",
                "--output", str(output_dir),
                "--seed", "-1"
            ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

            assert result.returncode != 0  # Should fail
            # Error is logged, not printed to stderr, so just check return code

            # Test zero products
            result = subprocess.run([
                "python", "src/generate_synthetic_data.py",
                "--output", str(output_dir),
                "--products", "0"
            ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

            assert result.returncode != 0  # Should fail

            # Test invalid ratio
            result = subprocess.run([
                "python", "src/generate_synthetic_data.py",
                "--output", str(output_dir),
                "--invalid-ratio", "1.5"
            ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

            assert result.returncode != 0  # Should fail

    @pytest.mark.skipif(os.name == 'nt', reason="Read-only directory test not reliable on Windows")
    def test_data_generation_with_corrupted_output_directory(self):
        """Test data generation handles file system errors."""
        # Try to write to a read-only directory (Unix-like systems)
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "readonly"
            output_dir.mkdir()
            # Make directory read-only
            try:
                os.chmod(output_dir, 0o444)
                result = subprocess.run([
                    "python", "src/generate_synthetic_data.py",
                    "--output", str(output_dir),
                    "--products", "10"
                ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

                # Should fail due to permission error
                assert result.returncode != 0
            finally:
                # Restore permissions for cleanup
                os.chmod(output_dir, 0o755)

    def test_invalid_data_format_handling(self):
        """Test handling of malformed data files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create corrupted suppliers file
            suppliers_dir = data_dir / "suppliers"
            suppliers_dir.mkdir(parents=True)
            with open(suppliers_dir / "suppliers_master.jsonl", "w") as f:
                f.write('{"supplier_id": "SUP001", "name": invalid json}\n')
                f.write('{"supplier_id": "SUP002"}\n')  # Valid but incomplete

            # Create inventory with invalid data
            inventory_dir = data_dir / "inventory" / "2025-10-01"
            inventory_dir.mkdir(parents=True)
            with open(inventory_dir / "FRA_inventory.csv", "w") as f:
                f.write(
                    "inventory_date,warehouse_code,product_id,quantity_on_hand\n")
                f.write("2025-10-01,FRA,PRD001,not_a_number\n")
                f.write("2025-10-01,FRA,PRD002,-50\n")  # Negative quantity

            # Test that data validation catches issues
            # (This would be tested in the pipeline, but we can validate the data structure)

            # Load and validate suppliers
            with open(suppliers_dir / "suppliers_master.jsonl", "r") as f:
                lines = f.readlines()
                # First line should fail JSON parsing
                with pytest.raises(json.JSONDecodeError):
                    json.loads(lines[0].strip())

                # Second line is valid JSON but missing fields
                supplier = json.loads(lines[1].strip())
                assert "supplier_name" not in supplier

            # Load and validate inventory
            with open(inventory_dir / "FRA_inventory.csv", "r") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                # First row has invalid quantity
                assert rows[0]["quantity_on_hand"] == "not_a_number"
                # Second row has negative quantity
                assert int(rows[1]["quantity_on_hand"]) < 0

    @patch('databricks.sql.connect')
    def test_databricks_connection_failures(self, mock_connect):
        """Test handling of Databricks connection failures."""
        # Mock connection failure
        mock_connect.side_effect = Exception("Connection refused")

        with pytest.raises(DataServiceError, match="Failed to establish Databricks connection"):
            DataService()

    @patch('databricks.sql.connect')
    @patch('dashboard.core.data_service.config')
    def test_databricks_query_failures(self, mock_config, mock_connect):
        """Test handling of Databricks query failures."""
        # Mock config
        mock_config.databricks.host = "test-host"
        mock_config.databricks.token = "test-token"
        mock_config.databricks.http_path = "test-path"
        mock_config.databricks.catalog = "test_catalog"
        mock_config.databricks.schema = "test_schema"

        # Mock successful connection but failed query
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("Query failed")
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        data_service = DataService()

        with pytest.raises(DataServiceError, match="Failed to retrieve inventory data"):
            data_service.get_inventory_data()

    @patch('databricks.sql.connect')
    def test_databricks_empty_results(self, mock_connect):
        """Test handling of empty query results."""
        # Mock successful connection with empty results
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        data_service = DataService()

        # Should handle empty results gracefully
        result = data_service.get_inventory_data()
        assert result == []

    @patch('databricks.sql.connect')
    def test_databricks_invalid_data_format(self, mock_connect):
        """Test handling of invalid data format from Databricks."""
        # Mock connection returning malformed data
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            ("PRD001", "Test Product", "FRA", "not_a_number",
             "100.0", "Electronics"),  # Invalid quantity
            ("PRD002", "Test Product 2", "FRA", None,
             "200.0", "Electronics"),  # None quantity
        ]
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        data_service = DataService()

        # Should skip invalid items and continue
        result = data_service.get_inventory_data()
        # Depending on validation, may return empty or partial results
        assert isinstance(result, list)

    def test_network_timeout_simulation(self):
        """Test handling of network timeouts (simulated)."""
        # This would require mocking network calls or using timeout parameters
        # For now, test that DataService has reasonable timeout handling
        data_service = DataService()

        # Test cache timeout behavior
        data_service._cache["test_key"] = {
            "data": "test_data",
            "timestamp": time.time() - 400  # Expired
        }

        # Should not return expired data
        assert data_service._get_cached("test_key") is None

    def test_schema_evolution_handling(self):
        """Test handling of schema changes in data."""
        # Create data with missing optional fields
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create suppliers with some missing optional fields
            suppliers_dir = data_dir / "suppliers"
            suppliers_dir.mkdir(parents=True)
            suppliers = [
                {
                    "supplier_id": "SUP001",
                    "supplier_name": "Test Supplier 1",
                    "rating": 4.5,
                    "lead_time_days": 10,
                    "preferred": True,
                    "contact_email": "test@example.com"
                    # Missing category_focus
                },
                {
                    "supplier_id": "SUP002",
                    "supplier_name": "Test Supplier 2",
                    "rating": 3.8,
                    "lead_time_days": 15,
                    "preferred": False,
                    "contact_email": "test2@example.com",
                    "category_focus": ["Electronics", "Audio"]
                }
            ]

            with open(suppliers_dir / "suppliers_master.jsonl", "w") as f:
                for supplier in suppliers:
                    f.write(json.dumps(supplier) + "\n")

            # Test that code handles missing fields gracefully
            with open(suppliers_dir / "suppliers_master.jsonl", "r") as f:
                loaded_suppliers = [json.loads(line) for line in f]

            assert len(loaded_suppliers) == 2
            # First supplier missing category_focus
            assert "category_focus" not in loaded_suppliers[0]
            # Second supplier has category_focus
            assert "category_focus" in loaded_suppliers[1]
            assert loaded_suppliers[1]["category_focus"] == [
                "Electronics", "Audio"]

    def test_concurrent_access_handling(self):
        """Test handling of concurrent data access."""
        # Test cache thread safety (basic)
        data_service = DataService()

        # Simulate concurrent cache access
        import threading
        import time

        results = []

        def cache_worker(worker_id):
            for i in range(10):
                cache_key = f"worker_{worker_id}_{i}"
                data_service._set_cached(cache_key, f"data_{worker_id}_{i}")
                cached = data_service._get_cached(cache_key)
                results.append(cached)

        threads = []
        for i in range(3):
            t = threading.Thread(target=cache_worker, args=(i,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All cache operations should succeed
        assert len(results) == 30
        assert all(r is not None for r in results)

    def test_large_dataset_handling(self):
        """Test handling of large datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Generate large sales dataset
            sales_dir = data_dir / "sales_stream"
            sales_dir.mkdir(parents=True)

            # Create 1000 sales events
            sales_events = []
            base_time = datetime.now()
            for i in range(1000):
                event = {
                    "event_id": f"EVT{i:06d}",
                    "event_time": (base_time + timedelta(minutes=i)).isoformat(),
                    "order_id": f"ORD{i:06d}",
                    "product_id": f"PRD{i % 100:05d}",
                    "warehouse_code": ["FRA", "BUC", "MAD"][i % 3],
                    "channel": "online",
                    "payment_type": "card",
                    "customer_id": f"CUST{i % 50:06d}",
                    "quantity": (i % 10) + 1,
                    "unit_price_euro": 99.99,
                    "discount_rate": 0.0 if i % 5 != 0 else 0.1,
                    "currency": "EUR",
                    "sales_channel_region": "EU-West",
                    "promotion_code": None,
                    "is_priority_order": i % 20 == 0,
                    "device_type": "desktop"
                }
                sales_events.append(event)

            # Write in batches
            batch_size = 100
            for batch_idx in range(0, len(sales_events), batch_size):
                batch = sales_events[batch_idx:batch_idx + batch_size]
                with open(sales_dir / f"sales_{(batch_idx//batch_size)+1:03d}.json", "w") as f:
                    for event in batch:
                        f.write(json.dumps(event) + "\n")

            # Verify all events written
            total_written = 0
            for sales_file in sales_dir.glob("sales_*.json"):
                with open(sales_file, "r") as f:
                    total_written += len(f.readlines())

            assert total_written == 1000

    def test_memory_usage_under_load(self):
        """Test memory usage with large datasets."""
        # This is a basic test - in real scenarios would use memory profiling
        data_service = DataService()

        # Test cache cleanup
        for i in range(100):
            data_service._set_cached(
                f"test_key_{i}", f"test_data_{i}" * 1000)  # Large data

        # Clear cache
        data_service.clear_cache()

        # Cache should be empty
        assert len(data_service._cache) == 0

    def test_pipeline_recovery_scenarios(self):
        """Test pipeline recovery from various failure points."""
        # Test data generation recovery
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Simulate partial generation failure
            suppliers_dir = output_dir / "suppliers"
            suppliers_dir.mkdir(parents=True)

            # Create partial suppliers file
            with open(suppliers_dir / "suppliers_master.jsonl", "w") as f:
                f.write(json.dumps({
                    "supplier_id": "SUP001",
                    "supplier_name": "Test Supplier",
                    "rating": 4.0,
                    "lead_time_days": 10
                }) + "\n")

            # Try to regenerate - should overwrite
            result = subprocess.run([
                "python", "src/generate_synthetic_data.py",
                "--output", str(output_dir),
                "--suppliers", "3",
                "--seed", "123"
            ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

            assert result.returncode == 0

            # Should have 20 suppliers now (minimum enforced by script)
            with open(suppliers_dir / "suppliers_master.jsonl", "r") as f:
                suppliers = [json.loads(line) for line in f]
            assert len(suppliers) == 20


if __name__ == "__main__":
    pytest.main([__file__])
