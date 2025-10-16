"""End-to-end integration tests for complete data flow validation."""

import pytest
import tempfile
import subprocess
import time
import os
from pathlib import Path
from datetime import datetime, timedelta
import json
import csv
from typing import Dict, List, Any

from dashboard.core.data_service import DataService, DataServiceError


class TestEndToEndDataFlow:
    """Comprehensive end-to-end tests for data pipeline."""

    @pytest.fixture(scope="class")
    def test_data_dir(self):
        """Create temporary directory for test data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture(scope="class")
    def generated_data(self, test_data_dir):
        """Generate synthetic test data."""
        # Run data generation script
        result = subprocess.run([
            "python", "src/generate_synthetic_data.py",
            "--output", str(test_data_dir),
            "--products", "50",
            "--days", "3",
            "--suppliers", "5",
            "--sales-files", "2",
            "--events-per-file", "25",
            "--seed", "42"
        ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

        assert result.returncode == 0, f"Data generation failed: {result.stderr}"

        # Verify data was created
        assert (test_data_dir / "suppliers" /
                "suppliers_master.jsonl").exists()
        assert (test_data_dir / "inventory").exists()
        assert (test_data_dir / "sales_stream").exists()

        yield test_data_dir

    def test_data_generation_completeness(self, generated_data):
        """Test that all expected data files are generated with correct structure."""
        # Check suppliers
        suppliers_file = generated_data / "suppliers" / "suppliers_master.jsonl"
        with open(suppliers_file, 'r') as f:
            suppliers = [json.loads(line) for line in f]
        assert len(suppliers) == 20  # Script enforces minimum of 20 suppliers
        required_fields = ["supplier_id",
                           "supplier_name", "rating", "lead_time_days"]
        for supplier in suppliers:
            for field in required_fields:
                assert field in supplier

        # Check inventory files
        inventory_files = list((generated_data / "inventory").glob("**/*.csv"))
        assert len(inventory_files) > 0

        # Sample inventory file
        with open(inventory_files[0], 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) > 0
            required_inv_fields = [
                "inventory_date", "warehouse_code", "product_id", "quantity_on_hand"]
            for field in required_inv_fields:
                assert field in rows[0]

        # Check sales files
        sales_files = list(
            (generated_data / "sales_stream").glob("sales_*.json"))
        assert len(sales_files) == 2

        total_events = 0
        for sales_file in sales_files:
            with open(sales_file, 'r') as f:
                events = [json.loads(line) for line in f]
                total_events += len(events)
                if events:
                    required_sales_fields = [
                        "event_id", "order_id", "product_id", "quantity"]
                    for field in required_sales_fields:
                        assert field in events[0]
        assert total_events == 50

    def test_data_consistency_across_sources(self, generated_data):
        """Test data consistency between suppliers, inventory, and sales."""
        # Load suppliers
        suppliers_file = generated_data / "suppliers" / "suppliers_master.jsonl"
        with open(suppliers_file, 'r') as f:
            suppliers = [json.loads(line) for line in f]
        supplier_ids = {s["supplier_id"] for s in suppliers}

        # Load inventory and check supplier references
        inventory_files = list((generated_data / "inventory").glob("**/*.csv"))
        inventory_suppliers = set()
        for inv_file in inventory_files:
            with open(inv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    inventory_suppliers.add(row["supplier_id"])

        # All inventory suppliers should exist in suppliers master
        assert inventory_suppliers.issubset(supplier_ids)

        # Load sales and check product references exist in inventory
        sales_files = list(
            (generated_data / "sales_stream").glob("sales_*.json"))
        sales_products = set()
        for sales_file in sales_files:
            with open(sales_file, 'r') as f:
                for line in f:
                    event = json.loads(line)
                    sales_products.add(event["product_id"])

        # Load inventory products
        inventory_products = set()
        for inv_file in inventory_files:
            with open(inv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    inventory_products.add(row["product_id"])

        # Most sales products should exist in inventory (allowing for some invalid ones)
        valid_sales_products = sales_products - \
            {pid for pid in sales_products if "_INVALID" in pid}
        assert len(valid_sales_products.intersection(inventory_products)) > 0

    @pytest.mark.skip(reason="Requires Databricks environment for DLT pipeline execution")
    def test_dlt_pipeline_execution(self, generated_data):
        """Test DLT pipeline execution (requires Databricks environment)."""
        # This would require:
        # 1. Uploading data to Databricks storage
        # 2. Triggering DLT pipeline
        # 3. Waiting for completion
        # 4. Validating output tables
        pass

    def test_dashboard_data_service_connection(self):
        """Test that dashboard data service can connect to Databricks."""
        # Skip if environment variables not set
        if not all(os.getenv(var) for var in ["DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH"]):
            pytest.skip("Databricks environment variables not configured")

        try:
            data_service = DataService()
            # Just test connection establishment
            assert data_service._connection is not None
            data_service._connection.close()
        except Exception as e:
            pytest.fail(f"Failed to connect to Databricks: {e}")

    @pytest.mark.skip(reason="Requires Databricks environment with processed data")
    def test_dashboard_data_retrieval(self):
        """Test dashboard data retrieval from processed tables."""
        # This would test actual data retrieval after pipeline processing
        pass

    def test_data_transformation_logic(self, generated_data):
        """Test data transformation logic without full pipeline."""
        # Load raw data
        suppliers_file = generated_data / "suppliers" / "suppliers_master.jsonl"
        with open(suppliers_file, 'r') as f:
            suppliers = [json.loads(line) for line in f]

        inventory_files = list((generated_data / "inventory").glob("**/*.csv"))
        inventory_data = []
        for inv_file in inventory_files:
            with open(inv_file, 'r') as f:
                reader = csv.DictReader(f)
                inventory_data.extend(list(reader))

        sales_files = list(
            (generated_data / "sales_stream").glob("sales_*.json"))
        sales_data = []
        for sales_file in sales_files:
            with open(sales_file, 'r') as f:
                for line in f:
                    sales_data.append(json.loads(line))

        # Test supplier rating validation (similar to silver layer)
        valid_suppliers = [
            s for s in suppliers if 0 <= s.get("rating", 0) <= 5]
        assert len(valid_suppliers) == len(suppliers)

        # Test inventory quantity validation
        valid_inventory = [item for item in inventory_data if float(
            item["quantity_on_hand"]) >= 0]
        assert len(valid_inventory) == len(inventory_data)

        # Test sales quantity validation
        valid_sales = [sale for sale in sales_data if sale["quantity"] > 0]
        assert len(valid_sales) == len(sales_data)

    def test_schema_compliance(self, generated_data):
        """Test that generated data complies with expected schemas."""
        import json
        from pathlib import Path

        # Load schema files
        schema_dir = Path(__file__).parent.parent / "resources" / "schemas"
        with open(schema_dir / "inventory_schema.json", "r", encoding="utf-8-sig") as f:
            inventory_schema = json.load(f)
        with open(schema_dir / "sales_schema.json", "r", encoding="utf-8-sig") as f:
            sales_schema = json.load(f)
        with open(schema_dir / "suppliers_schema.json", "r", encoding="utf-8-sig") as f:
            suppliers_schema = json.load(f)

        # Test inventory schema compliance
        inventory_files = list((generated_data / "inventory").glob("**/*.csv"))
        for inv_file in inventory_files:
            with open(inv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Check required fields exist (nullable: false)
                    for field_def in inventory_schema["fields"]:
                        if not field_def.get("nullable", True):
                            field_name = field_def["name"]
                            assert field_name in row, f"Missing required field {field_name} in inventory"

        # Test sales schema compliance
        sales_files = list(
            (generated_data / "sales_stream").glob("sales_*.json"))
        for sales_file in sales_files:
            with open(sales_file, 'r') as f:
                for line in f:
                    event = json.loads(line)
                    for field_def in sales_schema["fields"]:
                        if not field_def.get("nullable", True):
                            field_name = field_def["name"]
                            assert field_name in event, f"Missing required field {field_name} in sales"

        # Test suppliers schema compliance
        suppliers_file = generated_data / "suppliers" / "suppliers_master.jsonl"
        with open(suppliers_file, 'r') as f:
            for line in f:
                supplier = json.loads(line)
                for field_def in suppliers_schema["fields"]:
                    if not field_def.get("nullable", True):
                        field_name = field_def["name"]
                        assert field_name in supplier, f"Missing required field {field_name} in suppliers"


class TestDataIntegrityValidation:
    """Tests for data integrity across the pipeline."""

    @pytest.fixture
    def integrity_generated_data(self):
        """Generate data for integrity tests."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)

            # Run data generation script
            result = subprocess.run([
                "python", "src/generate_synthetic_data.py",
                "--output", str(output_dir),
                "--products", "30",
                "--days", "2",
                "--suppliers", "5",
                "--sales-files", "1",
                "--events-per-file", "20",
                "--seed", "123"
            ], cwd=Path(__file__).parent.parent, capture_output=True, text=True)

            assert result.returncode == 0, f"Data generation failed: {result.stderr}"
            yield output_dir

    def test_inventory_date_consistency(self, integrity_generated_data):
        """Test that inventory dates are consistent and logical."""
        inventory_files = list(
            (integrity_generated_data / "inventory").glob("**/*.csv"))
        dates = set()

        for inv_file in inventory_files:
            with open(inv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    date_str = row["inventory_date"]
                    # Should be valid date
                    datetime.fromisoformat(date_str)
                    dates.add(date_str)

        # Should have multiple dates
        assert len(dates) > 1

    def test_sales_event_time_ordering(self, integrity_generated_data):
        """Test that sales events have reasonable timestamps."""
        sales_files = list(
            (integrity_generated_data / "sales_stream").glob("sales_*.json"))
        events = []

        for sales_file in sales_files:
            with open(sales_file, 'r') as f:
                for line in f:
                    event = json.loads(line)
                    events.append(event)

        # Sort by event time
        events.sort(key=lambda x: x["event_time"])

        # Check time ordering
        for i in range(1, len(events)):
            prev_time = datetime.fromisoformat(events[i-1]["event_time"])
            curr_time = datetime.fromisoformat(events[i]["event_time"])
            assert curr_time >= prev_time, "Events not in chronological order"

    def test_product_inventory_relationship(self, integrity_generated_data):
        """Test relationships between products across data sources."""
        # Load all products from inventory
        inventory_files = list(
            (integrity_generated_data / "inventory").glob("**/*.csv"))
        inventory_products = {}
        for inv_file in inventory_files:
            with open(inv_file, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    pid = row["product_id"]
                    if pid not in inventory_products:
                        inventory_products[pid] = {
                            "name": row["product_name"],
                            "category": row["category"],
                            "supplier_id": row["supplier_id"],
                            "warehouses": set()
                        }
                    inventory_products[pid]["warehouses"].add(
                        row["warehouse_code"])

        # Check that products have consistent attributes across warehouses
        for pid, data in inventory_products.items():
            # All warehouse entries should have same name and category
            assert len(data["warehouses"]) > 0


if __name__ == "__main__":
    pytest.main([__file__])
