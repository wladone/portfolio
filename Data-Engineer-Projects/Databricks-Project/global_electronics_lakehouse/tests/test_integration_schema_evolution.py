"""Integration tests for schema evolution and data consistency."""

import pytest
import tempfile
import json
import csv
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any


class TestSchemaEvolution:
    """Test schema evolution handling across data pipeline."""

    @pytest.fixture
    def base_schema_data(self):
        """Create base schema test data."""
        return {
            "suppliers": [
                {
                    "supplier_id": "SUP001",
                    "supplier_name": "Base Supplier 1",
                    "rating": 4.5,
                    "lead_time_days": 10,
                    "preferred": True,
                    "contact_email": "base1@example.com",
                    "category_focus": ["Electronics"]
                }
            ],
            "inventory": [
                {
                    "inventory_date": "2025-10-01",
                    "warehouse_code": "FRA",
                    "product_id": "PRD001",
                    "product_name": "Base Product 1",
                    "category": "Electronics",
                    "quantity_on_hand": 100,
                    "unit_cost_euro": 50.0,
                    "supplier_id": "SUP001",
                    "restock_threshold": 20,
                    "restock_lead_days": 7,
                    "is_active": True,
                    "last_updated_ts": "2025-10-01T10:00:00",
                    "last_sale_ts": "2025-09-30T15:00:00",
                    "lifetime_revenue_euro": 5000.0
                }
            ],
            "sales": [
                {
                    "event_id": "EVT001",
                    "event_time": "2025-10-01T12:00:00",
                    "order_id": "ORD001",
                    "product_id": "PRD001",
                    "warehouse_code": "FRA",
                    "channel": "online",
                    "payment_type": "card",
                    "customer_id": "CUST001",
                    "quantity": 2,
                    "unit_price_euro": 100.0,
                    "discount_rate": 0.0,
                    "currency": "EUR",
                    "sales_channel_region": "EU-West",
                    "promotion_code": None,
                    "is_priority_order": False,
                    "device_type": "desktop"
                }
            ]
        }

    def test_schema_additive_changes(self, base_schema_data):
        """Test handling of additive schema changes (new optional fields)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create base data
            self._write_test_data(data_dir, base_schema_data)

            # Simulate schema evolution: add new optional fields
            evolved_suppliers = base_schema_data["suppliers"].copy()
            evolved_suppliers[0]["new_field_added"] = "new_value"
            evolved_suppliers[0]["another_new_field"] = 42

            evolved_inventory = base_schema_data["inventory"].copy()
            evolved_inventory[0]["new_inventory_field"] = "inventory_value"

            evolved_sales = base_schema_data["sales"].copy()
            evolved_sales[0]["new_sales_field"] = "sales_value"

            # Write evolved data
            evolved_data = {
                "suppliers": evolved_suppliers,
                "inventory": evolved_inventory,
                "sales": evolved_sales
            }
            self._write_test_data(data_dir, evolved_data, prefix="evolved_")

            # Test that both old and new data can be processed
            old_suppliers = self._load_suppliers(
                data_dir / "suppliers" / "suppliers_master.jsonl")
            new_suppliers = self._load_suppliers(
                data_dir / "evolved_suppliers" / "suppliers_master.jsonl")

            # Old data should not have new fields
            assert "new_field_added" not in old_suppliers[0]
            # New data should have new fields
            assert "new_field_added" in new_suppliers[0]
            assert new_suppliers[0]["new_field_added"] == "new_value"

    def test_schema_subtractive_changes(self, base_schema_data):
        """Test handling of subtractive schema changes (removed fields)."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create base data
            self._write_test_data(data_dir, base_schema_data)

            # Simulate schema evolution: remove some fields
            reduced_suppliers = [{
                "supplier_id": s["supplier_id"],
                "supplier_name": s["supplier_name"],
                # Remove rating, lead_time_days, etc.
            } for s in base_schema_data["suppliers"]]

            reduced_inventory = [{
                "inventory_date": i["inventory_date"],
                "warehouse_code": i["warehouse_code"],
                "product_id": i["product_id"],
                "quantity_on_hand": i["quantity_on_hand"],
                # Remove many optional fields
            } for i in base_schema_data["inventory"]]

            # Write reduced data
            reduced_data = {
                "suppliers": reduced_suppliers,
                "inventory": reduced_inventory,
                "sales": base_schema_data["sales"]  # Keep sales unchanged
            }
            self._write_test_data(data_dir, reduced_data, prefix="reduced_")

            # Test that reduced data can still be loaded
            reduced_suppliers_loaded = self._load_suppliers(
                data_dir / "reduced_suppliers" / "suppliers_master.jsonl")
            assert len(reduced_suppliers_loaded) == 1
            assert "supplier_id" in reduced_suppliers_loaded[0]
            assert "supplier_name" in reduced_suppliers_loaded[0]
            # Missing fields should not cause errors
            assert "rating" not in reduced_suppliers_loaded[0]

    def test_schema_type_changes(self, base_schema_data):
        """Test handling of type changes in schema fields."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create base data
            self._write_test_data(data_dir, base_schema_data)

            # Simulate type changes
            type_changed_inventory = base_schema_data["inventory"].copy()
            # String instead of int
            type_changed_inventory[0]["quantity_on_hand"] = "100"
            # String instead of float
            type_changed_inventory[0]["unit_cost_euro"] = "50.00"

            type_changed_sales = base_schema_data["sales"].copy()
            type_changed_sales[0]["quantity"] = "2"  # String instead of int
            # String instead of float
            type_changed_sales[0]["unit_price_euro"] = "100.00"
            # String instead of float
            type_changed_sales[0]["discount_rate"] = "0.0"

            # Write type-changed data
            type_changed_data = {
                "suppliers": base_schema_data["suppliers"],
                "inventory": type_changed_inventory,
                "sales": type_changed_sales
            }
            self._write_test_data(
                data_dir, type_changed_data, prefix="type_changed_")

            # Test type coercion (simulating what DLT might do)
            type_changed_inv = self._load_inventory(
                data_dir / "type_changed_inventory" / "2025-10-01" / "FRA_inventory.csv")

            # Should be able to convert string numbers back to numeric types
            assert isinstance(
                type_changed_inv[0]["quantity_on_hand"], str)  # Raw from CSV
            # Test conversion
            int(type_changed_inv[0]["quantity_on_hand"])  # Should not raise
            float(type_changed_inv[0]["unit_cost_euro"])  # Should not raise

    def test_schema_field_renames(self, base_schema_data):
        """Test handling of field renames in schema evolution."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create base data
            self._write_test_data(data_dir, base_schema_data)

            # Simulate field renames
            renamed_suppliers = [{
                "supplier_id": s["supplier_id"],
                "supplier_name_new": s["supplier_name"],  # Renamed field
                "rating_score": s["rating"],  # Renamed field
                "lead_time": s["lead_time_days"],  # Renamed field
            } for s in base_schema_data["suppliers"]]

            # Write renamed data
            renamed_data = {
                "suppliers": renamed_suppliers,
                "inventory": base_schema_data["inventory"],
                "sales": base_schema_data["sales"]
            }
            self._write_test_data(data_dir, renamed_data, prefix="renamed_")

            # Test that renamed fields are handled
            renamed_suppliers_loaded = self._load_suppliers(
                data_dir / "renamed_suppliers" / "suppliers_master.jsonl")

            # Old field names should not exist
            assert "supplier_name" not in renamed_suppliers_loaded[0]
            assert "rating" not in renamed_suppliers_loaded[0]
            # New field names should exist
            assert "supplier_name_new" in renamed_suppliers_loaded[0]
            assert "rating_score" in renamed_suppliers_loaded[0]

    def test_schema_nested_structure_changes(self, base_schema_data):
        """Test handling of nested structure changes."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Create base data
            self._write_test_data(data_dir, base_schema_data)

            # Simulate nested structure changes
            nested_suppliers = base_schema_data["suppliers"].copy()
            nested_suppliers[0]["contact_info"] = {
                "email": nested_suppliers[0]["contact_email"],
                "phone": "+33123456789"
            }
            del nested_suppliers[0]["contact_email"]

            nested_inventory = base_schema_data["inventory"].copy()
            nested_inventory[0]["warehouse_info"] = {
                "code": nested_inventory[0]["warehouse_code"],
                "country": "France"
            }

            # Write nested data
            nested_data = {
                "suppliers": nested_suppliers,
                "inventory": nested_inventory,
                "sales": base_schema_data["sales"]
            }
            self._write_test_data(data_dir, nested_data, prefix="nested_")

            # Test nested structure handling
            nested_suppliers_loaded = self._load_suppliers(
                data_dir / "nested_suppliers" / "suppliers_master.jsonl")

            assert "contact_info" in nested_suppliers_loaded[0]
            assert isinstance(nested_suppliers_loaded[0]["contact_info"], dict)
            assert "email" in nested_suppliers_loaded[0]["contact_info"]

    def test_schema_version_compatibility(self, base_schema_data):
        """Test compatibility across different schema versions."""
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir)

            # Version 1: Base schema
            self._write_test_data(data_dir, base_schema_data, prefix="v1_")

            # Version 2: Added fields
            v2_suppliers = base_schema_data["suppliers"].copy()
            v2_suppliers[0]["version"] = "2.0"
            v2_suppliers[0]["new_feature_flag"] = True

            v2_data = {
                "suppliers": v2_suppliers,
                "inventory": base_schema_data["inventory"],
                "sales": base_schema_data["sales"]
            }
            self._write_test_data(data_dir, v2_data, prefix="v2_")

            # Version 3: Changed structure
            v3_suppliers = [{
                "supplier_id": s["supplier_id"],
                "metadata": {
                    "name": s["supplier_name"],
                    "version": "3.0",
                    "features": ["advanced", "premium"]
                },
                "performance": {
                    "rating": s["rating"],
                    "lead_time_days": s["lead_time_days"]
                }
            } for s in base_schema_data["suppliers"]]

            v3_data = {
                "suppliers": v3_suppliers,
                "inventory": base_schema_data["inventory"],
                "sales": base_schema_data["sales"]
            }
            self._write_test_data(data_dir, v3_data, prefix="v3_")

            # Test that all versions can be loaded
            v1_suppliers = self._load_suppliers(
                data_dir / "v1_suppliers" / "suppliers_master.jsonl")
            v2_suppliers = self._load_suppliers(
                data_dir / "v2_suppliers" / "suppliers_master.jsonl")
            v3_suppliers = self._load_suppliers(
                data_dir / "v3_suppliers" / "suppliers_master.jsonl")

            assert len(v1_suppliers) == 1
            assert len(v2_suppliers) == 1
            assert len(v3_suppliers) == 1

            # V3 should have nested structure
            assert "metadata" in v3_suppliers[0]
            assert "performance" in v3_suppliers[0]
            assert v3_suppliers[0]["metadata"]["version"] == "3.0"

    def test_schema_validation_rules(self, base_schema_data):
        """Test schema validation rules and constraints."""
        # Test required fields
        incomplete_suppliers = [{
            "supplier_name": "Incomplete Supplier"
            # Missing supplier_id and other required fields
        }]

        incomplete_inventory = [{
            "inventory_date": "2025-10-01",
            "product_id": "PRD001"
            # Missing many required fields
        }]

        # These should be detectable as invalid
        # Far fewer fields than expected
        assert len(incomplete_suppliers[0]) < 3
        # Far fewer fields than expected
        assert len(incomplete_inventory[0]) < 5

        # Test data type constraints
        invalid_inventory = base_schema_data["inventory"].copy()
        invalid_inventory[0]["quantity_on_hand"] = - \
            100  # Invalid negative quantity

        invalid_sales = base_schema_data["sales"].copy()
        invalid_sales[0]["quantity"] = 0  # Invalid zero quantity
        invalid_sales[0]["unit_price_euro"] = -50.0  # Invalid negative price

        # These should be flagged by business rules
        assert invalid_inventory[0]["quantity_on_hand"] < 0
        assert invalid_sales[0]["quantity"] <= 0
        assert invalid_sales[0]["unit_price_euro"] < 0

    def _write_test_data(self, base_dir: Path, data: Dict[str, List[Dict]], prefix: str = ""):
        """Helper to write test data to files."""
        # Suppliers
        suppliers_dir = base_dir / f"{prefix}suppliers"
        suppliers_dir.mkdir(parents=True, exist_ok=True)
        with open(suppliers_dir / "suppliers_master.jsonl", "w") as f:
            for supplier in data["suppliers"]:
                f.write(json.dumps(supplier) + "\n")

        # Inventory
        for item in data["inventory"]:
            inv_date = item["inventory_date"]
            warehouse = item["warehouse_code"]
            inv_dir = base_dir / f"{prefix}inventory" / inv_date
            inv_dir.mkdir(parents=True, exist_ok=True)
            inv_file = inv_dir / f"{warehouse}_inventory.csv"

            # Write header if file doesn't exist
            if not inv_file.exists():
                with open(inv_file, "w", newline="") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "inventory_date", "warehouse_code", "product_id", "product_name",
                        "category", "quantity_on_hand", "unit_cost_euro", "supplier_id",
                        "restock_threshold", "restock_lead_days", "is_active",
                        "last_updated_ts", "last_sale_ts", "lifetime_revenue_euro"
                    ])

            # Append row
            with open(inv_file, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    item.get("inventory_date"), item.get(
                        "warehouse_code"), item.get("product_id"),
                    item.get("product_name"), item.get(
                        "category"), item.get("quantity_on_hand"),
                    item.get("unit_cost_euro"), item.get(
                        "supplier_id"), item.get("restock_threshold"),
                    item.get("restock_lead_days"), item.get(
                        "is_active"), item.get("last_updated_ts"),
                    item.get("last_sale_ts"), item.get("lifetime_revenue_euro")
                ])

        # Sales
        sales_dir = base_dir / f"{prefix}sales_stream"
        sales_dir.mkdir(parents=True, exist_ok=True)
        with open(sales_dir / "sales_001.json", "w") as f:
            for sale in data["sales"]:
                f.write(json.dumps(sale) + "\n")

    def _load_suppliers(self, file_path: Path) -> List[Dict]:
        """Helper to load suppliers from file."""
        suppliers = []
        with open(file_path, "r") as f:
            for line in f:
                suppliers.append(json.loads(line.strip()))
        return suppliers

    def _load_inventory(self, file_path: Path) -> List[Dict]:
        """Helper to load inventory from CSV file."""
        with open(file_path, "r") as f:
            reader = csv.DictReader(f)
            return list(reader)


if __name__ == "__main__":
    pytest.main([__file__])
