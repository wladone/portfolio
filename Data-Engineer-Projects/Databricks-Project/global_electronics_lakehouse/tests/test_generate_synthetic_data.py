"""Unit tests for generate_synthetic_data.py"""
import json
import random
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

from src.generate_synthetic_data import (
    build_suppliers,
    build_products,
    write_suppliers,
    write_inventory,
    build_sales_events,
    write_sales,
    random_string,
    main,
)


class TestRandomString:
    """Test random_string function."""

    def test_random_string_length(self):
        """Test that random_string generates correct length."""
        result = random_string("TEST", 5)
        assert len(result) == 9  # prefix + length
        assert result.startswith("TEST")

    def test_random_string_uniqueness(self):
        """Test that random_string generates unique strings."""
        results = [random_string("ID", 4) for _ in range(100)]
        assert len(set(results)) == len(results)  # All unique


class TestBuildSuppliers:
    """Test build_suppliers function."""

    @patch('src.generate_synthetic_data.random')
    def test_build_suppliers_structure(self, mock_random):
        """Test that build_suppliers creates correct structure."""
        # Mock random functions
        mock_random.choice.side_effect = lambda x: x[0] if x else "test"
        mock_random.uniform.side_effect = [
            4.5, 10, 0.8, 3.2, 3.5, 4.8]  # More values
        mock_random.randint.side_effect = [
            5, 20, 10000, 50000, 6, 25, 15000, 60000, 7, 30, 200000000, 300000000]  # More values
        mock_random.random.return_value = 0.6

        suppliers = build_suppliers(2)

        assert len(suppliers) == 2
        supplier = suppliers[0]
        assert "supplier_id" in supplier
        assert "supplier_name" in supplier
        assert "rating" in supplier
        assert "lead_time_days" in supplier
        assert "preferred" in supplier
        assert "contact_email" in supplier
        assert "category_focus" in supplier

    def test_build_suppliers_count_validation(self):
        """Test that build_suppliers handles count correctly."""
        suppliers = build_suppliers(1)
        assert len(suppliers) == 1

        suppliers = build_suppliers(5)
        assert len(suppliers) == 5


class TestBuildProducts:
    """Test build_products function."""

    def test_build_products_structure(self):
        """Test that build_products creates correct Product objects."""
        suppliers = [{"supplier_id": "SUP001"}]
        products = build_products(3, suppliers)

        assert len(products) == 3
        product = products[0]
        assert hasattr(product, 'product_id')
        assert hasattr(product, 'product_name')
        assert hasattr(product, 'category')
        assert hasattr(product, 'base_price')
        assert hasattr(product, 'unit_cost')
        assert hasattr(product, 'supplier_id')

    def test_build_products_price_logic(self):
        """Test that product prices are within expected ranges."""
        suppliers = [{"supplier_id": "SUP001"}]
        products = build_products(10, suppliers)

        for product in products:
            assert 30.0 <= product.base_price <= 2500.0
            assert product.unit_cost < product.base_price


class TestWriteSuppliers:
    """Test write_suppliers function."""

    def test_write_suppliers_creates_file(self):
        """Test that write_suppliers creates a JSONL file."""
        suppliers = [{"supplier_id": "SUP001",
                      "supplier_name": "Test Supplier"}]
        mock_logger = MagicMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / "suppliers.jsonl"
            write_suppliers(suppliers, output_path, mock_logger)

            assert output_path.exists()
            with output_path.open('r') as f:
                lines = f.readlines()
                assert len(lines) == 1
                data = json.loads(lines[0])
                assert data["supplier_id"] == "SUP001"
            mock_logger.info.assert_called_once()


class TestWriteInventory:
    """Test write_inventory function."""

    @patch('src.generate_synthetic_data.datetime')
    def test_write_inventory_creates_files(self, mock_datetime):
        """Test that write_inventory creates CSV files."""
        from datetime import datetime, date
        # Mock datetime
        mock_date = date(2023, 1, 1)
        mock_datetime_obj = datetime(2023, 1, 1, 12, 0, 0)
        mock_utcnow = MagicMock()
        mock_utcnow.date.return_value = mock_date
        mock_datetime.utcnow.return_value = mock_utcnow
        mock_datetime.combine.return_value = mock_datetime_obj
        mock_datetime.min.time.return_value = datetime.min.time()

        suppliers = [{"supplier_id": "SUP001"}]
        products = build_products(2, suppliers)
        mock_logger = MagicMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            write_inventory(products, 1, output_dir, mock_logger)

            # Check if files were created
            csv_files = list(output_dir.glob("**/*.csv"))
            assert len(csv_files) > 0


class TestBuildSalesEvents:
    """Test build_sales_events function."""

    def test_build_sales_events_structure(self):
        """Test that build_sales_events creates correct event structure."""
        suppliers = [{"supplier_id": "SUP001"}]
        products = build_products(2, suppliers)

        batches = build_sales_events(products, 2, 3, 0.0)

        assert len(batches) == 2
        assert len(batches[0]) == 3

        event = batches[0][0]
        assert "event_id" in event
        assert "order_id" in event
        assert "product_id" in event
        assert "quantity" in event
        assert "unit_price_euro" in event

    def test_build_sales_events_invalid_ratio(self):
        """Test that invalid product IDs are created based on ratio."""
        suppliers = [{"supplier_id": "SUP001"}]
        products = build_products(2, suppliers)

        batches = build_sales_events(products, 1, 10, 0.5)

        events = batches[0]
        invalid_count = sum(
            1 for event in events if "_INVALID" in event["product_id"])
        assert invalid_count > 0  # Should have some invalid events


class TestDeterminism:
    """Test deterministic generation."""

    def test_deterministic_sales_generation(self):
        """Test that sales generation is deterministic with same seed."""
        suppliers = [{"supplier_id": "SUP001"}]
        products = build_products(2, suppliers)

        random.seed(42)
        batches1 = build_sales_events(products, 1, 10, 0.0)

        random.seed(42)
        batches2 = build_sales_events(products, 1, 10, 0.0)

        assert batches1 == batches2


class TestWriteSales:
    """Test write_sales function."""

    def test_write_sales_creates_files(self):
        """Test that write_sales creates JSON files."""
        events = [
            {"event_id": "EVT001", "order_id": "ORD001", "product_id": "PRD001"}
        ]
        batches = [events]
        mock_logger = MagicMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            write_sales(batches, output_dir, mock_logger)

            json_files = list(output_dir.glob("sales_*.json"))
            assert len(json_files) == 1

            with json_files[0].open('r') as f:
                lines = f.readlines()
                assert len(lines) == 1
                data = json.loads(lines[0])
                assert data["event_id"] == "EVT001"
            mock_logger.info.assert_called_once()


class TestMainFunction:
    """Test main function."""

    @patch('src.generate_synthetic_data.argparse.ArgumentParser.parse_args')
    @patch('src.generate_synthetic_data.random.seed')
    @patch('src.generate_synthetic_data.build_suppliers')
    @patch('src.generate_synthetic_data.build_products')
    @patch('src.generate_synthetic_data.write_suppliers')
    @patch('src.generate_synthetic_data.write_inventory')
    @patch('src.generate_synthetic_data.build_sales_events')
    @patch('src.generate_synthetic_data.write_sales')
    @patch('src.generate_synthetic_data.Path')
    def test_main_success(self, mock_path, mock_write_sales, mock_build_sales,
                          mock_write_inventory, mock_write_suppliers, mock_build_products,
                          mock_build_suppliers, mock_random_seed, mock_parse_args):
        """Test main function with valid arguments."""
        # Mock arguments
        mock_args = type('Args', (), {
            'seed': 42,
            'products': 100,
            'days': 7,
            'suppliers': 10,
            'sales_files': 5,
            'events_per_file': 100,
            'invalid_ratio': 0.01,
            'sales_days': 1,
            'enable_seasonal': False,
            'enable_trending': False,
            'enable_cyclical': False,
            'enable_holidays': False,
            'output': '/tmp/test',
            'log_file': '/tmp/test.log'
        })()
        mock_parse_args.return_value = mock_args

        # Mock Path
        mock_path_instance = mock_path.return_value
        mock_path_instance.__truediv__.return_value = mock_path_instance
        mock_path_instance.mkdir = lambda **kwargs: None

        result = main()

        assert result == 0
        mock_random_seed.assert_called_once_with(42)

    def test_main_invalid_seed(self):
        """Test main function with invalid seed."""
        with patch('src.generate_synthetic_data.argparse.ArgumentParser.parse_args') as mock_parse:
            mock_args = type(
                'Args', (), {'seed': -1, 'log_file': '/tmp/test.log'})()
            mock_parse.return_value = mock_args

            result = main()
            assert result == 1

    def test_main_invalid_counts(self):
        """Test main function with invalid counts."""
        with patch('src.generate_synthetic_data.argparse.ArgumentParser.parse_args') as mock_parse:
            mock_args = type('Args', (), {
                'seed': 42,
                'products': -1,
                'days': 7,
                'suppliers': 10,
                'sales_files': 5,
                'events_per_file': 100,
                'invalid_ratio': 0.01,
                'output': '/tmp/test',
                'log_file': '/tmp/test.log'
            })()
            mock_parse.return_value = mock_args

            result = main()
            assert result == 1

    def test_main_invalid_ratio(self):
        """Test main function with invalid invalid_ratio."""
        with patch('src.generate_synthetic_data.argparse.ArgumentParser.parse_args') as mock_parse:
            mock_args = type('Args', (), {
                'seed': 42,
                'products': 100,
                'days': 7,
                'suppliers': 10,
                'sales_files': 5,
                'events_per_file': 100,
                'invalid_ratio': 1.5,
                'output': '/tmp/test',
                'log_file': '/tmp/test.log'
            })()
            mock_parse.return_value = mock_args

            result = main()
            assert result == 1


if __name__ == "__main__":
    pytest.main([__file__])
