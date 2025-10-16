"""Unit tests for ingestion functions in notebooks_local/1_local_ingest_delta.py"""
import csv
import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Import the module with numeric name using importlib
spec = importlib.util.spec_from_file_location(
    "ingestion_module",
    Path(__file__).parent.parent /
    "notebooks_local" / "1_local_ingest_delta.py"
)
ingestion_module = importlib.util.module_from_spec(spec)
sys.modules["ingestion_module"] = ingestion_module
spec.loader.exec_module(ingestion_module)

# Import functions from the loaded module
load_schema = ingestion_module.load_schema
write_quarantined_data = ingestion_module.write_quarantined_data
build_spark = ingestion_module.build_spark
ingest_inventory = ingestion_module.ingest_inventory
ingest_suppliers = ingestion_module.ingest_suppliers
ingest_sales = ingestion_module.ingest_sales


class TestLoadSchema:
    """Test load_schema function."""

    def test_load_schema_success(self):
        """Test successful schema loading."""
        schema_data = {
            "type": "struct",
            "fields": [
                {"name": "test_field", "type": "string",
                    "nullable": True, "metadata": {}}
            ]
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "test_schema.json"
            with schema_path.open('w') as f:
                json.dump(schema_data, f)

            schema = load_schema(schema_path)

            assert isinstance(schema, StructType)
            assert len(schema.fields) == 1
            assert schema.fields[0].name == "test_field"

    def test_load_schema_file_not_found(self):
        """Test load_schema with nonexistent file."""
        nonexistent_path = Path("/nonexistent/schema.json")

        with pytest.raises(FileNotFoundError):
            load_schema(nonexistent_path)

    def test_load_schema_invalid_json(self):
        """Test load_schema with invalid JSON."""
        with tempfile.TemporaryDirectory() as temp_dir:
            schema_path = Path(temp_dir) / "invalid_schema.json"
            with schema_path.open('w') as f:
                f.write("invalid json")

            with pytest.raises(ValueError):
                load_schema(schema_path)


class TestWriteQuarantinedData:
    """Test write_quarantined_data function."""

    def test_write_quarantined_data_with_rows(self):
        """Test writing quarantined data with actual rows."""
        mock_df = MagicMock()
        invalid_rows = [
            {"id": "1", "error": "invalid"},
            {"id": "2", "error": "invalid"}
        ]
        mock_df.collect.return_value = invalid_rows
        mock_logger = MagicMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            logs_dir = Path(temp_dir)
            result = write_quarantined_data(
                mock_df, logs_dir, "test.csv", mock_logger)

            assert result == 2
            csv_file = logs_dir / "test.csv"
            assert csv_file.exists()

            with csv_file.open('r', newline='') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) == 2
                assert rows[0]["id"] == "1"

    def test_write_quarantined_data_empty(self):
        """Test writing quarantined data with no rows."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_logger = MagicMock()

        with tempfile.TemporaryDirectory() as temp_dir:
            logs_dir = Path(temp_dir)
            result = write_quarantined_data(
                mock_df, logs_dir, "test.csv", mock_logger)

            assert result == 0
            csv_file = logs_dir / "test.csv"
            assert not csv_file.exists()


class TestBuildSpark:
    """Test build_spark function."""

    @patch('notebooks_local.ingestion_functions.configure_spark_with_delta_pip')
    @patch('notebooks_local.ingestion_functions.SparkSession')
    def test_build_spark_success(self, mock_spark_session, mock_configure):
        """Test successful Spark session creation."""
        mock_builder = MagicMock()
        mock_spark_session.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = MagicMock()

        mock_configure.return_value = mock_builder

        spark = build_spark(8)

        assert spark is not None
        mock_builder.config.assert_called()


class TestIngestInventory:
    """Test ingest_inventory function."""

    @patch('notebooks_local.ingestion_functions.Path')
    def test_ingest_inventory_missing_directory(self, mock_path):
        """Test ingest_inventory with missing inventory directory."""
        mock_spark = MagicMock()
        mock_data_dir = MagicMock()
        mock_data_dir.__truediv__.return_value.exists.return_value = False
        mock_schema = MagicMock()
        mock_output_dir = MagicMock()
        mock_logs_dir = MagicMock()
        mock_logger = MagicMock()

        with pytest.raises(FileNotFoundError, match="Inventory directory not found"):
            ingest_inventory(mock_spark, mock_data_dir, mock_schema,
                             mock_output_dir, mock_logs_dir, mock_logger)

    @patch('notebooks_local.ingestion_functions.Path')
    @patch('notebooks_local.ingestion_functions.F')
    def test_ingest_inventory_success(self, mock_f, mock_path):
        """Test successful inventory ingestion."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.where.return_value = mock_df
        mock_df.count.return_value = 100
        mock_spark.read.schema.return_value.option.return_value.option.return_value.option.return_value.option.return_value.csv.return_value.withColumn.return_value = mock_df

        mock_data_dir = MagicMock()
        mock_inventory_path = MagicMock()
        mock_inventory_path.exists.return_value = True
        mock_data_dir.__truediv__.return_value = mock_inventory_path

        mock_schema = MagicMock()
        mock_output_dir = MagicMock()
        mock_target_path = MagicMock()
        mock_output_dir.__truediv__.return_value = mock_target_path
        mock_logs_dir = MagicMock()
        mock_logger = MagicMock()

        result = ingest_inventory(
            mock_spark, mock_data_dir, mock_schema, mock_output_dir, mock_logs_dir, mock_logger)

        assert result == 100
        mock_logger.info.assert_called()


class TestIngestSuppliers:
    """Test ingest_suppliers function."""

    @patch('notebooks_local.ingestion_functions.Path')
    def test_ingest_suppliers_missing_file(self, mock_path):
        """Test ingest_suppliers with missing suppliers file."""
        mock_spark = MagicMock()
        mock_data_dir = MagicMock()
        mock_suppliers_path = MagicMock()
        mock_suppliers_path.exists.return_value = False
        mock_data_dir.__truediv__.return_value.__truediv__.return_value = mock_suppliers_path
        mock_schema = MagicMock()
        mock_output_dir = MagicMock()
        mock_logs_dir = MagicMock()
        mock_logger = MagicMock()

        with pytest.raises(FileNotFoundError, match="Suppliers file not found"):
            ingest_suppliers(mock_spark, mock_data_dir, mock_schema,
                             mock_output_dir, mock_logs_dir, mock_logger)

    @patch('notebooks_local.ingestion_functions.Path')
    @patch('notebooks_local.ingestion_functions.F')
    def test_ingest_suppliers_success(self, mock_f, mock_path):
        """Test successful suppliers ingestion."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.where.return_value = mock_df
        mock_df.count.return_value = 50
        mock_spark.read.schema.return_value.json.return_value.withColumn.return_value = mock_df

        mock_data_dir = MagicMock()
        mock_suppliers_path = MagicMock()
        mock_suppliers_path.exists.return_value = True
        mock_data_dir.__truediv__.return_value.__truediv__.return_value = mock_suppliers_path

        mock_schema = MagicMock()
        mock_output_dir = MagicMock()
        mock_target_path = MagicMock()
        mock_output_dir.__truediv__.return_value = mock_target_path
        mock_logs_dir = MagicMock()
        mock_logger = MagicMock()

        result = ingest_suppliers(
            mock_spark, mock_data_dir, mock_schema, mock_output_dir, mock_logs_dir, mock_logger)

        assert result == 50
        mock_logger.info.assert_called()


class TestIngestSales:
    """Test ingest_sales function."""

    @patch('notebooks_local.ingestion_functions.Path')
    def test_ingest_sales_missing_directory(self, mock_path):
        """Test ingest_sales with missing sales directory."""
        mock_spark = MagicMock()
        mock_data_dir = MagicMock()
        mock_sales_path = MagicMock()
        mock_sales_path.exists.return_value = False
        mock_data_dir.__truediv__.return_value = mock_sales_path
        mock_schema = MagicMock()
        mock_output_dir = MagicMock()
        mock_logs_dir = MagicMock()
        mock_logger = MagicMock()

        with pytest.raises(FileNotFoundError, match="Sales directory not found"):
            ingest_sales(mock_spark, mock_data_dir, mock_schema,
                         mock_output_dir, mock_logs_dir, mock_logger)

    @patch('notebooks_local.ingestion_functions.Path')
    @patch('notebooks_local.ingestion_functions.F')
    def test_ingest_sales_success(self, mock_f, mock_path):
        """Test successful sales ingestion."""
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_df.where.return_value = mock_df
        mock_df.count.return_value = 200
        mock_spark.read.schema.return_value.json.return_value.withColumn.return_value = mock_df

        mock_data_dir = MagicMock()
        mock_sales_path = MagicMock()
        mock_sales_path.exists.return_value = True
        mock_data_dir.__truediv__.return_value = mock_sales_path

        mock_schema = MagicMock()
        mock_output_dir = MagicMock()
        mock_target_path = MagicMock()
        mock_output_dir.__truediv__.return_value = mock_target_path
        mock_logs_dir = MagicMock()
        mock_logger = MagicMock()

        result = ingest_sales(mock_spark, mock_data_dir, mock_schema,
                              mock_output_dir, mock_logs_dir, mock_logger)

        assert result == 200
        mock_logger.info.assert_called()


if __name__ == "__main__":
    pytest.main([__file__])
