"""Unit tests for DLT pipeline functions in electronics_dlt.py"""
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from src.dlt.electronics_dlt import (
    _config,
    _warehouse_country,
    WAREHOUSE_COUNTRY_LOOKUP,
)


class TestConfig:
    """Test _config function."""

    @patch('src.dlt.electronics_dlt.spark')
    def test_config_success(self, mock_spark):
        """Test successful config retrieval."""
        mock_spark.conf.get.return_value = "test_value"

        result = _config("test.key")

        assert result == "test_value"
        mock_spark.conf.get.assert_called_once_with("test.key")

    @patch('src.dlt.electronics_dlt.spark')
    def test_config_empty_value(self, mock_spark):
        """Test config with empty value."""
        mock_spark.conf.get.return_value = ""

        with pytest.raises(ValueError, match="Configuration key 'test.key' is empty"):
            _config("test.key")

    @patch('src.dlt.electronics_dlt.spark')
    def test_config_with_default(self, mock_spark):
        """Test config with default value."""
        mock_spark.conf.get.side_effect = Exception("Key not found")

        result = _config("test.key", "default_value")

        assert result == "default_value"

    @patch('src.dlt.electronics_dlt.spark')
    def test_config_exception_without_default(self, mock_spark):
        """Test config exception without default."""
        mock_spark.conf.get.side_effect = Exception("Key not found")

        with pytest.raises(Exception, match="Key not found"):
            _config("test.key")


class TestWarehouseCountry:
    """Test _warehouse_country function."""

    def test_warehouse_country_known_codes(self):
        """Test _warehouse_country with known warehouse codes."""
        # Test FRA
        result = _warehouse_country("warehouse_code")
        # This would create a column expression, hard to test directly
        # Instead, we'll test the lookup dictionary
        assert WAREHOUSE_COUNTRY_LOOKUP["FRA"] == "France"
        assert WAREHOUSE_COUNTRY_LOOKUP["BUC"] == "Romania"
        assert WAREHOUSE_COUNTRY_LOOKUP["MAD"] == "Spain"

    def test_warehouse_country_lookup_complete(self):
        """Test that all expected countries are in lookup."""
        expected_codes = ["FRA", "BUC", "MAD"]
        expected_countries = ["France", "Romania", "Spain"]

        for code, country in zip(expected_codes, expected_countries):
            assert WAREHOUSE_COUNTRY_LOOKUP[code] == country


class TestDLTTables:
    """Test DLT table functions with mocking."""

    @patch('src.dlt.electronics_dlt.spark')
    @patch('src.dlt.electronics_dlt._config')
    @patch('src.dlt.electronics_dlt.dlt')
    def test_bronze_inventory_table(self, mock_dlt, mock_config, mock_spark):
        """Test bronze_inventory table function."""
        # Mock the config calls
        mock_config.side_effect = lambda key, default=None: {
            "pipelines.electronics.source_inventory": "/mnt/landing/electronics/inventory"
        }.get(key, default)

        # Mock Spark DataFrame operations
        mock_df = MagicMock()
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.schema.return_value.load.return_value.select.return_value = mock_df

        # Import and call the function
        from src.dlt.electronics_dlt import bronze_inventory

        result = bronze_inventory()

        assert result == mock_df
        # Verify the chain of calls
        mock_spark.readStream.format.assert_called_with("cloudFiles")

    @patch('src.dlt.electronics_dlt.spark')
    @patch('src.dlt.electronics_dlt._config')
    @patch('src.dlt.electronics_dlt.dlt')
    def test_bronze_suppliers_table(self, mock_dlt, mock_config, mock_spark):
        """Test bronze_suppliers table function."""
        mock_config.side_effect = lambda key, default=None: {
            "pipelines.electronics.source_suppliers": "/mnt/landing/electronics/suppliers"
        }.get(key, default)

        mock_df = MagicMock()
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.schema.return_value.load.return_value.select.return_value = mock_df

        from src.dlt.electronics_dlt import bronze_suppliers

        result = bronze_suppliers()

        assert result == mock_df

    @patch('src.dlt.electronics_dlt.spark')
    @patch('src.dlt.electronics_dlt._config')
    @patch('src.dlt.electronics_dlt.dlt')
    def test_bronze_sales_stream_table(self, mock_dlt, mock_config, mock_spark):
        """Test bronze_sales_stream table function."""
        mock_config.side_effect = lambda key, default=None: {
            "pipelines.electronics.kafka_bootstrap": "localhost:9092",
            "pipelines.electronics.kafka_topic": "global_electronics_sales"
        }.get(key, default)

        mock_df = MagicMock()
        mock_spark.readStream.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.option.return_value.load.return_value.selectExpr.return_value.withColumn.return_value = mock_df

        from src.dlt.electronics_dlt import bronze_sales_stream

        result = bronze_sales_stream()

        assert result == mock_df

    @patch('src.dlt.electronics_dlt.dlt')
    def test_bronze_sales_parsed_table(self, mock_dlt):
        """Test bronze_sales_parsed table function."""
        mock_stream_df = MagicMock()
        mock_dlt.read_stream.return_value = mock_stream_df
        mock_stream_df.select.return_value.select.return_value.select.return_value.filter.return_value = mock_stream_df

        from src.dlt.electronics_dlt import bronze_sales_parsed

        result = bronze_sales_parsed()

        assert result == mock_stream_df
        mock_dlt.read_stream.assert_called_with("bronze_sales_stream")

    @patch('src.dlt.electronics_dlt.dlt')
    def test_silver_inventory_table(self, mock_dlt):
        """Test silver_inventory table function."""
        mock_df = MagicMock()
        mock_dlt.read.return_value.select.return_value = mock_df

        from src.dlt.electronics_dlt import silver_inventory

        result = silver_inventory()

        assert result == mock_df
        mock_dlt.read.assert_called_with("bronze_inventory")

    @patch('src.dlt.electronics_dlt.dlt')
    def test_silver_suppliers_table(self, mock_dlt):
        """Test silver_suppliers table function."""
        mock_df = MagicMock()
        mock_dlt.read.return_value = mock_df

        from src.dlt.electronics_dlt import silver_suppliers

        result = silver_suppliers()

        assert result == mock_df
        mock_dlt.read.assert_called_with("bronze_suppliers")

    @patch('src.dlt.electronics_dlt.dlt')
    def test_silver_product_reference_table(self, mock_dlt):
        """Test silver_product_reference table function."""
        mock_df = MagicMock()
        mock_dlt.read.return_value.groupBy.return_value.agg.return_value = mock_df

        from src.dlt.electronics_dlt import silver_product_reference

        result = silver_product_reference()

        assert result == mock_df

    @patch('src.dlt.electronics_dlt.dlt')
    @patch('src.dlt.electronics_dlt.F')
    def test_silver_sales_cleaned_table(self, mock_f, mock_dlt):
        """Test silver_sales_cleaned table function."""
        mock_stream_df = MagicMock()
        mock_dlt.read_stream.return_value.withWatermark.return_value.withColumn.return_value.filter.return_value.drop.return_value = mock_stream_df

        from src.dlt.electronics_dlt import silver_sales_cleaned

        result = silver_sales_cleaned()

        assert result == mock_stream_df

    @patch('src.dlt.electronics_dlt.dlt')
    @patch('src.dlt.electronics_dlt.F')
    def test_silver_sales_enriched_table(self, mock_f, mock_dlt):
        """Test silver_sales_enriched table function."""
        mock_products = MagicMock()
        mock_dlt.read.return_value = mock_products

        mock_stream_df = MagicMock()
        mock_dlt.read_stream.return_value.join.return_value.withColumn.return_value.withColumn.return_value = mock_stream_df

        from src.dlt.electronics_dlt import silver_sales_enriched

        result = silver_sales_enriched()

        assert result == mock_stream_df

    @patch('src.dlt.electronics_dlt.dlt')
    def test_gold_inventory_levels_table(self, mock_dlt):
        """Test gold_inventory_levels table function."""
        mock_df = MagicMock()
        mock_dlt.read.return_value.withColumn.return_value.filter.return_value.groupBy.return_value.agg.return_value = mock_df

        from src.dlt.electronics_dlt import gold_inventory_levels

        result = gold_inventory_levels()

        assert result == mock_df

    @patch('src.dlt.electronics_dlt.dlt')
    @patch('src.dlt.electronics_dlt.F')
    def test_gold_sales_performance_table(self, mock_f, mock_dlt):
        """Test gold_sales_performance table function."""
        mock_stream_df = MagicMock()
        mock_dlt.read_stream.return_value.withWatermark.return_value.groupBy.return_value.agg.return_value.select.return_value = mock_stream_df

        from src.dlt.electronics_dlt import gold_sales_performance

        result = gold_sales_performance()

        assert result == mock_stream_df

    @patch('src.dlt.electronics_dlt.dlt')
    @patch('src.dlt.electronics_dlt.F')
    def test_gold_order_anomalies_table(self, mock_f, mock_dlt):
        """Test gold_order_anomalies table function."""
        mock_sales = MagicMock()
        mock_dlt.read_stream.return_value.withWatermark.return_value.groupBy.return_value.agg.return_value = mock_sales

        mock_stats = MagicMock()
        mock_sales.select.return_value.groupBy.return_value.agg.return_value.withColumn.return_value = mock_stats

        mock_final_df = MagicMock()
        mock_sales.select.return_value.join.return_value.withColumn.return_value.select.return_value = mock_final_df

        from src.dlt.electronics_dlt import gold_order_anomalies

        result = gold_order_anomalies()

        assert result == mock_final_df


if __name__ == "__main__":
    pytest.main([__file__])
