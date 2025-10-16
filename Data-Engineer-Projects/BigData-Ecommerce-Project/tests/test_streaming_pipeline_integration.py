import json
from datetime import datetime, timezone

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam import streaming_pipeline as sp


def test_full_pipeline_integration():
    """Integration test for the full streaming pipeline."""
    with TestPipeline() as p:
        # Create test data
        click_data = [
            json.dumps({
                "event_time": "2023-01-01T00:00:05Z",
                "product_id": "P1"
            }).encode("utf-8"),
            json.dumps({
                "event_time": "2023-01-01T00:00:10Z",
                "product_id": "P1"
            }).encode("utf-8"),
        ]

        transaction_data = [
            json.dumps({
                "event_time": "2023-01-01T00:00:05Z",
                "product_id": "P1",
                "store_id": "S1",
                "qty": 2
            }).encode("utf-8"),
        ]

        stock_data = [
            json.dumps({
                "event_time": "2023-01-01T00:00:05Z",
                "product_id": "P1",
                "warehouse_id": "W1",
                "delta": 10
            }).encode("utf-8"),
        ]

        # Mock known_args
        class MockArgs:
            input_topic_clicks = "clicks"
            input_topic_transactions = "transactions"
            input_topic_stock = "stock"
            dead_letter_topic = "dlq"
            output_bigquery_dataset = "test_dataset"
            output_table_views = "views"
            output_table_sales = "sales"
            output_table_stock = "stock"
            bigtable_project = "test_project"
            bigtable_instance = "test_instance"
            bigtable_table = "test_table"

        known_args = MockArgs()

        # Build IO (mocked)
        # Since we can't easily mock PubSub in integration test, we'll test the transforms separately

        # For now, test the transforms with direct PCollections
        click_events = p | "CreateClicks" >> beam.Create(click_data) | "ParseClicks" >> beam.ParDo(
            sp.ParseAndValidateEvent("click", sp.validate_click))
        transaction_events = p | "CreateTransactions" >> beam.Create(transaction_data) | "ParseTransactions" >> beam.ParDo(
            sp.ParseAndValidateEvent("transaction", sp.validate_transaction))
        stock_events = p | "CreateStock" >> beam.Create(stock_data) | "ParseStock" >> beam.ParDo(
            sp.ParseAndValidateEvent("stock", sp.validate_stock))

        view_counts, sales_counts, stock_counts = sp.build_transforms(
            click_events, transaction_events, stock_events)

        # Collect results
        view_results = view_counts | "CollectViews" >> beam.Map(lambda x: x)
        sales_results = sales_counts | "CollectSales" >> beam.Map(lambda x: x)
        stock_results = stock_counts | "CollectStock" >> beam.Map(lambda x: x)

        # Assert
        assert_that(view_results, equal_to([("P1", 2)]), label="check_views")
        assert_that(sales_results, equal_to(
            [(("P1", "S1"), 2), (("P1", "ALL"), 2)]), label="check_sales")
        assert_that(stock_results, equal_to(
            [(("P1", "W1"), 10), (("P1", "ALL"), 10)]), label="check_stock")


def test_parse_and_validate_with_test_pipeline():
    """Test ParseAndValidateEvent with TestPipeline."""
    with TestPipeline() as p:
        input_data = [
            json.dumps({"event_time": "2023-01-01T00:00:05Z",
                       "product_id": "P1"}).encode("utf-8"),
            b"invalid json",
        ]

        results = (
            p
            | beam.Create(input_data)
            | beam.ParDo(sp.ParseAndValidateEvent("click", sp.validate_click)).with_outputs(
                sp.ParseAndValidateEvent.DEADLETTER_TAG, main="valid"
            )
        )

        # Should have one valid result and one dead letter
        valid_results = results["valid"] | "CollectValid" >> beam.Map(
            lambda x: x.value)
        dead_letters = results[sp.ParseAndValidateEvent.DEADLETTER_TAG] | "CollectDead" >> beam.Map(
            lambda x: x)

        assert_that(valid_results, equal_to(
            [{"event_time": "2023-01-01T00:00:05Z", "product_id": "P1"}]), label="check_valid")
        assert_that(dead_letters, equal_to([{
            "event_type": "click",
            "error": "Expecting value: line 1 column 1 (char 0)",
            "payload": "invalid json"
        }]), label="check_dead")
