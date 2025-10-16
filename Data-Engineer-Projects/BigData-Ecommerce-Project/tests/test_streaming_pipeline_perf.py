import json
import time
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam import streaming_pipeline as sp


def generate_test_events(count: int, event_type: str = "click"):
    """Generate test events for performance testing."""
    events = []
    for i in range(count):
        event = {
            "event_time": "2023-01-01T00:00:00Z",
            "product_id": f"P{i % 100}",  # 100 different products
        }
        if event_type == "transaction":
            event.update({
                "store_id": f"S{i % 10}",
                "qty": 1
            })
        elif event_type == "stock":
            event.update({
                "warehouse_id": f"W{i % 5}",
                "delta": 1
            })
        events.append(json.dumps(event).encode("utf-8"))
    return events


def test_high_volume_click_processing():
    """Performance test for processing high volume of click events."""
    event_count = 10000  # Adjust based on performance requirements

    with TestPipeline() as p:
        input_data = generate_test_events(event_count, "click")

        start_time = time.time()

        results = (
            p
            | beam.Create(input_data)
            | beam.ParDo(sp.ParseAndValidateEvent("click", sp.validate_click)).with_outputs(
                sp.ParseAndValidateEvent.DEADLETTER_TAG, main="valid"
            )
        )

        valid_count = results["valid"] | "CountValid" >> beam.combiners.Count.Globally(
        )

        # Assert we processed all events
        assert_that(valid_count, equal_to(event_count), label="check_count")

        end_time = time.time()
        processing_time = end_time - start_time

        # Log performance metric (in real scenario, this would be monitored)
        print(
            f"Processed {event_count} click events in {processing_time:.2f} seconds")
        print(f"Throughput: {event_count / processing_time:.0f} events/second")

        # Assert reasonable performance (adjust thresholds as needed)
        assert processing_time < 60  # Should process 10k events in under 60 seconds
        assert event_count / processing_time > 100  # At least 100 events/second


def test_transforms_performance():
    """Test performance of the transform operations."""
    event_count = 5000

    with TestPipeline() as p:
        click_data = generate_test_events(event_count, "click")
        transaction_data = generate_test_events(event_count, "transaction")
        stock_data = generate_test_events(event_count, "stock")

        start_time = time.time()

        click_events = p | "CreateClicks" >> beam.Create(click_data) | "ParseClicks" >> beam.ParDo(
            sp.ParseAndValidateEvent("click", sp.validate_click))
        transaction_events = p | "CreateTransactions" >> beam.Create(transaction_data) | "ParseTransactions" >> beam.ParDo(
            sp.ParseAndValidateEvent("transaction", sp.validate_transaction))
        stock_events = p | "CreateStock" >> beam.Create(stock_data) | "ParseStock" >> beam.ParDo(
            sp.ParseAndValidateEvent("stock", sp.validate_stock))

        view_counts, sales_counts, stock_counts = sp.build_transforms(
            click_events, transaction_events, stock_events)

        # Count results
        view_count = view_counts | "CountViews" >> beam.combiners.Count.Globally()
        sales_count = sales_counts | "CountSales" >> beam.combiners.Count.Globally()
        stock_count = stock_counts | "CountStock" >> beam.combiners.Count.Globally()

        # Assert counts (views should be grouped by product)
        assert_that(view_count, equal_to(100),
                    label="check_view_count")  # 100 products
        # 100 products * 2 (product + ALL)
        assert_that(sales_count, equal_to(200), label="check_sales_count")
        # 100 products * 1.5 (product + ALL, but some overlap)
        assert_that(stock_count, equal_to(150), label="check_stock_count")

        end_time = time.time()
        processing_time = end_time - start_time

        print(
            f"Transformed {event_count * 3} events in {processing_time:.2f} seconds")
        print(
            f"Throughput: {(event_count * 3) / processing_time:.0f} events/second")

        # Performance assertions
        assert processing_time < 120  # Under 2 minutes for 15k events
        # At least 50 events/second
        assert (event_count * 3) / processing_time > 50


def test_memory_efficiency():
    """Test that processing doesn't cause memory issues with large datasets."""
    # This is a basic test - in production, you'd monitor memory usage
    event_count = 50000

    with TestPipeline() as p:
        input_data = generate_test_events(event_count, "click")

        results = (
            p
            | beam.Create(input_data)
            | beam.ParDo(sp.ParseAndValidateEvent("click", sp.validate_click)).with_outputs(
                sp.ParseAndValidateEvent.DEADLETTER_TAG, main="valid"
            )
        )

        valid_count = results["valid"] | "CountValid" >> beam.combiners.Count.Globally(
        )

        assert_that(valid_count, equal_to(event_count),
                    label="check_large_count")

        print(
            f"Successfully processed {event_count} events without memory issues")
