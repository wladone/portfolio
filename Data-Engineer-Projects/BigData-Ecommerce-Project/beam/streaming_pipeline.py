#!/usr/bin/env python
"""Streaming e-commerce pipeline built with Apache Beam."""
from __future__ import annotations

import argparse
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Tuple

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from apache_beam.io.gcp.pubsub import ReadFromPubSub, WriteToPubSub
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from google.cloud.bigtable.row import DirectRow

import config

LOGGER = logging.getLogger(__name__)

_TOPIC_PATH_PATTERN = re.compile(
    r"^projects/(?P<project>[a-z][a-z0-9-]{5,29})/topics/(?P<topic>[-A-Za-z0-9_]{3,255})$"
)

ADDITIONAL_BQ_PARAMETERS = {
    "timePartitioning": {"type": "DAY", "field": "window_start"},
    "clustering": {"fields": ["product_id"]},
}


def create_bigquery_schema(*fields: str) -> str:
    """Create BigQuery schema string from field definitions."""
    return ",".join(fields)


def create_output_metrics(row_type: str, namespace: str = config.METRICS_NAMESPACE) -> tuple:
    """Create common output metrics for formatting rows."""
    counter = Metrics.counter(namespace, f"{row_type}_rows_emitted")
    distribution = Metrics.distribution(namespace, f"{row_type}_per_window")
    return counter, distribution


def create_event_stream(
    pipeline, topic: str, event_name: str, validator: Callable[[Dict[str, Any]], Dict[str, Any]]
):
    """Create a standardized event processing stream from Pub/Sub topic."""
    return (
        pipeline
        | f"Read{event_name.capitalize()}" >> ReadFromPubSub(topic=topic)
        | f"Decode{event_name.capitalize()}"
        >> beam.ParDo(ParseAndValidateEvent(event_name, validator)).with_outputs(
            ParseAndValidateEvent.DEADLETTER_TAG, main="valid"
        )
    )


def format_row_with_window(row: Dict[str, Any], window) -> Dict[str, Any]:
    """Add window bounds to a row dictionary."""
    row.update(format_window_bounds(window))
    return row


def format_window(ts: datetime) -> str:
    """Return an ISO-8601 UTC timestamp string for BigQuery storage."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def format_window_bounds(window) -> Dict[str, str]:
    """Extract window start and end timestamps into a dictionary."""
    return {
        "window_start": format_window(window.start.to_utc_datetime()),
        "window_end": format_window(window.end.to_utc_datetime()),
    }


def log_event_processing(element: Dict[str, Any], event_type: str) -> None:
    """Emit a lightweight log record for observability."""
    product_id = element.get("product_id", "unknown")
    LOGGER.info(
        "Processed %s row for product_id=%s window_start=%s",
        event_type,
        product_id,
        element.get("window_start"),
    )


def parse_event_timestamp(value: str) -> float:
    """Convert ISO 8601 timestamp string to Beam timestamp (seconds)."""
    try:
        if value.endswith("Z"):
            value = value.replace("Z", "+00:00")
        return datetime.fromisoformat(value).timestamp()
    except ValueError as exc:  # pragma: no cover - defensive
        raise ValueError(f"Invalid event_time: {value}") from exc


def require_fields(payload: Dict[str, Any], fields: Tuple[str, ...]) -> None:
    for field in fields:
        if field not in payload:
            raise ValueError(f"Missing field '{field}'")
        if payload[field] in (None, ""):
            raise ValueError(f"Empty field '{field}'")


def _validate_topic_paths(paths: Iterable[str]) -> None:
    for path in paths:
        match = _TOPIC_PATH_PATTERN.fullmatch(path)
        if not match:
            raise ValueError(f"Invalid Pub/Sub topic path: {path!r}")
        config.validate_pubsub_topics([match.group("topic")])


class ParseAndValidateEvent(beam.DoFn):
    """Parse JSON messages, validate schema, and emit timestamped dicts."""

    DEADLETTER_TAG = "dead_letter"

    def __init__(
        self,
        event_name: str,
        validator: Callable[[Dict[str, Any]], Dict[str, Any]],
        metrics_namespace: str = config.METRICS_NAMESPACE,
    ) -> None:
        self.event_name = event_name
        self.validator = validator
        self.metrics_namespace = metrics_namespace
        self.received_counter = Metrics.counter(
            metrics_namespace, f"{event_name}_received")
        self.processed_counter = Metrics.counter(
            metrics_namespace, f"{event_name}_processed")
        self.dead_letter_counter = Metrics.counter(
            metrics_namespace, f"{event_name}_invalid")
        self.validation_latency = Metrics.distribution(
            metrics_namespace, f"{event_name}_validation_latency_ms"
        )
        self.consecutive_failures = Metrics.counter(
            metrics_namespace, f"{event_name}_consecutive_failures"
        )
        self.circuit_breaker_tripped = Metrics.counter(
            metrics_namespace, f"{event_name}_circuit_tripped"
        )
        self._failure_count = 0
        self._circuit_open = False
        self._next_attempt_time = 0.0
        self._CIRCUIT_THRESHOLD = 10
        self._CIRCUIT_TIMEOUT = 60

    def process(self, element: bytes) -> Iterable[pvalue.TaggedOutput]:
        current_time = time.time()
        self.received_counter.inc()

        if self._circuit_open:
            if current_time < self._next_attempt_time:
                LOGGER.debug(
                    "Circuit open for %s; skipping message to protect DLQ.",
                    self.event_name,
                )
                return
            self._circuit_open = False
            self._failure_count = 0
            LOGGER.info("Circuit breaker closed for %s", self.event_name)

        start = time.time()
        try:
            message = element.decode("utf-8")
            payload = json.loads(message)
            payload = self.validator(payload)
            event_timestamp = parse_event_timestamp(payload["event_time"])
            self.processed_counter.inc()
            self._failure_count = 0
            self.validation_latency.update(int((time.time() - start) * 1000))
            yield beam.window.TimestampedValue(payload, event_timestamp)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, KeyError) as exc:
            self._failure_count += 1
            self.dead_letter_counter.inc()
            self.consecutive_failures.inc()
            if self._failure_count >= self._CIRCUIT_THRESHOLD and not self._circuit_open:
                self._circuit_open = True
                self._next_attempt_time = current_time + self._CIRCUIT_TIMEOUT
                self.circuit_breaker_tripped.inc()
                LOGGER.warning(
                    "Circuit breaker tripped for %s after %d failures; pausing %d seconds.",
                    self.event_name,
                    self._failure_count,
                    self._CIRCUIT_TIMEOUT,
                )
            LOGGER.warning("Invalid %s event: %s", self.event_name, exc)
            dead_letter_payload = {
                "event_type": self.event_name,
                "error": str(exc),
                "payload": element.decode("utf-8", errors="replace"),
            }
            yield pvalue.TaggedOutput(self.DEADLETTER_TAG, dead_letter_payload)


def validate_click(payload: Dict[str, Any]) -> Dict[str, Any]:
    require_fields(payload, ("event_time", "product_id"))
    return payload


def validate_transaction(payload: Dict[str, Any]) -> Dict[str, Any]:
    require_fields(payload, ("event_time", "product_id", "store_id", "qty"))
    payload["qty"] = int(payload["qty"])
    return payload


def validate_stock(payload: Dict[str, Any]) -> Dict[str, Any]:
    require_fields(
        payload, ("event_time", "product_id", "warehouse_id", "delta"))
    payload["delta"] = int(payload["delta"])
    return payload


class FormatViewsRow(beam.DoFn):
    """Format product view counts for BigQuery."""

    def __init__(self) -> None:
        self.counter, self.view_distribution = create_output_metrics("views")

    def process(self, element, window=beam.DoFn.WindowParam):
        product_id, view_count = element
        row = format_row_with_window({
            "product_id": product_id,
            "view_count": view_count,
        }, window)
        self.counter.inc()
        self.view_distribution.update(int(view_count))
        log_event_processing(row, "views")
        yield row


class FormatSalesRow(beam.DoFn):
    """Format sales aggregates for BigQuery."""

    def __init__(self) -> None:
        self.counter, self.sales_distribution = create_output_metrics(
            "sales_quantity")

    def process(self, element, window=beam.DoFn.WindowParam):
        (product_id, store_id), sales_count = element
        row = format_row_with_window({
            "product_id": product_id,
            "store_id": store_id,
            "sales_count": sales_count,
        }, window)
        self.counter.inc()
        self.sales_distribution.update(int(sales_count))
        log_event_processing(row, "sales")
        yield row


class FormatStockRow(beam.DoFn):
    """Format stock aggregates for BigQuery."""

    def __init__(self) -> None:
        self.counter, self.stock_distribution = create_output_metrics(
            "stock_delta")

    def process(self, element, window=beam.DoFn.WindowParam):
        (product_id, warehouse_id), stock_count = element
        row = format_row_with_window({
            "product_id": product_id,
            "warehouse_id": warehouse_id,
            "stock_count": stock_count,
        }, window)
        self.counter.inc()
        self.stock_distribution.update(int(stock_count))
        log_event_processing(row, "stock")
        yield row


class ToBigtableRow(beam.DoFn):
    """Convert product view counts into Bigtable DirectRow mutations."""

    def __init__(
        self,
        column_family: str = config.BIGTABLE_COLUMN_FAMILY,
        column: str = config.BIGTABLE_VIEW_COLUMN,
    ) -> None:
        self.column_family = column_family
        self.column = column

    def process(self, element, window=beam.DoFn.WindowParam):
        product_id, view_count = element
        row = DirectRow(row_key=product_id.encode("utf-8"))
        row.set_cell(
            self.column_family,
            self.column.encode("utf-8"),
            str(view_count).encode("utf-8"),
            timestamp=window.end.to_utc_datetime(),
        )
        yield row


def ensure_bigtable_configuration(table: str, column_family: str) -> None:
    if not table:
        raise ValueError(
            "Bigtable table must be configured via --bigtable_table")
    if not column_family:
        raise ValueError("Bigtable column family must be configured")
    LOGGER.info("Bigtable sink configured table=%s column_family=%s",
                table, column_family)


def build_io(pipeline, known_args):
    clicks = create_event_stream(
        pipeline, known_args.input_topic_clicks, "click", validate_click)
    transactions = create_event_stream(
        pipeline, known_args.input_topic_transactions, "transaction", validate_transaction)
    stock_updates = create_event_stream(
        pipeline, known_args.input_topic_stock, "stock", validate_stock)

    dead_letters = [
        clicks[ParseAndValidateEvent.DEADLETTER_TAG],
        transactions[ParseAndValidateEvent.DEADLETTER_TAG],
        stock_updates[ParseAndValidateEvent.DEADLETTER_TAG],
    ]
    return clicks["valid"], transactions["valid"], stock_updates["valid"], dead_letters


def build_transforms(click_events, transaction_events, stock_events):
    view_counts = (
        click_events
        | "AddViewKeys" >> beam.Map(lambda row: (row["product_id"], 1))
        | "WindowViews" >> beam.WindowInto(FixedWindows(config.VIEWS_WINDOW_SECONDS))
        | "SumViews" >> beam.CombinePerKey(sum)
    )

    sales_counts = (
        transaction_events
        | "WindowSales" >> beam.WindowInto(FixedWindows(config.AGGREGATION_WINDOW_SECONDS))
        | "ExpandSalesKeys"
        >> beam.FlatMap(
            lambda row: [
                ((row["product_id"], row["store_id"]), row["qty"]),
                ((row["product_id"], "ALL"), row["qty"]),
            ]
        )
        | "SumSales" >> beam.CombinePerKey(sum)
    )

    stock_counts = (
        stock_events
        | "WindowStock" >> beam.WindowInto(FixedWindows(config.AGGREGATION_WINDOW_SECONDS))
        | "ExpandStockKeys"
        >> beam.FlatMap(
            lambda row: [
                ((row["product_id"], row["warehouse_id"]), row["delta"]),
                ((row["product_id"], "ALL"), row["delta"]),
            ]
        )
        | "SumStock" >> beam.CombinePerKey(sum)
    )

    return view_counts, sales_counts, stock_counts


def write_dead_letters(dead_letters, topic: str):
    if not dead_letters:
        return
    (
        pvalue.PCollectionList(dead_letters)
        | "FlattenDeadLetters" >> beam.Flatten()
        | "EncodeDeadLetters" >> beam.Map(lambda row: json.dumps(row).encode("utf-8"))
        | "WriteDeadLetters" >> WriteToPubSub(topic=topic)
    )


def write_sinks(view_counts, sales_counts, stock_counts, known_args):
    dataset = known_args.output_bigquery_dataset
    views_table = f"{dataset}.{known_args.output_table_views}"
    sales_table = f"{dataset}.{known_args.output_table_sales}"
    stock_table = f"{dataset}.{known_args.output_table_stock}"

    views_schema = create_bigquery_schema(
        "product_id:STRING", "window_start:TIMESTAMP", "window_end:TIMESTAMP", "view_count:INT64"
    )
    sales_schema = create_bigquery_schema(
        "product_id:STRING", "store_id:STRING",
        "window_start:TIMESTAMP", "window_end:TIMESTAMP", "sales_count:INT64"
    )
    stock_schema = create_bigquery_schema(
        "product_id:STRING", "warehouse_id:STRING",
        "window_start:TIMESTAMP", "window_end:TIMESTAMP", "stock_count:INT64"
    )

    LOGGER.info(
        "Writing aggregates to BigQuery dataset=%s tables=%s,%s,%s",
        dataset,
        known_args.output_table_views,
        known_args.output_table_sales,
        known_args.output_table_stock,
    )

    (
        view_counts
        | "FormatViews" >> beam.ParDo(FormatViewsRow())
        | "WriteViewsToBQ"
        >> WriteToBigQuery(
            views_table,
            schema=views_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters=ADDITIONAL_BQ_PARAMETERS,
        )
    )

    (
        sales_counts
        | "FormatSales" >> beam.ParDo(FormatSalesRow())
        | "WriteSalesToBQ"
        >> WriteToBigQuery(
            sales_table,
            schema=sales_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters=ADDITIONAL_BQ_PARAMETERS,
        )
    )

    (
        stock_counts
        | "FormatStock" >> beam.ParDo(FormatStockRow())
        | "WriteStockToBQ"
        >> WriteToBigQuery(
            stock_table,
            schema=stock_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            additional_bq_parameters=ADDITIONAL_BQ_PARAMETERS,
        )
    )

    (
        view_counts
        | "ToBigtableRows" >> beam.ParDo(ToBigtableRow())
        | "WriteViewsToBigtable"
        >> WriteToBigTable(
            project_id=known_args.bigtable_project,
            instance_id=known_args.bigtable_instance,
            table_id=known_args.bigtable_table,
        )
    )


def parse_pipeline_args(argv: Iterable[str] | None = None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_topic_clicks", default=config.TOPIC_CLICKS)
    parser.add_argument("--input_topic_transactions",
                        default=config.TOPIC_TRANSACTIONS)
    parser.add_argument("--input_topic_stock", default=config.TOPIC_STOCK)
    parser.add_argument("--dead_letter_topic", default=config.TOPIC_DLQ)
    parser.add_argument("--output_bigquery_dataset",
                        default=config.DATASET_NAME)
    parser.add_argument("--output_table_views", default=config.VIEWS_TABLE)
    parser.add_argument("--output_table_sales", default=config.SALES_TABLE)
    parser.add_argument("--output_table_stock", default=config.STOCK_TABLE)
    # Provide a default project
    parser.add_argument("--bigtable_project", default="my-gcp-project")
    parser.add_argument("--bigtable_instance", default="ecommerce-instance")
    parser.add_argument("--bigtable_table", default=config.BIGTABLE_TABLE)
    parser.add_argument("--local", action="store_true",
                        help="Run in local dev mode: skip external sinks and emulators")
    return parser.parse_known_args(argv)


def setup_pipeline_options(pipeline_args):
    pipeline_options = PipelineOptions(pipeline_args)
    standard_options = pipeline_options.view_as(StandardOptions)
    standard_options.streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True
    return pipeline_options


def create_and_run_pipeline(pipeline_options, known_args):
    with beam.Pipeline(options=pipeline_options) as pipeline:
        if getattr(known_args, "local", False):
            # Local/dev mode: create empty PCollections rather than reading Pub/Sub
            click_events = pipeline | "CreateClicks" >> beam.Create([])
            transaction_events = pipeline | "CreateTransactions" >> beam.Create([
            ])
            stock_events = pipeline | "CreateStock" >> beam.Create([])
            dead_letters = []
        else:
            click_events, transaction_events, stock_events, dead_letters = build_io(
                pipeline, known_args
            )
        view_counts, sales_counts, stock_counts = build_transforms(
            click_events, transaction_events, stock_events
        )
        if getattr(known_args, "local", False):
            LOGGER.info("Local mode: skipping dead-letter and sink writes")
        else:
            write_dead_letters(dead_letters, known_args.dead_letter_topic)
            write_sinks(view_counts, sales_counts, stock_counts, known_args)


def run(argv: Iterable[str] | None = None) -> None:
    known_args, pipeline_args = parse_pipeline_args(argv)

    # Validate static configuration (will raise early if misconfigured)
    config.validate_configuration()

    # Validate runtime targets supplied via CLI
    config.validate_bigquery_targets(
        known_args.output_bigquery_dataset,
        [
            known_args.output_table_views,
            known_args.output_table_sales,
            known_args.output_table_stock,
        ],
    )
    if not getattr(known_args, "local", False):
        config.validate_bigtable(known_args.bigtable_table,
                                 config.BIGTABLE_COLUMN_FAMILY)
        _validate_topic_paths(
            [
                known_args.input_topic_clicks,
                known_args.input_topic_transactions,
                known_args.input_topic_stock,
                known_args.dead_letter_topic,
            ]
        )
        ensure_bigtable_configuration(
            known_args.bigtable_table, config.BIGTABLE_COLUMN_FAMILY
        )

    pipeline_options = setup_pipeline_options(pipeline_args)
    create_and_run_pipeline(pipeline_options, known_args)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
