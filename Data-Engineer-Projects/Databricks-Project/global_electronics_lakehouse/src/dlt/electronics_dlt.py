"""Delta Live Tables PySpark pipeline for the Global Electronics Lakehouse."""
import dlt
import logging
import time
import functools
from typing import Callable, Any
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window


WAREHOUSE_COUNTRY_LOOKUP = {
    "FRA": "France",
    "BUC": "Romania",
    "MAD": "Spain",
}

inventory_schema = T.StructType([
    T.StructField("inventory_date", T.StringType(), True),
    T.StructField("warehouse_code", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("product_name", T.StringType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("quantity_on_hand", T.DoubleType(), True),
    T.StructField("unit_cost_euro", T.DoubleType(), True),
])

suppliers_schema = T.StructType([
    T.StructField("supplier_id", T.StringType(), True),
    T.StructField("supplier_name", T.StringType(), True),
    T.StructField("rating", T.DoubleType(), True),
    T.StructField("lead_time_days", T.IntegerType(), True),
    T.StructField("preferred", T.BooleanType(), True),
    T.StructField("contact_email", T.StringType(), True),
    T.StructField("category_focus", T.ArrayType(T.StringType()), True),
])

sales_schema = T.StructType([
    T.StructField("event_id", T.StringType(), True),
    T.StructField("event_time", T.StringType(), True),
    T.StructField("order_id", T.StringType(), True),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("warehouse_code", T.StringType(), True),
    T.StructField("channel", T.StringType(), True),
    T.StructField("payment_type", T.StringType(), True),
    T.StructField("customer_id", T.StringType(), True),
    T.StructField("quantity", T.IntegerType(), True),
    T.StructField("unit_price_euro", T.DoubleType(), True),
    T.StructField("discount_rate", T.DoubleType(), True),
])


def validate_config() -> None:
    """Validate required pipeline configuration at startup."""
    logger = logging.getLogger(__name__)
    required_configs = [
        "pipelines.electronics.source_inventory",
        "pipelines.electronics.source_suppliers",
        "pipelines.electronics.kafka_bootstrap",
        "pipelines.electronics.kafka_topic"
    ]

    logger.info("Validating pipeline configuration...")

    for config_key in required_configs:
        try:
            value = spark.conf.get(config_key)
            if not value or not value.strip():
                raise ValueError(
                    f"Configuration '{config_key}' is missing or empty")
            logger.debug(f"Config '{config_key}': {value}")
        except Exception as e:
            logger.error(
                f"Configuration validation failed for '{config_key}': {e}")
            raise ValueError(f"Invalid configuration: {e}")

    # Validate Kafka bootstrap format (basic check)
    kafka_bootstrap = _config("pipelines.electronics.kafka_bootstrap")
    if ":" not in kafka_bootstrap:
        raise ValueError(f"Invalid Kafka bootstrap format: {kafka_bootstrap}")

    logger.info("Pipeline configuration validation completed successfully")


def retry_on_failure(max_retries: int = 3, backoff_factor: float = 2.0, exceptions: tuple = (Exception,)):
    """Decorator to retry table functions on failure with exponential backoff."""
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            logger = logging.getLogger(__name__)
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = backoff_factor ** attempt
                        logger.warning(
                            f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"All {max_retries + 1} attempts failed for {func.__name__}: {e}")
                        raise e
            raise last_exception
        return wrapper
    return decorator


def alert_on_expect_violation(table_name: str, expectation_name: str, violation_count: int) -> None:
    """Log alerts for expectation violations."""
    logger = logging.getLogger(__name__)
    if violation_count > 0:
        alert_msg = f"ALERT: Expectation violation in table '{table_name}': '{expectation_name}' failed {violation_count} times"
        logger.error(alert_msg)
        # Structured log for monitoring systems
        logger.info(
            f"STRUCTURED_ALERT: {{\"type\": \"expectation_violation\", \"table\": \"{table_name}\", \"expectation\": \"{expectation_name}\", \"violations\": {violation_count}}}")


# Validate config at pipeline startup
validate_config()


def _config(key: str, default: str | None = None) -> str:
    """Helper to read pipeline configuration with an optional default."""
    logger = logging.getLogger(__name__)
    try:
        value = spark.conf.get(key)
        if not value:
            raise ValueError(f"Configuration key '{key}' is empty")
        return value
    except Exception as e:
        if default is not None:
            logger.warning(
                f"Using default value for config '{key}': {default} (error: {e})")
            return default
        logger.error(f"Failed to get config '{key}': {e}")
        raise


def _warehouse_country(col_name: str) -> F.Column:
    mapping_entries: list[F.Column] = []
    for code, country in WAREHOUSE_COUNTRY_LOOKUP.items():
        mapping_entries.extend([F.lit(code), F.lit(country)])
    return F.coalesce(F.create_map(*mapping_entries).getItem(F.col(col_name)), F.lit("Unknown"))


@dlt.table(comment="Daily inventory snapshots ingested with Auto Loader (CSV).")
@retry_on_failure(max_retries=3, backoff_factor=1.5)
def bronze_inventory():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("cloudFiles.useIncrementalListing", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "inventory_date STRING, warehouse_code STRING, product_id STRING, product_name STRING, "
            "category STRING, quantity_on_hand DOUBLE, unit_cost_euro DOUBLE",
        )
        .schema(inventory_schema)
        .load(_config("pipelines.electronics.source_inventory", "/mnt/landing/electronics/inventory"))
        .select(
            F.to_date("inventory_date").alias("inventory_date"),
            F.upper(F.trim("warehouse_code")).alias("warehouse_code"),
            F.trim("product_id").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.trim("category").alias("category"),
            F.col("quantity_on_hand").cast("int").alias("quantity_on_hand"),
            F.col("unit_cost_euro").cast(
                "decimal(10,2)").alias("unit_cost_euro"),
            F.input_file_name().alias("input_file"),
            F.current_timestamp().alias("ingestion_ts"),
        )
    )


@dlt.table(comment="Supplier master data ingested from JSON lines via Auto Loader.")
@retry_on_failure(max_retries=3, backoff_factor=1.5)
def bronze_suppliers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "false")
        .option("cloudFiles.useIncrementalListing", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "supplier_id STRING, supplier_name STRING, rating DOUBLE, lead_time_days INT, "
            "preferred BOOLEAN, contact_email STRING, category_focus ARRAY<STRING>",
        )
        .schema(suppliers_schema)
        .load(_config("pipelines.electronics.source_suppliers", "/mnt/landing/electronics/suppliers"))
        .select(
            F.trim("supplier_id").alias("supplier_id"),
            F.trim("supplier_name").alias("supplier_name"),
            F.col("rating").cast("double").alias("rating"),
            F.col("lead_time_days").cast("int").alias("lead_time_days"),
            F.col("preferred").alias("preferred"),
            F.lower(F.trim("contact_email")).alias("contact_email"),
            F.col("category_focus").alias("category_focus"),
            F.input_file_name().alias("input_file"),
            F.current_timestamp().alias("ingestion_ts"),
        )
    )


@dlt.table(comment="Raw sales payloads streamed from Kafka.")
@retry_on_failure(max_retries=3, backoff_factor=1.5)
def bronze_sales_stream():
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", _config("pipelines.electronics.kafka_bootstrap", "localhost:9092"))
        .option("subscribe", _config("pipelines.electronics.kafka_topic", "global_electronics_sales"))
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("includeHeaders", "true")
        .load()
        .selectExpr(
            "CAST(value AS STRING) AS raw_value",
            "CAST(key AS STRING) AS message_key",
            "headers",
            "timestamp AS kafka_timestamp",
        )
        .withColumn("ingestion_ts", F.current_timestamp())
    )


@dlt.table(comment="Parsed sales events with normalized schema.")
@retry_on_failure(max_retries=3, backoff_factor=1.5)
def bronze_sales_parsed():
    return (
        dlt.read_stream("bronze_sales_stream")
        .select(
            F.from_json("raw_value", sales_schema).alias("payload"),
            "message_key",
            "kafka_timestamp",
            "headers",
            "ingestion_ts",
        )
        .select(
            F.col("payload.event_id").alias("event_id"),
            F.col("payload.order_id").alias("order_id"),
            F.col("payload.product_id").alias("product_id"),
            F.upper(F.trim(F.col("payload.warehouse_code"))
                    ).alias("warehouse_code"),
            F.trim(F.col("payload.channel")).alias("channel"),
            F.trim(F.col("payload.payment_type")).alias("payment_type"),
            F.trim(F.col("payload.customer_id")).alias("customer_id"),
            F.col("payload.quantity").cast("int").alias("quantity"),
            F.col("payload.unit_price_euro").cast(
                "decimal(10,2)").alias("unit_price_euro"),
            F.coalesce(F.col("payload.discount_rate"),
                       F.lit(0.0)).alias("discount_rate"),
            F.to_timestamp("payload.event_time").alias("event_time"),
            F.to_date(F.to_timestamp("payload.event_time")
                      ).alias("event_date"),
            "message_key",
            "kafka_timestamp",
            "ingestion_ts",
            (F.col("payload.quantity") * F.col("payload.unit_price_euro") * (1 - F.coalesce(F.col(
                "payload.discount_rate"), F.lit(0.0)))).cast("decimal(12,2)").alias("sale_amount_euro"),
        )
        .filter((F.col("order_id").isNotNull()) & (F.col("product_id").isNotNull()))
    )


@dlt.table(comment="Cleaned inventory snapshots with enforced constraints.")
@dlt.expect_or_fail("non_negative_inventory", "quantity_on_hand >= 0")
def silver_inventory():
    df = (
        dlt.read("bronze_inventory")
        .select(
            "inventory_date",
            "warehouse_code",
            "product_id",
            "product_name",
            "category",
            F.col("quantity_on_hand").cast("int").alias("quantity_on_hand"),
            F.col("unit_cost_euro").cast(
                "decimal(10,2)").alias("unit_cost_euro"),
            "ingestion_ts",
        )
    )
    # Check for violations and alert
    negative_count = df.filter(F.col("quantity_on_hand") < 0).count()
    alert_on_expect_violation(
        "silver_inventory", "non_negative_inventory", negative_count)
    return df


@dlt.table(comment="Validated supplier dimension with rating expectation.")
@dlt.expect_or_drop("rating_between_zero_and_five", "rating BETWEEN 0 AND 5")
def silver_suppliers():
    df = dlt.read("bronze_suppliers")
    # Check for violations and alert
    invalid_rating_count = df.filter(
        (F.col("rating") < 0) | (F.col("rating") > 5)).count()
    alert_on_expect_violation(
        "silver_suppliers", "rating_between_zero_and_five", invalid_rating_count)
    return df


@dlt.table(comment="Product attributes derived from inventory history.")
def silver_product_reference():
    return (
        dlt.read("silver_inventory")
        .groupBy("product_id")
        .agg(
            F.max_by("product_name", "inventory_date").alias("product_name"),
            F.max_by("category", "inventory_date").alias("category"),
            F.avg("unit_cost_euro").alias("avg_unit_cost_euro"),
        )
    )


@dlt.table(comment="Deduplicated sales events with quality rules and watermarking.")
@dlt.expect_or_drop("quantity_positive", "quantity > 0")
@dlt.expect_or_drop("amount_positive", "sale_amount_euro > 0")
def silver_sales_cleaned():
    window_spec = Window.partitionBy("order_id", "product_id", "warehouse_code").orderBy(
        F.col("event_time").desc(), F.col("ingestion_ts").desc()
    )
    df = (
        dlt.read_stream("bronze_sales_parsed")
        .withWatermark("event_time", "2 hours")
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
    # Check for violations and alert
    negative_quantity_count = df.filter(F.col("quantity") <= 0).count()
    alert_on_expect_violation("silver_sales_cleaned",
                              "quantity_positive", negative_quantity_count)
    negative_amount_count = df.filter(F.col("sale_amount_euro") <= 0).count()
    alert_on_expect_violation("silver_sales_cleaned",
                              "amount_positive", negative_amount_count)
    return df


@dlt.table(comment="Sales facts enriched with product master data.")
@dlt.expect_or_drop("product_known", "product_name IS NOT NULL")
def silver_sales_enriched():
    products = dlt.read("silver_product_reference")
    mapping_entries: list[F.Column] = []
    for code, country in WAREHOUSE_COUNTRY_LOOKUP.items():
        mapping_entries.extend([F.lit(code), F.lit(country)])
    warehouse_lookup = F.create_map(*mapping_entries)
    df = (
        dlt.read_stream("silver_sales_cleaned")
        .join(products, on="product_id", how="left")
        .withColumn("warehouse_country", F.coalesce(warehouse_lookup.getItem(F.col("warehouse_code")), F.lit("Unknown")))
        .withColumn(
            "margin_euro",
            (F.col("sale_amount_euro") - F.col("quantity") *
             F.col("avg_unit_cost_euro")).cast("decimal(12,2)"),
        )
    )
    # Check for violations and alert
    unknown_product_count = df.filter(F.col("product_name").isNull()).count()
    alert_on_expect_violation("silver_sales_enriched",
                              "product_known", unknown_product_count)
    return df


@dlt.table(comment="Sales events referencing unknown products for observability.")
def silver_sales_invalid_products():
    products = dlt.read("silver_product_reference")
    return (
        dlt.read_stream("silver_sales_cleaned")
        .join(products, on="product_id", how="left")
        .filter(F.col("product_name").isNull())
    )


@dlt.table(comment="Latest inventory levels per product across warehouses.")
def gold_inventory_levels():
    latest_window = Window.partitionBy(
        "warehouse_code", "product_id").orderBy(F.col("inventory_date").desc())
    latest = (
        dlt.read("silver_inventory")
        .withColumn("rn", F.row_number().over(latest_window))
        .filter(F.col("rn") == 1)
    )
    return (
        latest.groupBy("product_id")
        .agg(
            F.max_by("product_name", "inventory_date").alias("product_name"),
            F.max_by("category", "inventory_date").alias("category"),
            F.sum("quantity_on_hand").alias("total_quantity_on_hand"),
            F.sum(F.col("quantity_on_hand") * F.col("unit_cost_euro")
                  ).alias("stock_value_euro"),
            F.max("inventory_date").alias("as_of_date"),
            F.collect_list(F.struct("warehouse_code", "quantity_on_hand")).alias(
                "warehouse_breakdown"),
        )
    )


@dlt.table(comment="Hourly revenue and quantity per warehouse.")
def gold_sales_performance():
    return (
        dlt.read_stream("silver_sales_enriched")
        .withWatermark("event_time", "26 hours")
        .groupBy(
            "warehouse_code",
            "warehouse_country",
            F.window("event_time", "1 hour", "5 minutes").alias("window"),
        )
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("sale_amount_euro").alias("revenue_euro"),
            F.sum("margin_euro").alias("margin_euro"),
            F.countDistinct("order_id").alias("distinct_orders"),
        )
        .select(
            "warehouse_code",
            "warehouse_country",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "total_quantity",
            "revenue_euro",
            "margin_euro",
            "distinct_orders",
        )
    )


@dlt.table(comment="IQR-based anomaly detection over trailing 24 hours.")
def gold_order_anomalies():
    sales = (
        dlt.read_stream("silver_sales_enriched")
        .withWatermark("event_time", "26 hours")
        .groupBy("order_id", "warehouse_code", "warehouse_country")
        .agg(
            F.max("event_time").alias("order_timestamp"),
            F.sum("sale_amount_euro").alias("order_total_euro"),
            F.sum("quantity").alias("total_quantity"),
        )
    )

    order_with_window = sales.select(
        "order_id",
        "warehouse_code",
        "warehouse_country",
        "order_timestamp",
        "order_total_euro",
        "total_quantity",
        F.window("order_timestamp", "24 hours",
                 "1 hour").alias("stats_window"),
    )

    stats = (
        order_with_window.groupBy(
            "warehouse_code", "warehouse_country", "stats_window")
        .agg(
            F.expr("percentile_approx(order_total_euro, 0.25, 100)").alias("q1"),
            F.expr("percentile_approx(order_total_euro, 0.75, 100)").alias("q3"),
        )
        .withColumn("iqr", F.col("q3") - F.col("q1"))
    )

    return (
        order_with_window.join(
            stats,
            on=["warehouse_code", "warehouse_country", "stats_window"],
            how="inner",
        )
        .withColumn(
            "is_anomalous",
            F.when(F.col("iqr") == 0, F.lit(False))
            .when(F.col("order_total_euro") < F.col("q1") - 1.5 * F.col("iqr"), F.lit(True))
            .when(F.col("order_total_euro") > F.col("q3") + 1.5 * F.col("iqr"), F.lit(True))
            .otherwise(F.lit(False)),
        )
        .select(
            "order_id",
            "warehouse_code",
            "warehouse_country",
            F.col("order_timestamp").alias("event_time"),
            "order_total_euro",
            "total_quantity",
            F.col("stats_window.start").alias("window_start"),
            F.col("stats_window.end").alias("window_end"),
            "q1",
            "q3",
            "iqr",
            "is_anomalous",
        )
    )
