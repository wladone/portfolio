"""Local analytics and validation script for the Global Electronics Lakehouse demo."""
from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run local analytics and validations on Delta outputs.")
    parser.add_argument("--delta-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_delta"), help="Directory containing Delta tables")
    parser.add_argument("--logs-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs"), help="Directory containing merge metadata")
    parser.add_argument("--reports-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_reports"), help="Optional output directory for CSV reports")
    parser.add_argument("--top-n", type=int, default=25,
                        help="Number of top products to show")
    parser.add_argument("--hours", type=int, default=24,
                        help="Lookback window (hours) for sales trend")
    parser.add_argument("--skip-validation",
                        action="store_true", help="Skip invariant checks")
    parser.add_argument("--shuffle-partitions", type=int,
                        default=8, help="spark.sql.shuffle.partitions override")
    parser.add_argument("--log-file", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs" / "analysis.log"), help="Log file path")
    return parser.parse_args(argv)


def build_spark(shuffle_partitions: int) -> SparkSession:
    builder = (
        SparkSession.builder.appName("GlobalElectronicsLocalAnalysis")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def maybe_write(df, path: Path, name: str, logger) -> None:
    try:
        path.mkdir(parents=True, exist_ok=True)
        output_file = path / f"{name}.csv"
        df.coalesce(1).write.mode("overwrite").option(
            "header", "true").csv(str(output_file))
        logger.info(f"Report exported to {output_file}")
    except Exception as e:
        logger.error(f"Failed to write report {name}: {e}")
        raise


def load_metadata(logs_dir: Path, logger) -> dict | None:
    try:
        metadata_file = logs_dir / "inventory_versions.json"
        if not metadata_file.exists():
            logger.warning(
                "Merge metadata not found; run 2_local_update_stocks.py first.")
            return None
        with metadata_file.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in metadata file: {e}")
        return None
    except Exception as e:
        logger.error(f"Failed to load metadata: {e}")
        return None


def run_validation(
    spark: SparkSession,
    delta_dir: Path,
    metadata: dict,
    hours: int,
    logger,
) -> None:
    try:
        inventory_path = (delta_dir / "inventory").as_posix()
        sales_path = (delta_dir / "sales").as_posix()

        previous_version = metadata.get("previous_version")
        current_version = metadata.get("current_version")
        days_applied = metadata.get("days_applied", 0)

        if previous_version is None or current_version is None:
            logger.warning(
                "Metadata missing version numbers; skipping validation.")
            return

        inventory_current = spark.read.format("delta").load(inventory_path)
        inventory_previous = spark.read.format("delta").option(
            "versionAsOf", int(previous_version)).load(inventory_path)
        sales_df = spark.read.format("delta").load(sales_path)

        if days_applied and days_applied > 0:
            sales_window = sales_df.where(F.col("event_date") >= F.date_sub(
                F.current_date(), int(days_applied)))
        else:
            cutoff = F.expr(f"current_timestamp() - interval {hours} hours")
            sales_window = sales_df.where(F.col("event_time") >= cutoff)

        sales_agg = sales_window.groupBy("warehouse_code", "product_id").agg(
            F.sum("quantity").alias("quantity_sold"))

        joined = (
            inventory_previous.select("warehouse_code", "product_id", "quantity_on_hand").withColumnRenamed(
                "quantity_on_hand", "previous_qty")
            .join(
                inventory_current.select(
                    "warehouse_code", "product_id", "quantity_on_hand"),
                on=["warehouse_code", "product_id"],
                how="inner",
            )
            .withColumnRenamed("quantity_on_hand", "current_qty")
            .join(sales_agg, ["warehouse_code", "product_id"], "left")
            .fillna({"quantity_sold": 0})
            .withColumn("expected_qty", F.col("previous_qty") - F.col("quantity_sold"))
            .withColumn("is_consistent", F.col("expected_qty") == F.col("current_qty"))
        )

        sample = joined.orderBy(F.rand()).limit(100)
        inconsistent = sample.where(~F.col("is_consistent")).count()
        if inconsistent == 0:
            logger.info(
                "Validation passed: sampled rows maintain stock consistency.")
        else:
            logger.error(
                f"Validation failed: {inconsistent} sampled rows have inconsistent stock values.")

        negative = inventory_current.where(F.col("quantity_on_hand") < 0)
        neg_count = negative.count()
        if neg_count == 0:
            logger.info("Validation passed: no negative stock detected.")
        else:
            logger.error(
                f"Validation failed: {neg_count} rows have negative stock.")
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        raise


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=args.log_file)
    logger = logging.getLogger(__name__)

    try:
        delta_dir = Path(args.delta_dir)
        if not delta_dir.exists():
            raise FileNotFoundError(
                f"Delta directory does not exist: {delta_dir}")

        logs_dir = Path(args.logs_dir)
        reports_dir = Path(args.reports_dir) if args.reports_dir else None

        spark = build_spark(args.shuffle_partitions)

        start_time = time.time()
        try:
            inventory_path = (delta_dir / "inventory").as_posix()
            sales_path = (delta_dir / "sales").as_posix()

            if not Path(inventory_path).exists() or not Path(sales_path).exists():
                raise FileNotFoundError(
                    "Inventory or sales Delta tables are missing.")

            inventory_df = spark.read.format("delta").load(inventory_path)
            sales_df = spark.read.format("delta").load(sales_path)

            lookback_cutoff = F.expr(
                f"current_timestamp() - interval {args.hours} hours")
            recent_sales = sales_df.where(
                F.col("event_time") >= lookback_cutoff)

            top_products = (
                recent_sales.groupBy("product_id", "warehouse_code")
                .agg(
                    F.sum("quantity").alias("units_sold"),
                    F.sum("sale_amount_euro").alias("revenue_euro"),
                )
                .orderBy(F.col("units_sold").desc())
                .limit(args.top_n)
            )

            low_stock = (
                inventory_df.where(F.col("quantity_on_hand") <=
                                   F.col("restock_threshold"))
                .orderBy("quantity_on_hand")
                .select("warehouse_code", "product_id", "product_name", "quantity_on_hand", "restock_threshold")
            )

            revenue_per_hour = (
                recent_sales.groupBy(
                    F.window("event_time", "1 hour").alias("window"),
                    "warehouse_code",
                )
                .agg(
                    F.sum("sale_amount_euro").alias("revenue_euro"),
                    F.sum("quantity").alias("units"),
                )
                .select(
                    F.col("window.start").alias("window_start"),
                    F.col("window.end").alias("window_end"),
                    "warehouse_code",
                    "revenue_euro",
                    "units",
                )
                .orderBy("window_start", "warehouse_code")
            )

            logger.info("=== Top Products ===")
            top_products.show(truncate=False)
            logger.info("=== Low Stock Alerts ===")
            low_stock.show(truncate=False)
            logger.info("=== Revenue Per Hour ===")
            revenue_per_hour.show(truncate=False)

            if reports_dir:
                maybe_write(top_products, reports_dir, "top_products", logger)
                maybe_write(low_stock, reports_dir, "low_stock", logger)
                maybe_write(revenue_per_hour, reports_dir,
                            "revenue_per_hour", logger)

            if not args.skip_validation:
                metadata = load_metadata(logs_dir, logger)
                if metadata:
                    run_validation(spark, delta_dir, metadata,
                                   args.hours, logger)

        except Exception as e:
            logger.error(f"Analysis failed: {e}")
            raise
        finally:
            spark.stop()
        end_time = time.time()
        logger.info(
            f"Total analysis time: {end_time - start_time:.2f} seconds")
        return 0
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
