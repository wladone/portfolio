"""Update inventory Delta table with sales deductions and log anomalies."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply sales deductions to local inventory Delta table.")
    parser.add_argument("--delta-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_delta"), help="Base directory containing inventory and sales Delta tables")
    parser.add_argument("--logs-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs"), help="Directory for generated log files")
    parser.add_argument("--shuffle-partitions", type=int,
                        default=4, help="spark.sql.shuffle.partitions override")
    parser.add_argument("--days", type=int, default=7,
                        help="Number of days of sales to apply (0 for all time)")
    parser.add_argument("--log-file", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs" / "update_stocks.log"), help="Log file path")
    return parser.parse_args(argv)


def build_spark(shuffle_partitions: int) -> SparkSession:
    builder = (
        SparkSession.builder.appName("GlobalElectronicsUpdateStocks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.io.native.lib.available", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ensure_dirs(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_invalid_sales(invalid_df, logs_dir: Path, logger) -> int:
    try:
        invalid_rows = [row.asDict() for row in invalid_df.collect()]
        if not invalid_rows:
            return 0
        output_file = logs_dir / "invalid_sales.csv"
        with output_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=invalid_rows[0].keys())
            writer.writeheader()
            writer.writerows(invalid_rows)
        logger.info(
            f"Logged {len(invalid_rows)} invalid sales rows to {output_file}")
        return len(invalid_rows)
    except Exception as e:
        logger.error(f"Failed to write invalid sales data: {e}")
        raise


def record_versions(
    logs_dir: Path,
    previous_version: int,
    current_version: int,
    days_applied: int,
    invalid_rows: int,
    valid_rows: int,
    logger,
) -> None:
    try:
        metadata_file = logs_dir / "inventory_versions.json"
        payload = {
            "previous_version": previous_version,
            "current_version": current_version,
            "days_applied": days_applied,
            "invalid_rows": invalid_rows,
            "valid_rows": valid_rows,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        with metadata_file.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        logger.info(f"Inventory versions recorded in {metadata_file}")
    except Exception as e:
        logger.error(f"Failed to record versions: {e}")
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
        ensure_dirs(logs_dir)

        spark = build_spark(args.shuffle_partitions)

        start_time = time.time()
        try:
            inventory_path = (delta_dir / "inventory").as_posix()
            sales_path = (delta_dir / "sales").as_posix()

            if not Path(inventory_path).exists() or not Path(sales_path).exists():
                raise FileNotFoundError(
                    "Inventory or sales Delta tables are missing. Run 1_local_ingest_delta.py first.")

            inventory = DeltaTable.forPath(spark, inventory_path)
            sales_df = spark.read.format("delta").load(sales_path)

            if args.days > 0:
                cutoff = F.date_sub(F.current_date(), args.days)
                sales_df = sales_df.where(F.col("event_date") >= cutoff)
            else:
                cutoff = None

            existing_products = inventory.toDF().select(
                "warehouse_code", "product_id").distinct()

            aggregated_sales = (
                sales_df.groupBy("warehouse_code", "product_id")
                .agg(
                    F.sum("quantity").alias("quantity_sold"),
                    F.sum("sale_amount_euro").alias("revenue_euro"),
                    F.max("event_time").alias("last_sale_ts"),
                )
                .cache()
            )

            invalid_sales = aggregated_sales.join(
                existing_products, ["warehouse_code", "product_id"], "left_anti")
            invalid_rows = write_invalid_sales(invalid_sales, logs_dir, logger)

            # Additional constraint: positive quantities
            valid_sales_temp = aggregated_sales.join(
                existing_products, ["warehouse_code", "product_id"], "inner")
            invalid_quantity_sales = valid_sales_temp.where(
                F.col("quantity_sold") <= 0)
            invalid_quantity_count = write_invalid_sales(
                invalid_quantity_sales, logs_dir, logger)

            valid_sales = valid_sales_temp.where(F.col("quantity_sold") > 0)
            valid_rows = valid_sales.count()

            if valid_rows == 0:
                logger.info("No valid sales to apply. Inventory not updated.")
                return 0

            history_before = inventory.history().select(
                "version").orderBy("version", ascending=False).first()
            previous_version = history_before[0] if history_before else 0

            inventory.alias("target").merge(
                valid_sales.alias("source"),
                "target.warehouse_code = source.warehouse_code AND target.product_id = source.product_id",
            ).whenMatchedUpdate(
                set={
                    "quantity_on_hand": F.greatest(F.col("target.quantity_on_hand") - F.col("source.quantity_sold"), F.lit(0)),
                    "last_updated_ts": F.current_timestamp(),
                    "last_sale_ts": F.col("source.last_sale_ts"),
                    "lifetime_revenue_euro": (
                        F.coalesce(F.col("target.lifetime_revenue_euro"),
                                   F.lit(0.0)) + F.col("source.revenue_euro")
                    ).cast("double"),
                }
            ).execute()

            history_after = inventory.history().select(
                "version").orderBy("version", ascending=False).first()
            current_version = history_after[0] if history_after else previous_version

            record_versions(logs_dir, previous_version, current_version,
                            args.days, invalid_rows + invalid_quantity_count, valid_rows, logger)

            negatives = inventory.toDF().where("quantity_on_hand < 0")
            if not negatives.rdd.isEmpty():
                count = negatives.count()
                logger.warning(
                    f"Warning: {count} inventory rows have negative stock after merge.")
            else:
                logger.info(
                    "Inventory merge successful with no negative stock.")

            if cutoff is not None:
                logger.info(
                    f"Sales applied from {args.days} trailing days (cutoff >= {cutoff}).")

        except Exception as e:
            logger.error(f"Update stocks failed: {e}")
            raise
        finally:
            spark.stop()
        end_time = time.time()
        logger.info(f"Total update time: {end_time - start_time:.2f} seconds")
        return 0
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
