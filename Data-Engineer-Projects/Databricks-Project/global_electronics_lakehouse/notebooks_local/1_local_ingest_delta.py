"""Local ingestion script for the Global Electronics Lakehouse demo."""
from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F, types as T


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw CSV/JSON into local Delta tables.")
    parser.add_argument("--data-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "data"), help="Root data directory containing inventory, suppliers, sales_stream")
    parser.add_argument("--schemas-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "resources" / "schemas"), help="Directory with schema JSON definitions")
    parser.add_argument("--output-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_delta"), help="Output folder for Delta tables")
    parser.add_argument("--logs-dir", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs"), help="Directory for quarantined data logs")
    parser.add_argument("--shuffle-partitions", type=int,
                        default=8, help="spark.sql.shuffle.partitions override")
    parser.add_argument("--log-file", default=str(Path(__file__).resolve(
    ).parents[1] / "_logs" / "ingest.log"), help="Log file path")
    return parser.parse_args(argv)


def load_schema(path: Path) -> T.StructType:
    try:
        if not path.exists():
            raise FileNotFoundError(f"Schema file not found: {path}")
        with path.open("r", encoding="utf-8-sig") as handle:
            schema_json = json.load(handle)
        return T.StructType.fromJson(schema_json)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in schema file {path}: {e}")
    except Exception as e:
        raise RuntimeError(f"Failed to load schema from {path}: {e}")


def write_quarantined_data(invalid_df, logs_dir: Path, filename: str, logger) -> int:
    try:
        invalid_rows = [row.asDict() for row in invalid_df.collect()]
        if not invalid_rows:
            return 0
        logs_dir.mkdir(parents=True, exist_ok=True)
        output_file = logs_dir / filename
        with output_file.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=invalid_rows[0].keys())
            writer.writeheader()
            writer.writerows(invalid_rows)
        logger.info(
            f"Logged {len(invalid_rows)} quarantined rows to {output_file}")
        return len(invalid_rows)
    except Exception as e:
        logger.error(
            f"Failed to write quarantined data to {logs_dir / filename}: {e}")
        raise


def build_spark(shuffle_partitions: int) -> SparkSession:
    import platform

    builder = (
        SparkSession.builder.appName("GlobalElectronicsLocalIngest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
    )

    # Windows-specific configurations to prevent hanging
    if platform.system() == "Windows":
        builder = (
            builder
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.hadoop.hadoop.security.authentication", "simple")
            .config("spark.sql.warehouse.dir", "file:///" + str(Path(__file__).resolve().parents[1] / "_delta" / "warehouse").replace("\\", "/"))
            # Disable file locking to prevent Windows permission issues
            .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            # Additional Windows-specific fixes
            .config("spark.hadoop.fs.windows.getfileattributes.extradata", "false")
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
            .config("spark.sql.hive.metastore.sharedPrefixes", "file:///")
        )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def ingest_inventory(spark: SparkSession, data_dir: Path, schema: T.StructType, output_dir: Path, logs_dir: Path, logger) -> int:
    try:
        inventory_path = data_dir / "inventory"
        if not inventory_path.exists():
            raise FileNotFoundError(
                f"Inventory directory not found: {inventory_path}")

        # Use a simpler approach to avoid Windows native library issues
        inventory_files = []
        for date_dir in inventory_path.iterdir():
            if date_dir.is_dir():
                for file in date_dir.iterdir():
                    if file.name.endswith('_inventory.csv'):
                        inventory_files.append(str(file))

        if not inventory_files:
            raise FileNotFoundError(
                f"No inventory CSV files found in {inventory_path}")

        raw_df = (
            spark.read.schema(schema)
            .option("header", "true")
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
            .csv(inventory_files)  # Use explicit file list instead of globbing
            .withColumn("inventory_date", F.to_date("inventory_date"))
            .withColumn("warehouse_code", F.upper(F.col("warehouse_code")))
            .withColumn("product_id", F.trim("product_id"))
            .withColumn("product_name", F.trim("product_name"))
            .withColumn("category", F.trim("category"))
            .withColumn("supplier_id", F.trim("supplier_id"))
            .withColumn("restock_threshold", F.col("restock_threshold").cast("int"))
            .withColumn("restock_lead_days", F.col("restock_lead_days").cast("int"))
            .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(True)))
            .withColumn("last_updated_ts", F.to_timestamp("last_updated_ts"))
            .withColumn("last_sale_ts", F.to_timestamp("last_sale_ts"))
            .withColumn("lifetime_revenue_euro", F.col("lifetime_revenue_euro").cast("double"))
            .withColumn("ingested_at", F.current_timestamp())
        )

        # Data quality constraints: non-negative inventory, valid product_id, warehouse_code
        invalid_df = raw_df.where(
            (F.col("quantity_on_hand") < 0) |
            F.col("product_id").isNull() |
            (F.trim(F.col("product_id")) == "") |
            F.col("warehouse_code").isNull() |
            (F.trim(F.col("warehouse_code")) == "")
        )
        write_quarantined_data(invalid_df, logs_dir,
                               "quarantined_inventory.csv", logger)

        df = raw_df.where(
            (F.col("quantity_on_hand") >= 0) &
            F.col("product_id").isNotNull() &
            (F.trim(F.col("product_id")) != "") &
            F.col("warehouse_code").isNotNull() &
            (F.trim(F.col("warehouse_code")) != "")
        )
        target_path = output_dir / "inventory"
        df.write.format("delta").mode("overwrite").partitionBy(
            "inventory_date").save(str(target_path))
        spark.sql(
            f"OPTIMIZE '{target_path}' ZORDER BY (warehouse_code, product_id)")
        count = df.count()
        logger.info(f"Inventory rows written: {count:,}")
        return count
    except Exception as e:
        logger.error(f"Failed to ingest inventory: {e}")
        raise


def ingest_suppliers(spark: SparkSession, data_dir: Path, schema: T.StructType, output_dir: Path, logs_dir: Path, logger) -> int:
    try:
        suppliers_path = data_dir / "suppliers" / "suppliers_master.jsonl"
        if not suppliers_path.exists():
            raise FileNotFoundError(
                f"Suppliers file not found: {suppliers_path}")

        raw_df = (
            spark.read.schema(schema)
            .json(suppliers_path.as_posix())
            .withColumn("supplier_id", F.trim("supplier_id"))
            .withColumn("supplier_name", F.trim("supplier_name"))
            .withColumn("contact_email", F.lower(F.trim("contact_email")))
            .withColumn("preferred", F.coalesce("preferred", F.lit(False)))
            .withColumn("ingested_at", F.current_timestamp())
        )

        # Data quality constraints: valid supplier_id
        invalid_df = raw_df.where(
            F.col("supplier_id").isNull() |
            (F.trim(F.col("supplier_id")) == "")
        )
        write_quarantined_data(invalid_df, logs_dir,
                               "quarantined_suppliers.csv", logger)

        df = raw_df.where(
            F.col("supplier_id").isNotNull() &
            (F.trim(F.col("supplier_id")) != "")
        )
        target_path = output_dir / "suppliers"
        df.write.format("delta").mode("overwrite").save(str(target_path))
        spark.sql(f"OPTIMIZE '{target_path}' ZORDER BY (supplier_id)")
        count = df.count()
        logger.info(f"Suppliers rows written: {count:,}")
        return count
    except Exception as e:
        logger.error(f"Failed to ingest suppliers: {e}")
        raise


def ingest_sales(spark: SparkSession, data_dir: Path, schema: T.StructType, output_dir: Path, logs_dir: Path, logger) -> int:
    try:
        sales_path = data_dir / "sales_stream"
        if not sales_path.exists():
            raise FileNotFoundError(f"Sales directory not found: {sales_path}")

        raw_df = (
            spark.read.schema(schema)
            .json(str(sales_path / "sales_*.json"))
            .withColumn("event_time", F.to_timestamp("event_time"))
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("warehouse_code", F.upper(F.col("warehouse_code")))
            .withColumn("sale_amount_euro", (
                F.col("quantity") * F.col("unit_price_euro") *
                (1 - F.coalesce(F.col("discount_rate"), F.lit(0.0)))
            ).cast("double"))
            .withColumn("ingested_at", F.current_timestamp())
        )

        # Data quality constraints: positive quantity, valid product_id, warehouse_code
        invalid_df = raw_df.where(
            (F.col("quantity") <= 0) |
            F.col("product_id").isNull() |
            (F.trim(F.col("product_id")) == "") |
            F.col("warehouse_code").isNull() |
            (F.trim(F.col("warehouse_code")) == "")
        )
        write_quarantined_data(invalid_df, logs_dir,
                               "quarantined_sales.csv", logger)

        df = raw_df.where(
            (F.col("quantity") > 0) &
            F.col("product_id").isNotNull() &
            (F.trim(F.col("product_id")) != "") &
            F.col("warehouse_code").isNotNull() &
            (F.trim(F.col("warehouse_code")) != "")
        )
        target_path = output_dir / "sales"
        df.write.format("delta").mode("overwrite").partitionBy(
            "event_date").save(str(target_path))
        spark.sql(
            f"OPTIMIZE '{target_path}' ZORDER BY (warehouse_code, product_id)")
        count = df.count()
        logger.info(f"Sales events written: {count:,}")
        return count
    except Exception as e:
        logger.error(f"Failed to ingest sales: {e}")
        raise


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=args.log_file)
    logger = logging.getLogger(__name__)

    try:
        data_dir = Path(args.data_dir).resolve()
        if not data_dir.exists():
            raise FileNotFoundError(
                f"Data directory does not exist: {data_dir}")

        schemas_dir = Path(args.schemas_dir)
        if not schemas_dir.exists():
            raise FileNotFoundError(
                f"Schemas directory does not exist: {schemas_dir}")

        output_dir = Path(args.output_dir)
        logs_dir = Path(args.logs_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        logs_dir.mkdir(parents=True, exist_ok=True)

        spark = build_spark(args.shuffle_partitions)

        start_time = time.time()
        try:
            inventory_schema = load_schema(
                schemas_dir / "inventory_schema.json")
            suppliers_schema = load_schema(
                schemas_dir / "suppliers_schema.json")
            sales_schema = load_schema(schemas_dir / "sales_schema.json")

            ingest_inventory(spark, data_dir, inventory_schema,
                             output_dir, logs_dir, logger)
            ingest_suppliers(spark, data_dir, suppliers_schema,
                             output_dir, logs_dir, logger)
            ingest_sales(spark, data_dir, sales_schema,
                         output_dir, logs_dir, logger)
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            raise
        finally:
            spark.stop()
        end_time = time.time()
        logger.info(
            f"Total ingestion time: {end_time - start_time:.2f} seconds")
        return 0
    except Exception as e:
        logger.error(f"Script execution failed: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
