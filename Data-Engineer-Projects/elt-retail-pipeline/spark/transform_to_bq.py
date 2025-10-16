#!/usr/bin/env python
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, round, sum as _sum, countDistinct

# ENV
STAGING_DIR = os.getenv("STAGING_DIR", "staging")
GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET = os.getenv("BQ_DATASET", "retail_db")
GCS_TEMP_BUCKET = os.getenv("GCS_TEMP_BUCKET", "")


def main():
    spark = (
        SparkSession.builder
        .appName("RetailPipelineTransformToBQ")
        .getOrCreate()
    )

    if not GCS_TEMP_BUCKET:
        raise RuntimeError("GCS_TEMP_BUCKET env var is required for BigQuery connector staging.")
    spark.conf.set("temporaryGcsBucket", GCS_TEMP_BUCKET)
    if GCP_PROJECT:
        spark.conf.set("parentProject", GCP_PROJECT)

    # Read staging files
    produse_path = os.path.join(STAGING_DIR, "produse.json")
    vanzari_path = os.path.join(STAGING_DIR, "vanzari.csv")
    clienti_path = os.path.join(STAGING_DIR, "clienti.csv")

    produse_df = spark.read.json(produse_path)
    vanzari_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(vanzari_path)
    )
    clienti_df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(clienti_path)
    )

    # Normalize column names
    if "Data" in vanzari_df.columns:
        vanzari_df = vanzari_df.withColumn("Data", to_date("Data"))
    if "Preț_unitar" in vanzari_df.columns:
        vanzari_df = vanzari_df.withColumnRenamed("Preț_unitar","Pret_unitar")

    # Join sales with products
    vprod = vanzari_df.join(produse_df, vanzari_df["ID_produs"] == produse_df["id"], "left").drop(produse_df["id"])
    # Join with customers
    complet = vprod.join(clienti_df, on="ID_client", how="left")

    # Derive totals if missing
    if "Total_vanzare" not in complet.columns and "Cantitate" in complet.columns and "price" in complet.columns:
        complet = complet.withColumn("Total_vanzare", round(col("Cantitate") * col("price"), 2))

    # Write dim tables
    produse_bq = f"{BQ_DATASET}.Products"
    clienti_bq = f"{BQ_DATASET}.Customers"
    sales_bq = f"{BQ_DATASET}.Sales"

    produse_df.select("id","title","price","category").write.format("bigquery").mode("overwrite").option("table", produse_bq).save()
    clienti_df.write.format("bigquery").mode("overwrite").option("table", clienti_bq).save()

    keep_cols = [c for c in ["ID_vanzare","Data","ID_produs","ID_client","Cantitate","Total_vanzare","title","category","price"] if c in complet.columns]
    sales = complet.select(*keep_cols)
    sales.write.format("bigquery").mode("overwrite").option("table", sales_bq).save()

    # Simple QC
    total = sales.agg(_sum("Total_vanzare").alias("total")).collect()[0]["total"]
    distinct_clients = sales.agg(countDistinct("ID_client").alias("clients")).collect()[0]["clients"]
    print(f"[QC] Sum(Total_vanzare)={total} | Distinct clients={distinct_clients}")

    spark.stop()


if __name__ == "__main__":
    main()

