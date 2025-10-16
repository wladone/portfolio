package com.example.ecommerce.spark

import com.example.ecommerce.model.Schemas
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object EcomBatchJob {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ecommerce Batch ETL")
      .master("local[*]")
      .getOrCreate()

    try {
      logger.info("Starting Ecommerce Batch ETL job")

      val rawDataPath = "data/ecommerce/raw"
      val silverDataPath = "data/ecommerce/silver/products"

      // Read raw NDJSON data
      val rawDf = readRawData(spark, rawDataPath)
      logger.info(s"Read ${rawDf.count()} raw records")

      // Parse and transform data
      val parsedDf = parseAndTransform(rawDf)
      logger.info(s"Parsed ${parsedDf.count()} records after transformation")

      // Deduplicate
      val dedupedDf = deduplicate(parsedDf)
      logger.info(s"After deduplication: ${dedupedDf.count()} records")

      // Write to silver layer
      writeSilverData(dedupedDf, silverDataPath)

      // Create temp views and run sample queries
      runSampleQueries(dedupedDf)

      logger.info("Ecommerce Batch ETL job completed successfully")

    } catch {
      case ex: Exception =>
        logger.error("Batch ETL job failed", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }

  private def readRawData(spark: SparkSession, rawDataPath: String): DataFrame = {
    spark.read
      .option("multiline", "false")
      .json(s"$rawDataPath/source=*/run_date=*/*.ndjson")
  }

  private def parseAndTransform(rawDf: DataFrame): DataFrame = {
    import rawDf.sparkSession.implicits._

    // Extract source and run_date from input path
    val withMetadata = rawDf
      .withColumn("_source", F.regexp_extract(F.input_file_name(), "source=([^/]+)", 1))
      .withColumn("run_date", F.regexp_extract(F.input_file_name(), "run_date=([^/]+)", 1))
      .withColumn("_ingest_ts", F.current_timestamp().cast("long") * 1000)

    // Parse nested images array and flatten
    val parsed = withMetadata
      .withColumn("images", F.from_json(F.col("images"), ArrayType(StringType)))

    // Select and cast to match schema
    parsed.select(
      F.col("id").cast(IntegerType),
      F.col("title"),
      F.col("description"),
      F.col("price").cast(DoubleType),
      F.col("discountPercentage").cast(DoubleType),
      F.col("rating").cast(DoubleType),
      F.col("stock").cast(IntegerType),
      F.col("brand"),
      F.col("category"),
      F.col("thumbnail"),
      F.col("images"),
      F.col("_source"),
      F.col("run_date"),
      F.col("_ingest_ts")
    )
  }

  def deduplicate(df: DataFrame): DataFrame = {
    val windowSpec = Window
      .partitionBy("_source", "id")
      .orderBy(F.col("_ingest_ts").desc)

    df.withColumn("row_num", F.row_number().over(windowSpec))
      .filter(F.col("row_num") === 1)
      .drop("row_num")
  }

  private def writeSilverData(df: DataFrame, silverDataPath: String): Unit = {
    val ingestDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)

    df.write
      .mode("overwrite")
      .partitionBy("category", "ingest_date")
      .parquet(s"$silverDataPath/ingest_date=$ingestDate")
  }

  private def runSampleQueries(df: DataFrame): Unit = {
    // Create temp view
    df.createOrReplaceTempView("products")

    // Sample queries
    logger.info("=== Sample Analytics Queries ===")

    // Top 10 brands by product count
    logger.info("Top 10 brands by product count:")
    df.groupBy("brand")
      .count()
      .orderBy(F.desc("count"))
      .limit(10)
      .show(false)

    // Price distribution buckets
    logger.info("Price distribution:")
    df.withColumn("price_bucket",
        F.when(F.col("price") < 10, "< $10")
         .when(F.col("price") < 50, "$10-$49")
         .when(F.col("price") < 100, "$50-$99")
         .when(F.col("price") < 500, "$100-$499")
         .otherwise("$500+")
      )
      .groupBy("price_bucket")
      .count()
      .orderBy("price_bucket")
      .show(false)

    // Average rating by category
    logger.info("Average rating by category:")
    df.groupBy("category")
      .agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("product_count"))
      .orderBy(F.desc("avg_rating"))
      .show(false)

    // Top products by rating
    logger.info("Top 10 products by rating:")
    df.select("title", "brand", "category", "rating", "price")
      .orderBy(F.desc("rating"))
      .limit(10)
      .show(false)
  }
}