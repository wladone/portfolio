package com.example.ecommerce.spark

import com.example.ecommerce.model.Schemas
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object EcomOrdersStream {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Ecommerce Orders Streaming")
      .master("local[*]")
      .getOrCreate()

    try {
      logger.info("Starting Ecommerce Orders Streaming job")

      val inputPath = "data/ecommerce/orders_incoming"
      val metricsPath = "data/ecommerce/metrics"
      val checkpointPath = "checkpoints/ecom"

      // Read streaming data
      val ordersStream = readOrdersStream(spark, inputPath)

      // Add watermark
      val withWatermark = ordersStream
        .withWatermark("event_time", "5 minutes")

      // Compute KPIs
      val gmvPerMinute = computeGMVPerMinute(withWatermark)
      val ordersPerMinute = computeOrdersPerMinute(withWatermark)
      val topProducts = computeTopProducts(withWatermark)

      // Start streaming queries
      val queries = List(
        startConsoleQuery(gmvPerMinute, "GMV per Minute"),
        startConsoleQuery(ordersPerMinute, "Orders per Minute"),
        startConsoleQuery(topProducts, "Top Products"),
        startParquetQuery(gmvPerMinute, s"$metricsPath/gmv", checkpointPath),
        startParquetQuery(ordersPerMinute, s"$metricsPath/orders", checkpointPath),
        startParquetQuery(topProducts, s"$metricsPath/top_products", checkpointPath)
      )

      // Wait for termination
      queries.foreach(_.awaitTermination())

    } catch {
      case ex: Exception =>
        logger.error("Streaming job failed", ex)
        throw ex
    } finally {
      spark.stop()
    }
  }

  private def readOrdersStream(spark: SparkSession, inputPath: String): DataFrame = {
    spark.readStream
      .option("multiline", "false")
      .schema(Schemas.orderSchema)
      .json(inputPath)
      .withColumn("event_time", F.col("event_time").cast("timestamp"))
  }

  private def computeGMVPerMinute(ordersDf: DataFrame): DataFrame = {
    ordersDf
      .withColumn("window", F.window(F.col("event_time"), "1 minute"))
      .groupBy(F.col("window"))
      .agg(
        F.sum(F.col("price") * F.col("quantity")).alias("gmv"),
        F.count("*").alias("order_count")
      )
      .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("gmv"),
        F.col("order_count"),
        F.lit("gmv_per_minute").alias("metric_type")
      )
  }

  private def computeOrdersPerMinute(ordersDf: DataFrame): DataFrame = {
    ordersDf
      .withColumn("window", F.window(F.col("event_time"), "1 minute"))
      .groupBy(F.col("window"))
      .agg(F.count("*").alias("orders_count"))
      .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("orders_count"),
        F.lit("orders_per_minute").alias("metric_type")
      )
  }

  private def computeTopProducts(ordersDf: DataFrame): DataFrame = {
    ordersDf
      .withColumn("window", F.window(F.col("event_time"), "5 minutes", "1 minute"))
      .groupBy(F.col("window"), F.col("product_id"))
      .agg(
        F.sum(F.col("quantity")).alias("total_quantity"),
        F.count("*").alias("order_count")
      )
      .withColumn("rank", F.row_number().over(
        Window.partitionBy(F.col("window")).orderBy(F.desc("total_quantity"))
      ))
      .filter(F.col("rank") <= 5)
      .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("product_id"),
        F.col("total_quantity"),
        F.col("order_count"),
        F.col("rank"),
        F.lit("top_products").alias("metric_type")
      )
  }

  private def startConsoleQuery(df: DataFrame, name: String): StreamingQuery = {
    df.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .queryName(s"console_$name")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()
  }

  private def startParquetQuery(df: DataFrame, path: String, checkpointBase: String): StreamingQuery = {
    val today = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
    val outputPath = s"$path/date=$today"
    val checkpointPath = s"$checkpointBase/${path.split("/").last}"

    df.writeStream
      .outputMode("append")
      .format("parquet")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .partitionBy("window_start")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
  }
}