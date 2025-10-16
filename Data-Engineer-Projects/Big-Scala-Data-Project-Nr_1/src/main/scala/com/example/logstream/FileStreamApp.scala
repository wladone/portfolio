package com.example.logstream

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{ StreamingQuery, Trigger }
import org.apache.spark.sql.types._
import com.datastax.spark.connector._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.slf4j.LoggerFactory

// Import Spark-specific schema extensions
import LogParserSchema._

/** File-based Structured Streaming application for real-time log analytics
  *
  * Computes the following metrics:
  *   1. Requests per minute (RPM) on 1-minute tumbling windows 2. 5xx errors per minute 3. Status
  *      code distribution per minute (1xx, 2xx, 3xx, 4xx, 5xx) 4. Top endpoints on 5-minute sliding
  *      windows with 1-minute slides
  */
object FileStreamApp {

  private val logger = LoggerFactory.getLogger(getClass)

  private val AllowedPathPattern = "^[a-zA-Z0-9/_.-]+$".r
  private val MaxPathLength      = 500

  private val MinEndpointThreshold  = 5
  private val WatermarkDelayMinutes = 2
  private val WindowSizeMinutes     = 5
  private val SlideIntervalMinutes  = 1

  case class Config(
      inputPath: String = "input/",
      outputPath: String = "output/",
      checkpointPath: String = "checkpoint/",
      cassandraEnabled: Boolean = false,
      cassandraHost: String = "localhost",
      cassandraKeyspace: String = "log_analytics",
      processingTime: String = "0 seconds"
  ) {
    validateConfig()

    private def validateConfig(): Unit = {
      if (!isValidPath(inputPath)) {
        throw new IllegalArgumentException(s"Invalid input path: $inputPath")
      }
      if (!isValidPath(outputPath)) {
        throw new IllegalArgumentException(s"Invalid output path: $outputPath")
      }
      if (!isValidPath(checkpointPath)) {
        throw new IllegalArgumentException(s"Invalid checkpoint path: $checkpointPath")
      }
      if (cassandraEnabled && cassandraHost.trim.isEmpty) {
        throw new IllegalArgumentException(
          "Cassandra host cannot be empty when Cassandra is enabled"
        )
      }
      if (cassandraEnabled && cassandraKeyspace.trim.isEmpty) {
        throw new IllegalArgumentException(
          "Cassandra keyspace cannot be empty when Cassandra is enabled"
        )
      }
    }

    private def isValidPath(path: String): Boolean =
      path != null &&
        path.length <= MaxPathLength &&
        path.nonEmpty &&
        AllowedPathPattern.findFirstIn(path).isDefined &&
        !path.contains("..")
  }

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    validateInputPaths(config.inputPath)

    val spark = SparkSession.builder()
      .appName("LogAnalytics-FileStream")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.streaming.checkpointLocation", config.checkpointPath)
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    if (config.cassandraEnabled) {
      spark.conf.set("spark.cassandra.connection.host", config.cassandraHost)
      logger.info(
        s"Cassandra enabled - Host: ${config.cassandraHost}, Keyspace: ${config.cassandraKeyspace}"
      )
    }

    logger.info(s"Starting FileStreamApp with config: $config")

    try {
      val logStream    = createLogStream(spark, config.inputPath)
      val parsedStream = parseLogEntries(logStream)
      val watermarkedStream =
        parsedStream.withWatermark("timestamp", s"$WatermarkDelayMinutes minutes")

      logger.info("Starting metrics computation...")
      val startTime = System.currentTimeMillis()

      val rpmStream = computeRequestsPerMinute(watermarkedStream)
      logger.info("Requests per minute computation configured")

      val errorStream = computeErrorsPerMinute(watermarkedStream)
      logger.info("Error computation configured")

      val statusDistributionStream = computeStatusDistribution(watermarkedStream)
      logger.info("Status distribution computation configured")

      val topEndpointsStream = computeTopEndpoints(watermarkedStream)
      logger.info("Top endpoints computation configured")

      val computationTime = System.currentTimeMillis() - startTime
      logger.info(s"Metrics computation setup completed in ${computationTime}ms")

      val consoleQueries = Seq(
        writeToConsole(rpmStream, "Requests Per Minute", config),
        writeToConsole(errorStream, "5xx Errors Per Minute", config),
        writeToConsole(statusDistributionStream, "Status Distribution", config),
        writeToConsole(topEndpointsStream, "Top Endpoints", config)
      )

      val parquetQueries = Seq(
        writeToParquet(rpmStream, s"${config.outputPath}/requests_per_minute", config),
        writeToParquet(errorStream, s"${config.outputPath}/errors_per_minute", config),
        writeToParquet(
          statusDistributionStream,
          s"${config.outputPath}/status_distribution",
          config
        ),
        writeToParquet(topEndpointsStream, s"${config.outputPath}/top_endpoints", config)
      )

      val cassandraQueries =
        if (config.cassandraEnabled) {
          Seq(
            writeRequestsPerMinuteToCassandra(rpmStream, config),
            writeErrorsPerMinuteToCassandra(errorStream, config),
            writeStatusDistributionToCassandra(statusDistributionStream, config),
            writeTopEndpointsToCassandra(topEndpointsStream, config)
          )
        } else {
          Seq.empty[StreamingQuery]
        }

      val activeQueries = consoleQueries ++ parquetQueries ++ cassandraQueries
      logger.info(
        s"All streaming queries started successfully (${activeQueries.size} active queries)"
      )

      spark.streams.awaitAnyTermination()

    } catch {
      case e: java.nio.file.AccessDeniedException =>
        logger.error(s"Access denied to path: ${config.inputPath}", e)
        throw new RuntimeException(s"Cannot access input path: ${config.inputPath}", e)
      case e: java.net.ConnectException =>
        logger.error(s"Network connection failed for Cassandra host: ${config.cassandraHost}", e)
        throw new RuntimeException(s"Cannot connect to Cassandra at ${config.cassandraHost}", e)
      case e: java.io.FileNotFoundException =>
        logger.error("Required file or directory not found", e)
        throw new RuntimeException("File system error", e)
      case e: IllegalArgumentException =>
        logger.error(s"Configuration error: ${e.getMessage}", e)
        throw e
      case e: Exception =>
        logger.error(s"Unexpected error in FileStreamApp: ${e.getMessage}", e)
        throw new RuntimeException("Application failed", e)
    } finally {
      logger.info("Closing Spark session")
      spark.close()
    }
  }

  private def parseArgs(args: Array[String]): Config = {
    val defaults = Config()
    val argMap = args.grouped(2).collect {
      case Array(key, value) => key -> value
    }.toMap

    Config(
      inputPath = argMap.getOrElse("--input", defaults.inputPath),
      outputPath = argMap.getOrElse("--output", defaults.outputPath),
      checkpointPath = argMap.getOrElse("--checkpoint", defaults.checkpointPath),
      cassandraEnabled = argMap.get("--cassandra.enabled").exists(_.toBoolean),
      cassandraHost = argMap.getOrElse("--cassandra.host", defaults.cassandraHost),
      cassandraKeyspace = argMap.getOrElse("--cassandra.keyspace", defaults.cassandraKeyspace),
      processingTime = argMap.getOrElse("--processing.time", defaults.processingTime)
    )
  }

  private def validateInputPaths(inputPath: String): Unit = {
    val inputDir = new java.io.File(inputPath)
    if (!inputDir.exists()) {
      logger.info(s"Input directory $inputPath does not exist, creating it")
      inputDir.mkdirs()
    } else if (!inputDir.isDirectory) {
      logger.warn(s"Input path $inputPath exists but is not a directory")
    } else if (!inputDir.canRead) {
      logger.warn(s"Input directory $inputPath is not readable")
    } else {
      logger.info(s"Using input directory: $inputPath")
    }
  }

  private def createLogStream(spark: SparkSession, inputPath: String): DataFrame =
    spark
      .readStream
      .format("text")
      .option("path", inputPath)
      .option("maxFilesPerTrigger", 1)
      .load()
      .select(col("value"))
      .withColumn("value", trim(col("value")))
      .filter(col("value") =!= "")

  private def parseLogEntries(logStream: DataFrame): DataFrame = {
    val parseUdf = udf((line: String) => LogParser.parseLogLine(line).map(_.toRow))

    logStream
      .withColumn("parsed", parseUdf(col("value")))
      .filter(col("parsed").isNotNull)
      .select("parsed.*")
      .withColumn("processing_time_ms", current_timestamp().cast(LongType))
  }

  private def computeRequestsPerMinute(stream: DataFrame): DataFrame =
    stream
      .withColumn("window", window(col("timestamp"), "1 minute"))
      .groupBy(col("window"))
      .agg(count(lit(1)).as("request_count"))
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .withColumn("metric_type", lit("requests_per_minute"))
      .withColumn("processing_timestamp", current_timestamp())
      .drop("window")

  private def computeErrorsPerMinute(stream: DataFrame): DataFrame =
    stream
      .filter(col("status_code") >= 500)
      .withColumn("window", window(col("timestamp"), "1 minute"))
      .groupBy(col("window"))
      .agg(count(lit(1)).as("error_count"))
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .withColumn("metric_type", lit("errors_per_minute"))
      .drop("window")

  private def computeStatusDistribution(stream: DataFrame): DataFrame =
    stream
      .withColumn("status_class", concat(floor(col("status_code") / 100), lit("xx")))
      .withColumn("window", window(col("timestamp"), "1 minute"))
      .groupBy(col("window"), col("status_class"))
      .agg(count(lit(1)).as("count"))
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .withColumn("metric_type", lit("status_distribution"))
      .drop("window")

  private def computeTopEndpoints(stream: DataFrame): DataFrame = {
    val endpointUdf = udf(LogParser.extractEndpoint _)

    stream
      .withColumn("endpoint", endpointUdf(col("request_line")))
      .withColumn(
        "window",
        window(col("timestamp"), s"$WindowSizeMinutes minutes", s"$SlideIntervalMinutes minute")
      )
      .groupBy(col("window"), col("endpoint"))
      .agg(count(lit(1)).as("request_count"))
      .filter(col("request_count") > MinEndpointThreshold)
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .withColumn("metric_type", lit("top_endpoints"))
      .withColumn("processing_timestamp", current_timestamp())
      .drop("window")
  }

  private def writeToConsole(df: DataFrame, name: String, config: Config): StreamingQuery = {
    val checkpointName = name.replaceAll(" ", "_").toLowerCase
    df.writeStream
      .format("console")
      .option("checkpointLocation", s"${config.checkpointPath}/console_$checkpointName")
      .option("truncate", "false")
      .start()
  }

  private def writeToParquet(df: DataFrame, path: String, config: Config): StreamingQuery = {
    val checkpointSuffix = path.replaceAll("[^a-zA-Z0-9]", "_")
    df.writeStream
      .format("parquet")
      .option("path", path)
      .option("checkpointLocation", s"${config.checkpointPath}/parquet_${checkpointSuffix}")
      .partitionBy("window_start")
      .start()
  }

  private def writeRequestsPerMinuteToCassandra(df: DataFrame, config: Config): StreamingQuery =
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Writing requests per minute batch $batchId to Cassandra")

        val cassandraDF = batchDF
          .withColumn("minute", col("window_start"))
          .withColumn("requests", col("request_count").cast(IntegerType))
          .select("minute", "requests")

        cassandraDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", config.cassandraKeyspace)
          .option("table", "requests_per_minute")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", s"${config.checkpointPath}/cass_requests_per_minute")
      .start()

  private def writeErrorsPerMinuteToCassandra(df: DataFrame, config: Config): StreamingQuery =
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Writing errors per minute batch $batchId to Cassandra")

        val cassandraDF = batchDF
          .withColumn("minute", col("window_start"))
          .withColumn("status", lit(500))
          .withColumn("count", col("error_count").cast(IntegerType))
          .select("minute", "status", "count")

        cassandraDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", config.cassandraKeyspace)
          .option("table", "errors_per_minute")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", s"${config.checkpointPath}/cass_errors_per_minute")
      .start()

  private def writeStatusDistributionToCassandra(df: DataFrame, config: Config): StreamingQuery =
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Writing status distribution batch $batchId to Cassandra")

        val cassandraDF = batchDF
          .withColumn("bucket_date", to_date(col("window_start")))
          .withColumn("minute", col("window_start"))
          .withColumn("status_class", substring(col("status_class"), 0, 1).cast(IntegerType))
          .withColumn("count", col("count").cast(IntegerType))
          .select("bucket_date", "minute", "status_class", "count")

        cassandraDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", config.cassandraKeyspace)
          .option("table", "status_distribution_per_minute_by_day")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", s"${config.checkpointPath}/cass_status_distribution")
      .start()

  private def writeTopEndpointsToCassandra(df: DataFrame, config: Config): StreamingQuery =
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Writing top endpoints batch $batchId to Cassandra")

        val cassandraDF = batchDF
          .withColumn("bucket_date", to_date(col("window_start")))
          .withColumn("window_start", col("window_start"))
          .withColumn("endpoint", col("endpoint"))
          .withColumn("hits", col("request_count").cast(IntegerType))
          .select("bucket_date", "window_start", "endpoint", "hits")

        cassandraDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", config.cassandraKeyspace)
          .option("table", "top_endpoints_5min_by_day")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", s"${config.checkpointPath}/cass_top_endpoints")
      .start()
}
