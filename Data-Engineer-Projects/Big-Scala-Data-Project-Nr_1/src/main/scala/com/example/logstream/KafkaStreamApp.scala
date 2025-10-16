package com.example.logstream

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

// Import Spark-specific schema extensions
import LogParserSchema._

object KafkaStreamApp {

  private val logger = LoggerFactory.getLogger(getClass)

  case class Config(
      kafkaBootstrapServers: String = "localhost:9092",
      kafkaTopic: String = "log-events",
      outputPath: String = "output/kafka/",
      checkpointPath: String = "checkpoint/kafka/",
      cassandraEnabled: Boolean = false,
      cassandraHost: String = "localhost",
      cassandraKeyspace: String = "log_analytics",
      startingOffsets: String = "latest"
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    val spark = SparkSession.builder()
      .appName("LogAnalytics-KafkaStream")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.streaming.checkpointLocation", config.checkpointPath)
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    logger.info(s"Starting KafkaStreamApp with config: $config")

    try {
      val kafkaStream       = createKafkaStream(spark, config)
      val parsedStream      = parseLogEntries(kafkaStream)
      val watermarkedStream = parsedStream.withWatermark("timestamp", "2 minutes")
      val rpmStream         = computeRequestsPerMinute(watermarkedStream, config.kafkaTopic)

      val consoleQueries = Seq(writeToConsole(rpmStream, "Kafka Requests Per Minute", config))
      val parquetQueries =
        Seq(writeToParquet(rpmStream, s"${config.outputPath}/kafka_requests_per_minute", config))
      val cassandraQueries =
        if (config.cassandraEnabled) {
          Seq(writeToCassandra(rpmStream, config, "kafka_requests_per_minute"))
        } else {
          Seq.empty[StreamingQuery]
        }

      val activeQueries = consoleQueries ++ parquetQueries ++ cassandraQueries
      logger.info(
        s"Kafka streaming queries started successfully (${activeQueries.size} active queries)"
      )

      spark.streams.awaitAnyTermination()

    } catch {
      case e: Exception =>
        logger.error("Error in KafkaStreamApp", e)
        throw e
    } finally {
      logger.info("Closing Spark session")
      spark.close()
    }
  }

  private def parseArgs(args: Array[String]): Config = {
    var config = Config()

    args.sliding(2, 2).foreach {
      case Array("--kafka.bootstrap.servers", value) =>
        config = config.copy(kafkaBootstrapServers = value)
      case Array("--kafka.topic", value) => config = config.copy(kafkaTopic = value)
      case Array("--output", value)      => config = config.copy(outputPath = value)
      case Array("--checkpoint", value)  => config = config.copy(checkpointPath = value)
      case Array("--cassandra.enabled", value) =>
        config = config.copy(cassandraEnabled = value.toBoolean)
      case Array("--cassandra.host", value)     => config = config.copy(cassandraHost = value)
      case Array("--cassandra.keyspace", value) => config = config.copy(cassandraKeyspace = value)
      case Array("--starting.offsets", value)   => config = config.copy(startingOffsets = value)
      case _                                    =>
    }

    config
  }

  private def createKafkaStream(spark: SparkSession, config: Config): DataFrame =
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrapServers)
      .option("subscribe", config.kafkaTopic)
      .option("startingOffsets", config.startingOffsets)
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(value AS STRING) as value")
      .withColumn("value", trim(col("value")))
      .filter(col("value") =!= "")

  private def parseLogEntries(kafkaStream: DataFrame): DataFrame = {
    val parseUdf = udf((line: String) => LogParser.parseLogLine(line).map(_.toRow))

    kafkaStream
      .withColumn("parsed", parseUdf(col("value")))
      .filter(col("parsed").isNotNull)
      .select("parsed.*")
      .withColumn("processing_time_ms", current_timestamp().cast(LongType))
  }

  private def computeRequestsPerMinute(stream: DataFrame, topic: String): DataFrame =
    stream
      .withColumn("window", window(col("timestamp"), "1 minute"))
      .groupBy(col("window"))
      .agg(count(lit(1)).as("request_count"))
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .withColumn("metric_type", lit("kafka_requests_per_minute"))
      .withColumn("kafka_topic", lit(topic))
      .drop("window")

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

  private def writeToCassandra(df: DataFrame, config: Config, tableName: String): StreamingQuery =
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Writing Kafka batch $batchId to Cassandra table $tableName")

        batchDF.write
          .format("org.apache.spark.sql.cassandra")
          .option("keyspace", config.cassandraKeyspace)
          .option("table", tableName)
          .mode("append")
          .save()

        if (batchDF.columns.contains("window_start")) {
          val bucketedDF = batchDF
            .withColumn("bucket_date", to_date(col("window_start")))
            .withColumn("batch_id", lit(batchId))

          bucketedDF.write
            .format("org.apache.spark.sql.cassandra")
            .option("keyspace", config.cassandraKeyspace)
            .option("table", s"${tableName}_by_day")
            .mode("append")
            .save()
        }
      }
      .option("checkpointLocation", s"${config.checkpointPath}/cass_${tableName}")
      .start()
}
