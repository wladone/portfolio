package com.example.logstream

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/** Spark-specific schema definitions for LogParser This file contains Spark dependencies that are
  * separate from the core parsing logic.
  */
object LogParserSchema {
  val schema: StructType = StructType(Seq(
    StructField("client_ip", StringType, nullable = true),
    StructField("remote_logname", StringType, nullable = true),
    StructField("user", StringType, nullable = true),
    StructField("timestamp", TimestampType, nullable = true),
    StructField("request_line", StringType, nullable = true),
    StructField("status_code", IntegerType, nullable = true),
    StructField("response_size", LongType, nullable = true),
    StructField("referer", StringType, nullable = true),
    StructField("user_agent", StringType, nullable = true),
    StructField("processing_time_ms", LongType, nullable = true)
  ))

  implicit class ToRow(val r: LogParser.LogRecord) extends AnyVal {
    def toRow: Row = Row(
      r.client_ip,
      r.remote_logname,
      r.user,
      r.timestamp,
      r.request_line,
      r.status_code,
      r.response_size,
      r.referer,
      r.user_agent,
      r.processing_time_ms
    )
  }
}
