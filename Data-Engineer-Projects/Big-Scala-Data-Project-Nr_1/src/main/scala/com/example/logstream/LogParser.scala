package com.example.logstream

import scala.util.{ Try, Success, Failure }
import java.time.{ LocalDateTime, format }
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex

/** Parser for Apache Combined Log Format Format: %h %l %u %t \"%r\" %>s %b \"%{Referer}i\"
  * \"%{User-Agent}i\"
  *
  * Example: 127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
  * "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)"
  *
  * NOTE: This parser is designed to be standalone and not depend on Spark. Schema definitions are
  * handled separately in Spark-specific code.
  */
object LogParser {

  // Security: Input length limits to prevent DoS
  private val MaxLogLineLength = 8192
  private val MaxFieldLength   = 2048

  // Apache Combined Log Format regex pattern - optimized to prevent catastrophic backtracking
  private val CombinedLogPattern: Regex =
    raw"""^([a-zA-Z0-9._:-]+) ([^\s]+) ([^\s]+) \[([^\]]+)\] "([^"]+)" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)"$$""".r

  // Date formatters for different log formats
  private val CommonLogDateFormatter =
    DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", java.util.Locale.ENGLISH)
  private val IsoDateFormatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME

  /** Case class representing a parsed log entry
    */
  case class LogRecord(
      client_ip: String,
      remote_logname: String,
      user: String,
      timestamp: java.sql.Timestamp,
      request_line: String,
      status_code: Int,
      response_size: Long,
      referer: String,
      user_agent: String,
      processing_time_ms: Long = 0L
  )

  /** Parse a single log line into a LogRecord with security validation
    */
  def parseLogLine(logLine: String): Option[LogRecord] = {
    // Security: Input validation to prevent DoS attacks
    if (logLine == null || logLine.trim.isEmpty || logLine.length > MaxLogLineLength) {
      return None
    }

    // Sanitize input - remove potential injection attempts
    val sanitizedLine = sanitizeLogLine(logLine)

    Try {
      logLine match {
        case CombinedLogPattern(
              clientIp,
              remoteLogname,
              user,
              timestampStr,
              requestLine,
              statusCodeStr,
              responseSizeStr,
              referer,
              userAgent
            ) =>
          // Parse timestamp
          val timestamp = parseTimestamp(timestampStr) match {
            case Success(ts) => ts
            case Failure(_)  => return None
          }

          // Parse status code
          val statusCode = Try(statusCodeStr.toInt).getOrElse(0)

          // Parse response size
          val responseSize =
            if (responseSizeStr == "-") 0L else Try(responseSizeStr.toLong).getOrElse(0L)

          Some(LogRecord(
            clientIp,
            remoteLogname,
            user,
            timestamp,
            requestLine,
            statusCode,
            responseSize,
            referer,
            userAgent
          ))

        case _ => None
      }
    } match {
      case Success(entry) => entry
      case Failure(_)     => None
    }
  }

  /** Sanitize log line to prevent injection attacks
    */
  private def sanitizeLogLine(logLine: String): String =
    // Remove null bytes and control characters that could cause issues
    logLine.replace("\u0000", "").filter(_ >= ' ')

  /** Validate individual field lengths to prevent memory exhaustion
    */
  private def validateFieldLength(field: String, maxLength: Int): Boolean =
    field != null && field.length <= maxLength

  /** Parse timestamp string into java.sql.Timestamp
    */
  private def parseTimestamp(timestampStr: String): Try[java.sql.Timestamp] = {
    if (!validateFieldLength(timestampStr, 50)) {
      return Failure(new IllegalArgumentException("Timestamp too long"))
    }

    Try {
      val offsetDateTime = java.time.OffsetDateTime.parse(timestampStr, CommonLogDateFormatter)
      java.sql.Timestamp.from(offsetDateTime.toInstant)
    }
  }

  /** Extract HTTP method from request line
    */
  def extractHttpMethod(requestLine: String): String =
    if (requestLine == null || requestLine.trim.isEmpty) "UNKNOWN"
    else requestLine.trim.split(' ').headOption.getOrElse("UNKNOWN")

  /** Extract endpoint from request line
    */
  def extractEndpoint(requestLine: String): String =
    if (requestLine == null || requestLine.trim.isEmpty) "/"
    else {
      val parts = requestLine.trim.split(' ')
      if (parts.length >= 2) {
        val uri = parts(1)
        // Extract path without query parameters
        uri.split('?').headOption.getOrElse("/")
      } else "/"
    }

  /** Get status class (1xx, 2xx, 3xx, 4xx, 5xx)
    */
  def getStatusClass(statusCode: Int): String =
    s"${statusCode / 100}xx"

  /** Check if status code indicates an error (4xx or 5xx)
    */
  def isErrorStatus(statusCode: Int): Boolean =
    statusCode >= 400 && statusCode < 600

  /** Check if status code indicates a server error (5xx)
    */
  def isServerError(statusCode: Int): Boolean =
    statusCode >= 500 && statusCode < 600
}
