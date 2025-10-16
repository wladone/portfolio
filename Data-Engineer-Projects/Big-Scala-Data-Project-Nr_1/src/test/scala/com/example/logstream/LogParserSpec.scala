package com.example.logstream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.sql.Timestamp
import java.time.LocalDateTime

// Import Spark schema extensions for testing
import LogParserSchema._

class LogParserSpec extends AnyFlatSpec with Matchers {

  "LogParser" should "parse a valid Apache Combined Log Format line" in {
    val logLine = """127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "http://www.example.com/start.html" "Mozilla/4.08 [en] (Win98; I ;Nav)""""

    val result = LogParser.parseLogLine(logLine)

    result should be (defined)
    result.get.client_ip should be ("127.0.0.1")
    result.get.remote_logname should be ("-")
    result.get.user should be ("-")
    result.get.request_line should be ("GET /apache_pb.gif HTTP/1.0")
    result.get.status_code should be (200)
    result.get.response_size should be (2326L)
    result.get.referer should be ("http://www.example.com/start.html")
    result.get.user_agent should be ("Mozilla/4.08 [en] (Win98; I ;Nav)")
  }

  it should "handle missing referer and user agent" in {
    val logLine = """192.168.1.1 - user1 [15/Nov/2023:10:30:45 +0000] "POST /api/data HTTP/1.1" 201 1024 "-" "-""""

    val result = LogParser.parseLogLine(logLine)

    result should be (defined)
    result.get.client_ip should be ("192.168.1.1")
    result.get.user should be ("user1")
    result.get.referer should be ("-")
    result.get.user_agent should be ("-")
  }

  it should "handle zero response size" in {
    val logLine = """10.0.0.5 - - [20/Dec/2023:14:22:10 -0500] "HEAD /health HTTP/1.1" 200 0 "http://internal.monitor" "HealthCheck/1.0""""

    val result = LogParser.parseLogLine(logLine)

    result should be (defined)
    result.get.response_size should be (0L)
  }

  it should "return None for malformed log lines" in {
    val malformedLines = Seq(
      "",
      "invalid log line",
      "127.0.0.1 - - [invalid-date] \"GET /test HTTP/1.0\" 200 100",
      "not-enough-fields",
      "127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] \"GET /test HTTP/1.0\" invalid-status 100"
    )

    malformedLines.foreach { line =>
      LogParser.parseLogLine(line) should be (None)
    }
  }

  it should "extract HTTP method correctly" in {
    val testCases = Seq(
      ("GET /path HTTP/1.1", "GET"),
      ("POST /api/data HTTP/1.0", "POST"),
      ("PUT /resource/123 HTTP/1.1", "PUT"),
      ("DELETE /item/456 HTTP/1.0", "DELETE"),
      ("HEAD /health HTTP/1.1", "HEAD"),
      ("", "UNKNOWN"),
      (null, "UNKNOWN")
    )

    testCases.foreach { case (requestLine, expectedMethod) =>
      LogParser.extractHttpMethod(requestLine) should be (expectedMethod)
    }
  }

  it should "extract endpoint correctly" in {
    val testCases = Seq(
      ("GET /api/users HTTP/1.1", "/api/users"),
      ("POST /api/users/123 HTTP/1.0", "/api/users/123"),
      ("GET /api/search?q=test HTTP/1.1", "/api/search"),
      ("GET / HTTP/1.1", "/"),
      ("GET /static/css/main.css HTTP/1.1", "/static/css/main.css"),
      ("", "/"),
      (null, "/")
    )

    testCases.foreach { case (requestLine, expectedEndpoint) =>
      LogParser.extractEndpoint(requestLine) should be (expectedEndpoint)
    }
  }

  it should "classify status codes correctly" in {
    val testCases = Map(
      100 -> "1xx",
      200 -> "2xx",
      201 -> "2xx",
      300 -> "3xx",
      400 -> "4xx",
      404 -> "4xx",
      500 -> "5xx",
      502 -> "5xx",
      0 -> "0xx"
    )

    testCases.foreach { case (statusCode, expectedClass) =>
      LogParser.getStatusClass(statusCode) should be (expectedClass)
    }
  }

  it should "identify error status codes correctly" in {
    LogParser.isErrorStatus(400) should be (true)
    LogParser.isErrorStatus(404) should be (true)
    LogParser.isErrorStatus(500) should be (true)
    LogParser.isErrorStatus(502) should be (true)

    LogParser.isErrorStatus(200) should be (false)
    LogParser.isErrorStatus(201) should be (false)
    LogParser.isErrorStatus(300) should be (false)
  }

  it should "identify server error status codes correctly" in {
    LogParser.isServerError(500) should be (true)
    LogParser.isServerError(502) should be (true)
    LogParser.isServerError(503) should be (true)

    LogParser.isServerError(400) should be (false)
    LogParser.isServerError(404) should be (false)
    LogParser.isServerError(200) should be (false)
  }

  it should "handle various timestamp formats" in {
    val validTimestamps = Seq(
      "[10/Oct/2000:13:55:36 -0700]",
      "[15/Nov/2023:10:30:45 +0000]",
      "[20/Dec/2023:14:22:10 -0500]",
      "[01/Jan/2024:00:00:00 +0200]"
    )

    validTimestamps.foreach { timestampStr =>
      val logLine = s"""127.0.0.1 - - $timestampStr "GET /test HTTP/1.0" 200 100 "-" "-""""
      val result = LogParser.parseLogLine(logLine)

      result should be (defined)
      result.get.timestamp should not be null
    }
  }

  it should "create correct Row from LogRecord" in {
    val logRecord = LogParser.LogRecord(
      "192.168.1.1",
      "-",
      "testuser",
      Timestamp.valueOf("2023-12-01 10:30:45"),
      "GET /api/test HTTP/1.1",
      200,
      512L,
      "http://example.com",
      "TestAgent/1.0",
      100L
    )

    val row = logRecord.toRow

    row.length should be (10)
    row.getString(0) should be ("192.168.1.1")
    row.getString(1) should be ("-")
    row.getString(2) should be ("testuser")
    row.getTimestamp(3) should be (Timestamp.valueOf("2023-12-01 10:30:45"))
    row.getString(4) should be ("GET /api/test HTTP/1.1")
    row.getInt(5) should be (200)
    row.getLong(6) should be (512L)
    row.getString(7) should be ("http://example.com")
    row.getString(8) should be ("TestAgent/1.0")
    row.getLong(9) should be (100L)
  }

  it should "handle edge cases gracefully" in {
    // Empty request line
    val emptyRequestLine = """127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "" 200 0 "-" "-""""
    LogParser.parseLogLine(emptyRequestLine) should be (None)

    // Very long user agent
    val longUserAgent = """127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "Very long user agent string with special characters !@#$%^&*()_+{}|:<>?[]\;',./""""
    val result = LogParser.parseLogLine(longUserAgent)
    result should be (defined)
    result.get.user_agent should include ("Very long user agent string")
  }

  it should "handle various edge cases in log parsing" in {
    // Test with IPv6-like address (should still work as string)
    val ipv6LikeLine = """2001:db8::1 - - [10/Oct/2000:13:55:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "-""""
    val result1 = LogParser.parseLogLine(ipv6LikeLine)
    result1 should be (defined)
    result1.get.client_ip should be ("2001:db8::1")

    // Test with extremely large response size
    val largeResponseLine = """127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /large-file HTTP/1.0" 200 9223372036854775807 "-" "-""""
    val result2 = LogParser.parseLogLine(largeResponseLine)
    result2 should be (defined)
    result2.get.response_size should be (9223372036854775807L)

    // Test with unicode characters in user agent
    val unicodeUserAgent = """127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "Mozilla/5.0 (测试浏览器)""""
    val result3 = LogParser.parseLogLine(unicodeUserAgent)
    result3 should be (defined)
    result3.get.user_agent should include ("测试浏览器")
  }

  it should "handle malformed timestamp formats gracefully" in {
    val malformedTimestampLines = Seq(
      """127.0.0.1 - - [invalid-date] "GET /test HTTP/1.0" 200 100 "-" "-"""",
      """127.0.0.1 - - [10/Oct/2000:13:55:36] "GET /test HTTP/1.0" 200 100 "-" "-"""", // Missing timezone
      """127.0.0.1 - - [10/Oct/2000:25:55:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "-"""", // Invalid hour
      """127.0.0.1 - - [10/Oct/2000:13:61:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "-"""", // Invalid minute
      """127.0.0.1 - - [32/Oct/2000:13:55:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "-"""", // Invalid day
      """127.0.0.1 - - [10/Invalid/2000:13:55:36 -0700] "GET /test HTTP/1.0" 200 100 "-" "-"""", // Invalid month
      """127.0.0.1 - - [10/Oct/2000:13:55:36 -2500] "GET /test HTTP/1.0" 200 100 "-" "-"""" // Invalid timezone
    )

    malformedTimestampLines.foreach { line =>
      LogParser.parseLogLine(line) should be (None)
    }
  }

  it should "handle boundary status codes correctly" in {
    // Test boundary status codes
    LogParser.getStatusClass(100) should be ("1xx")
    LogParser.getStatusClass(199) should be ("1xx")
    LogParser.getStatusClass(200) should be ("2xx")
    LogParser.getStatusClass(299) should be ("2xx")
    LogParser.getStatusClass(300) should be ("3xx")
    LogParser.getStatusClass(399) should be ("3xx")
    LogParser.getStatusClass(400) should be ("4xx")
    LogParser.getStatusClass(499) should be ("4xx")
    LogParser.getStatusClass(500) should be ("5xx")
    LogParser.getStatusClass(599) should be ("5xx")

    // Test error status boundaries
    LogParser.isErrorStatus(400) should be (true)
    LogParser.isErrorStatus(499) should be (true)
    LogParser.isErrorStatus(500) should be (true)
    LogParser.isErrorStatus(599) should be (true)
    LogParser.isErrorStatus(399) should be (false)
    LogParser.isErrorStatus(600) should be (false)

    // Test server error boundaries
    LogParser.isServerError(500) should be (true)
    LogParser.isServerError(599) should be (true)
    LogParser.isServerError(499) should be (false)
    LogParser.isServerError(600) should be (false)
  }
}