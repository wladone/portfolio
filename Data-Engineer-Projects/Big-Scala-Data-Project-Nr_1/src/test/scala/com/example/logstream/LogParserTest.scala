package com.example.logstream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LogParserTest extends AnyFlatSpec with Matchers {

  "LogParser.parseLogLine" should "parse valid Apache Combined log lines" in {
    val validLogLine = """192.168.1.1 - - [01/Oct/2025:11:50:00 +0000] "GET /api/test HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0""""

    val result = LogParser.parseLogLine(validLogLine)

    result shouldBe defined
    val entry = result.get
    entry.client_ip shouldBe "192.168.1.1"
    entry.status_code shouldBe 200
    entry.response_size shouldBe 1234L
    entry.request_line shouldBe "GET /api/test HTTP/1.1"
  }

  it should "return None for malformed log lines" in {
    val invalidLogLine = "this is not a valid log line"

    val result = LogParser.parseLogLine(invalidLogLine)

    result shouldBe None
  }

  it should "return None for null input" in {
    val result = LogParser.parseLogLine(null)

    result shouldBe None
  }

  it should "return None for empty input" in {
    val result = LogParser.parseLogLine("")

    result shouldBe None
  }

  it should "return None for overly long input" in {
    val longLine = "x" * 10000
    val result = LogParser.parseLogLine(longLine)

    result shouldBe None
  }

  "LogParser.extractHttpMethod" should "extract HTTP methods correctly" in {
    LogParser.extractHttpMethod("GET /api/test HTTP/1.1") shouldBe "GET"
    LogParser.extractHttpMethod("POST /api/data HTTP/1.0") shouldBe "POST"
    LogParser.extractHttpMethod("") shouldBe "UNKNOWN"
    LogParser.extractHttpMethod(null) shouldBe "UNKNOWN"
  }

  "LogParser.extractEndpoint" should "extract endpoints correctly" in {
    LogParser.extractEndpoint("GET /api/test HTTP/1.1") shouldBe "/api/test"
    LogParser.extractEndpoint("POST /api/data?id=123 HTTP/1.0") shouldBe "/api/data"
    LogParser.extractEndpoint("GET / HTTP/1.1") shouldBe "/"
    LogParser.extractEndpoint("") shouldBe "/"
    LogParser.extractEndpoint(null) shouldBe "/"
  }

  "LogParser.getStatusClass" should "classify status codes correctly" in {
    LogParser.getStatusClass(200) shouldBe "2xx"
    LogParser.getStatusClass(404) shouldBe "4xx"
    LogParser.getStatusClass(500) shouldBe "5xx"
    LogParser.getStatusClass(100) shouldBe "1xx"
    LogParser.getStatusClass(300) shouldBe "3xx"
  }

  "LogParser.isErrorStatus" should "identify error status codes" in {
    LogParser.isErrorStatus(400) shouldBe true
    LogParser.isErrorStatus(404) shouldBe true
    LogParser.isErrorStatus(500) shouldBe true
    LogParser.isErrorStatus(200) shouldBe false
    LogParser.isErrorStatus(300) shouldBe false
  }

  "LogParser.isServerError" should "identify server error status codes" in {
    LogParser.isServerError(500) shouldBe true
    LogParser.isServerError(502) shouldBe true
    LogParser.isServerError(599) shouldBe true
    LogParser.isServerError(400) shouldBe false
    LogParser.isServerError(200) shouldBe false
  }
}