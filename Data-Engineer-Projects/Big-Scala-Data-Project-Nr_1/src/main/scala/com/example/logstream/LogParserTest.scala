package com.example.logstream

import scala.io.Source
import scala.collection.mutable.Map
import scala.util.matching.Regex
import java.nio.file.{ Files, Paths }

/** Simple LogParser test using Scala - no Spark or compilation required! Tests your LogParser logic
  * by reading the input file and analyzing the data.
  */
object LogParserTest {

  def main(args: Array[String]): Unit =
    testLogParser()

  def testLogParser(): Unit = {
    println("LogParser Test (Scala Version)")
    println("=" * 40)

    // Check if input file exists
    val inputFile = "input/access.log"
    if (!Files.exists(Paths.get(inputFile))) {
      println(s"ERROR: Input file not found: $inputFile")
      println("Generate test data first:")
      println(
        "   sbt \"runMain com.example.logstream.LogGenerator --output input/access.log --lines 1000\""
      )
      return
    }

    val lines = Source.fromFile(inputFile).getLines().toArray
    println(s"Found ${lines.length} log lines in $inputFile")

    // Test parsing of first few lines
    println("\nTesting first 5 log lines:")

    // Simple regex pattern (similar to your Scala version)
    val logPattern =
      """^(\S+) ([^\s]+) ([^\s]+) \[([^\]]+)\] "([^"]+)" (\d+) (\d+|[0-9-]+) "([^"]*)" "([^"]*)"$$""".r

    var validEntries   = 0
    val statusCounts   = Map[String, Int]().withDefaultValue(0)
    val methodCounts   = Map[String, Int]().withDefaultValue(0)
    val endpointCounts = Map[String, Int]().withDefaultValue(0)

    // Test first 100 lines
    val testLines = lines.take(100)
    testLines.zipWithIndex.foreach { case (line, i) =>
      val trimmedLine = line.trim
      if (trimmedLine.nonEmpty) {
        logPattern.findFirstMatchIn(trimmedLine) match {
          case Some(m) =>
            validEntries += 1
            val clientIp     = m.group(1)
            val timestampStr = m.group(4)
            val requestLine  = m.group(5)
            val statusCode   = m.group(6).toInt
            val responseSize = m.group(7)
            val referer      = m.group(8)
            val userAgent    = m.group(9)

            // Extract method and endpoint (similar to your Scala logic)
            val method = if (requestLine != null && requestLine.trim.nonEmpty) {
              requestLine.trim.split(' ').headOption.getOrElse("UNKNOWN")
            } else "UNKNOWN"

            val endpoint = if (requestLine != null && requestLine.trim.nonEmpty) {
              val parts = requestLine.trim.split(' ')
              if (parts.length >= 2) {
                val uri = parts(1)
                // Extract path without query parameters
                uri.split('?').headOption.getOrElse("/")
              } else "/"
            } else "/"

            // Status classification (similar to your Scala logic)
            val statusClass = s"${statusCode / 100}xx"

            println(f"${i + 1}%2d. OK $method $endpoint -> $statusCode ($statusClass)")

            statusCounts(statusClass) += 1
            methodCounts(method) += 1
            endpointCounts(endpoint) += 1

          case None =>
            println(f"${i + 1}%2d. FAILED to parse: ${trimmedLine.take(80)}...")
        }
      }
    }

    println("\nAnalysis Results:")
    println(f"   Valid entries: $validEntries/100")
    println(f"   Parse success rate: ${validEntries.toDouble / 100 * 100}%.1f%%")

    println("\nStatus Code Distribution:")
    statusCounts.toSeq.sortBy(_._1).foreach { case (statusClass, count) =>
      val percentage = count.toDouble / validEntries * 100
      println(f"   $statusClass: $count (${percentage}%.1f%%)")
    }

    println("\nHTTP Methods:")
    methodCounts.toSeq.sortBy(_._1).foreach { case (method, count) =>
      println(f"   $method: $count")
    }

    println("\nTop Endpoints:")
    endpointCounts.toSeq.sortBy(-_._2).take(10).foreach { case (endpoint, count) =>
      println(f"   $endpoint: $count")
    }

    // Error analysis
    val errorCount       = statusCounts.getOrElse("4xx", 0) + statusCounts.getOrElse("5xx", 0)
    val serverErrorCount = statusCounts.getOrElse("5xx", 0)

    println("\nError Analysis:")
    println(f"   Total errors (4xx+5xx): $errorCount")
    println(f"   Server errors (5xx): $serverErrorCount")
    val errorRate = if (validEntries > 0) errorCount.toDouble / validEntries * 100 else 0.0
    println(f"   Error rate: ${errorRate}%.1f%%")

    println("\nLogParser test complete!")

    // Performance test
    println("\nPerformance Test:")
    val startTime = System.nanoTime()

    var validCount = 0
    lines.foreach { line =>
      if (logPattern.findFirstMatchIn(line.trim).isDefined) {
        validCount += 1
      }
    }

    val endTime         = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9
    val throughput      = lines.length / durationSeconds

    println(f"   Processed ${lines.length} lines in ${durationSeconds}%.3f seconds")
    println(f"   Throughput: ${throughput}%.0f lines/second")
    println(
      f"   Valid entries: $validCount/${lines.length} (${validCount.toDouble / lines.length * 100}%.1f%%)"
    )
  }
}
