package com.example.logstream

import java.io.{ File, PrintWriter }
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source

/** Standalone LogParser test - no Spark required! This validates that your LogParser works
  * correctly without needing Spark.
  */
object LogParserStandalone {

  def main(args: Array[String]): Unit = {
    println("🧪 LogParser Standalone Test")
    println("=" * 40)

    try {
      // Test 1: Parse a single log line
      println("\n📋 Test 1: Single Log Line Parsing")
      val testLogLine =
        """192.168.1.1 - - [01/Oct/2025:11:50:00 +0000] "GET /api/test HTTP/1.1" 200 1234 "http://example.com" "Mozilla/5.0""""

      LogParser.parseLogLine(testLogLine) match {
        case Some(entry) =>
          println("✅ Successfully parsed log entry:")
          println(f"   IP: ${entry.client_ip}")
          println(f"   Method: ${LogParser.extractHttpMethod(entry.request_line)}")
          println(f"   Endpoint: ${LogParser.extractEndpoint(entry.request_line)}")
          println(f"   Status: ${entry.status_code}")
          println(f"   Status Class: ${LogParser.getStatusClass(entry.status_code)}")
          println(f"   Response Size: ${entry.response_size}")
          println(f"   Referer: ${entry.referer}")
          println(f"   User Agent: ${entry.user_agent}")
        case None =>
          println("❌ Failed to parse log line")
      }

      // Test 2: Parse multiple lines from file
      println("\n📋 Test 2: Multiple Log Lines from File")

      val inputFile = "input/access.log"
      if (new File(inputFile).exists()) {
        val source = Source.fromFile(inputFile)
        val lines  = source.getLines().take(5).toList
        source.close()

        println(s"📖 Processing first ${lines.length} lines from $inputFile:")

        lines.zipWithIndex.foreach { case (line, index) =>
          LogParser.parseLogLine(line) match {
            case Some(entry) =>
              println(f"${index + 1}. ✅ ${LogParser.extractHttpMethod(
                  entry.request_line
                )} ${LogParser.extractEndpoint(entry.request_line)} -> ${entry.status_code}")
            case None =>
              println(f"${index + 1}. ❌ Failed to parse: $line")
          }
        }

        // Test 3: Analyze status code distribution
        println("\n📋 Test 3: Status Code Analysis")

        val allLines      = Source.fromFile(inputFile).getLines().toList
        val parsedEntries = allLines.flatMap(LogParser.parseLogLine)

        println(
          s"📊 Processed ${parsedEntries.length} valid log entries out of ${allLines.length} total lines"
        )

        val statusCounts = parsedEntries.groupBy(_.status_code).mapValues(_.size)
        println("📈 Status Code Distribution:")
        statusCounts.toSeq.sortBy(_._1).foreach { case (status, count) =>
          val statusClass = LogParser.getStatusClass(status)
          println(f"   $statusClass ($status): $count entries")
        }

        val errorCount = parsedEntries.count(entry => LogParser.isErrorStatus(entry.status_code))
        val serverErrorCount =
          parsedEntries.count(entry => LogParser.isServerError(entry.status_code))

        println(f"🚨 Error Analysis: $errorCount total errors, $serverErrorCount server errors")

      } else {
        println(s"❌ Input file not found: $inputFile")
        println("💡 Run: python scripts/generate_logs.py --output input/access.log --lines 100")
      }

      // Test 4: Performance test
      println("\n📋 Test 4: Performance Test")

      val source   = Source.fromFile("input/access.log")
      val allLines = source.getLines().toList
      source.close()

      val startTime    = System.currentTimeMillis()
      val validEntries = allLines.flatMap(LogParser.parseLogLine)
      val endTime      = System.currentTimeMillis()

      val throughput = allLines.length.toDouble / (endTime - startTime) * 1000
      println(f"⚡ Processed ${allLines.length} lines in ${endTime - startTime}ms")
      println(f"📊 Throughput: ${throughput.toInt} lines/second")
      println(f"✅ Valid entries: ${validEntries.length}/${allLines.length}")

      println("\n🎉 LogParser validation complete!")

    } catch {
      case e: Exception =>
        println(s"❌ Error during testing: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
