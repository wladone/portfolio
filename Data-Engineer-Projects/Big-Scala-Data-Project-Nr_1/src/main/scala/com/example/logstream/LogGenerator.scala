package com.example.logstream

import scala.util.Random
import java.time.{ LocalDateTime, ZoneOffset, format }
import java.time.format.DateTimeFormatter
import java.nio.file.{ Files, Paths, StandardOpenOption }
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ListBuffer

/** Log Generator for Apache Combined Log Format
  *
  * Generates realistic log entries for testing the log analytics pipeline. Supports various HTTP
  * methods, status codes, endpoints, and realistic user agents.
  */
object LogGenerator {

  // Common HTTP methods
  private val HttpMethods = Array("GET", "POST", "PUT", "DELETE", "HEAD", "PATCH", "OPTIONS")

  // Common endpoints
  private val Endpoints = Array(
    "/",
    "/index.html",
    "/api/users",
    "/api/users/{id}",
    "/api/posts",
    "/api/posts/{id}",
    "/api/search",
    "/health",
    "/metrics",
    "/static/css/main.css",
    "/static/js/app.js",
    "/favicon.ico",
    "/robots.txt",
    "/sitemap.xml"
  )

  // Common status codes with realistic distribution
  private val StatusCodes = Array(200, 201, 204, 301, 302, 400, 401, 403, 404, 405, 500, 502, 503)

  // Realistic user agents
  private val UserAgents = Array(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36",
    "curl/7.68.0",
    "python-requests/2.25.1",
    "Go-http-client/1.1",
    "HealthCheck/1.0",
    "Prometheus/2.26.0",
    "ELB-HealthChecker/2.0"
  )

  // Common referers
  private val Referers = Array(
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://example.com/",
    "https://example.com/api",
    "-",
    "http://localhost:3000/",
    "https://developer.mozilla.org/",
    "https://stackoverflow.com/"
  )

  // Date formatter for Apache log format
  private val ApacheDateFormatter =
    DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", java.util.Locale.ENGLISH)

  /** Configuration for log generation
    */
  case class Config(
      outputFile: String = "input/access.log",
      numLines: Int = 100,
      mode: String = "steady",
      burstDuration: Int = 60,
      realisticDurationMinutes: Int = 10,
      seed: Option[Long] = None,
      append: Boolean = false
  )

  /** Main entry point for log generation
    */
  def main(args: Array[String]): Unit = {
    val config    = parseArgs(args)
    val generator = new LogGenerator(config.seed)

    val entries = config.mode match {
      case "burst" => generator.generateBurst(config.numLines, config.burstDuration)
      case "realistic" =>
        generator.generateRealisticTraffic(config.numLines, config.realisticDurationMinutes)
      case _ => generator.generateSteady(config.numLines)
    }

    generator.writeToFile(entries, config.outputFile, config.append)

    println(s"Generated ${entries.length} log entries in ${config.mode} mode")
    println(s"Output written to: ${config.outputFile}")

    // Print sample entries
    println("\nSample entries:")
    entries.take(3).zipWithIndex.foreach { case (entry, i) =>
      println(f"  ${i + 1}%2d: $entry")
    }
    if (entries.length > 3) {
      println(f"  ... and ${entries.length - 3} more entries")
    }
  }

  /** Parse command line arguments
    */
  private def parseArgs(args: Array[String]): Config = {
    def parseArgsRec(args: List[String], config: Config): Config = args match {
      case "--output" :: output :: tail => parseArgsRec(tail, config.copy(outputFile = output))
      case "-o" :: output :: tail       => parseArgsRec(tail, config.copy(outputFile = output))
      case "--lines" :: lines :: tail   => parseArgsRec(tail, config.copy(numLines = lines.toInt))
      case "-n" :: lines :: tail        => parseArgsRec(tail, config.copy(numLines = lines.toInt))
      case "--mode" :: mode :: tail     => parseArgsRec(tail, config.copy(mode = mode))
      case "-m" :: mode :: tail         => parseArgsRec(tail, config.copy(mode = mode))
      case "--duration" :: duration :: tail =>
        parseArgsRec(tail, config.copy(burstDuration = duration.toInt))
      case "-d" :: duration :: tail =>
        parseArgsRec(tail, config.copy(burstDuration = duration.toInt))
      case "--duration-minutes" :: duration :: tail =>
        parseArgsRec(tail, config.copy(realisticDurationMinutes = duration.toInt))
      case "--seed" :: seed :: tail => parseArgsRec(tail, config.copy(seed = Some(seed.toLong)))
      case "-s" :: seed :: tail     => parseArgsRec(tail, config.copy(seed = Some(seed.toLong)))
      case "--append" :: tail       => parseArgsRec(tail, config.copy(append = true))
      case "-a" :: tail             => parseArgsRec(tail, config.copy(append = true))
      case _ :: tail                => parseArgsRec(tail, config) // Skip unknown args
      case Nil                      => config
    }

    parseArgsRec(args.toList, Config())
  }
}

class LogGenerator(seed: Option[Long] = None) {
  private val random = seed match {
    case Some(s) => new Random(s)
    case None    => new Random()
  }

  private val startTime = LocalDateTime.now(ZoneOffset.UTC)

  /** Generate a realistic IP address
    */
  private def generateIp(): String =
    f"192.168.${random.nextInt(256)}.${random.nextInt(256)}"

  /** Generate timestamp string in Apache log format
    */
  private def generateTimestamp(offsetSeconds: Int = 0): String = {
    val timestamp     = startTime.plusSeconds(offsetSeconds)
    val zonedDateTime = timestamp.atZone(ZoneOffset.UTC)
    s"[${zonedDateTime.format(LogGenerator.ApacheDateFormatter)}]"
  }

  /** Generate HTTP request line
    */
  private def generateRequestLine(): String = {
    val method   = LogGenerator.HttpMethods(random.nextInt(LogGenerator.HttpMethods.length))
    var endpoint = LogGenerator.Endpoints(random.nextInt(LogGenerator.Endpoints.length))

    // Add query parameters for some GET requests
    if (method == "GET" && random.nextDouble() < 0.3) {
      val queryParams = Array("q=test", "page=1", "limit=10", "sort=date", "filter=active")
      val param       = queryParams(random.nextInt(queryParams.length))
      if (endpoint.contains("?")) {
        endpoint += s"&$param"
      } else {
        endpoint += s"?$param"
      }
    }

    s""""$method $endpoint HTTP/1.1""""
  }

  /** Generate a single log entry
    */
  def generateLogEntry(offsetSeconds: Int = 0): String = {
    val ip          = generateIp()
    val timestamp   = generateTimestamp(offsetSeconds)
    val requestLine = generateRequestLine()
    val statusCode  = LogGenerator.StatusCodes(random.nextInt(LogGenerator.StatusCodes.length))

    // Generate response size based on status code
    val responseSize = statusCode match {
      case 200 | 201              => random.nextInt(10000) + 100
      case 204                    => 0
      case 301 | 302              => random.nextInt(500) + 200
      case _ if statusCode >= 400 => random.nextInt(1000) + 50
      case _                      => random.nextInt(5000) + 100
    }

    val referer   = LogGenerator.Referers(random.nextInt(LogGenerator.Referers.length))
    val userAgent = LogGenerator.UserAgents(random.nextInt(LogGenerator.UserAgents.length))

    s"""$ip - - $timestamp $requestLine $statusCode $responseSize "$referer" "$userAgent""""
  }

  /** Generate a burst of log entries over a time period
    */
  def generateBurst(numLines: Int, burstDuration: Int): Array[String] = {
    val entries = ListBuffer[String]()
    for (i <- 0 until numLines) {
      // Distribute entries over the burst duration
      val offset = random.nextInt(burstDuration + 1)
      entries += generateLogEntry(offset)
    }
    entries.toArray.sorted
  }

  /** Generate realistic traffic pattern
    */
  def generateRealisticTraffic(numLines: Int, durationMinutes: Int): Array[String] = {
    val entries = ListBuffer[String]()

    // Weights for different hours (more traffic during business hours)
    val hourWeights = Array(0.1, 0.05, 0.02, 0.01, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.35, 0.3,
      0.25, 0.3, 0.35, 0.4, 0.35, 0.3, 0.25, 0.2, 0.15, 0.1, 0.08, 0.05)

    for (i <- 0 until numLines) {
      val hour          = selectWeighted(hourWeights)
      val offsetSeconds = i * (durationMinutes * 60) / numLines + random.nextInt(61)
      entries += generateLogEntry(offsetSeconds)
    }

    entries.toArray.sorted
  }

  /** Generate steady stream of log entries
    */
  def generateSteady(numLines: Int): Array[String] =
    Array.tabulate(numLines)(i => generateLogEntry(i))

  /** Write entries to file
    */
  def writeToFile(entries: Array[String], outputFile: String, append: Boolean = false): Unit = {
    // Create output directory if it doesn't exist
    val path = Paths.get(outputFile)
    Files.createDirectories(path.getParent)

    val content = entries.mkString("\n") + "\n"
    val options = if (append) Array(StandardOpenOption.CREATE, StandardOpenOption.APPEND)
    else Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)

    Files.write(path, content.getBytes(StandardCharsets.UTF_8), options: _*)
  }

  /** Select an index based on weights
    */
  private def selectWeighted(weights: Array[Double]): Int = {
    val totalWeight      = weights.sum
    val randomValue      = random.nextDouble() * totalWeight
    var cumulativeWeight = 0.0

    for (i <- weights.indices) {
      cumulativeWeight += weights(i)
      if (randomValue <= cumulativeWeight) return i
    }

    weights.length - 1 // fallback
  }
}
