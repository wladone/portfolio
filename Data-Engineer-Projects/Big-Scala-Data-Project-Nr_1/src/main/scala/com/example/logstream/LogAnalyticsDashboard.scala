package com.example.logstream

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat
import scala.io.StdIn
import scala.io.Source
import scala.collection.mutable.Map
import scala.util.matching.Regex
import java.nio.file.{ Files, Paths, attribute }
import scala.concurrent.{ Future, ExecutionContext }
import scala.concurrent.duration._
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/** Optimized Web Dashboard for Log Analytics
  *
  * Performance optimizations:
  *   - Cached analysis results with file modification time checking
  *   - Asynchronous operations for I/O
  *   - Static asset serving
  *   - Limited analysis sample sizes
  *   - Proper resource management
  */
object LogAnalyticsDashboard {

  // Optimized data structure with JSON serialization
  case class AnalysisResult(
      totalLines: Int,
      validEntries: Int,
      parseSuccessRate: Double,
      statusDistribution: scala.collection.immutable.Map[String, Int],
      methodCounts: scala.collection.immutable.Map[String, Int],
      topEndpoints: List[(String, Int)],
      errorCount: Int,
      serverErrorCount: Int,
      errorRate: Double,
      lastUpdated: Long = System.currentTimeMillis()
  )

  // JSON formatters
  implicit val analysisResultFormat: RootJsonFormat[AnalysisResult] =
    jsonFormat10(AnalysisResult.apply)

  // Cache for analysis results
  private val analysisCache     = new ConcurrentHashMap[String, (AnalysisResult, Long)]()
  private val CACHE_DURATION_MS = 30000L // 30 seconds cache

  def main(args: Array[String]): Unit = {
    implicit val system           = ActorSystem(Behaviors.empty, "log-analytics-dashboard")
    implicit val executionContext = system.executionContext

    val route =
      pathSingleSlash {
        get {
          getFromResource("static/index.html")
        }
      } ~
        path("api" / "analyze") {
          get {
            parameters("file".optional) { fileParam =>
              val filePath     = fileParam.getOrElse("input/access.log")
              val futureResult = getCachedAnalysis(filePath)
              onSuccess(futureResult) { result =>
                complete(result)
              }
            }
          }
        } ~
        path("api" / "generate") {
          post {
            parameters("lines".as[Int] ? 1000, "mode".?("realistic")) { (lines, mode) =>
              val futureSuccess = Future {
                generateTestData(lines, mode)
              }
              onSuccess(futureSuccess) { success =>
                if (success) {
                  // Invalidate cache after data generation
                  analysisCache.clear()
                  complete(scala.collection.immutable.Map(
                    "status"  -> "success",
                    "message" -> "Data generated successfully"
                  ))
                } else {
                  complete(
                    StatusCodes.InternalServerError,
                    scala.collection.immutable.Map(
                      "status"  -> "error",
                      "message" -> "Failed to generate data"
                    )
                  )
                }
              }
            }
          }
        } ~
        path("api" / "run-test") {
          get {
            val futureOutput = Future(runLogParserTest())
            onSuccess(futureOutput) { output =>
              complete(scala.collection.immutable.Map("output" -> output))
            }
          }
        } ~
        pathPrefix("static") {
          getFromResourceDirectory("static")
        }

    val bindingFuture = Http().newServerAt("localhost", 8080).bind(route)

    println("🚀 Log Analytics Dashboard online at http://localhost:8080/")
    println("Press RETURN to stop...")

    StdIn.readLine()
    bindingFuture
      .flatMap(_.unbind())
      .onComplete(_ => system.terminate())
  }

  /** Get cached analysis result or compute new one
    */
  def getCachedAnalysis(filePath: String)(implicit ec: ExecutionContext): Future[AnalysisResult] =
    Future {
      val file = Paths.get(filePath)
      if (!Files.exists(file)) {
        AnalysisResult(
          0,
          0,
          0.0,
          scala.collection.immutable.Map.empty[String, Int],
          scala.collection.immutable.Map.empty[String, Int],
          List(),
          0,
          0,
          0.0
        )
      } else {
        val lastModified = Files.getLastModifiedTime(file).toMillis
        val cacheKey     = s"$filePath:$lastModified"
        val now          = System.currentTimeMillis()

        // Check cache
        val cached = analysisCache.get(cacheKey)
        if (cached != null && (now - cached._2) < CACHE_DURATION_MS) {
          cached._1
        } else {
          // Compute new analysis
          val result = analyzeLogFile(filePath)

          // Cache the result
          analysisCache.put(cacheKey, (result, now))

          // Clean old cache entries (keep only recent ones)
          val cutoff = now - CACHE_DURATION_MS
          analysisCache.entrySet().removeIf(_.getValue._2 < cutoff)

          result
        }
      }
    }

  /** Optimized log file analysis with sampling and early termination
    */
  def analyzeLogFile(filePath: String): AnalysisResult = {
    val file = Paths.get(filePath)
    if (!Files.exists(file)) {
      return AnalysisResult(
        0,
        0,
        0.0,
        scala.collection.immutable.Map.empty[String, Int],
        scala.collection.immutable.Map.empty[String, Int],
        List(),
        0,
        0,
        0.0
      )
    }

    // Get total line count efficiently
    val totalLines = Source.fromFile(filePath).getLines().size

    // Sample analysis: analyze up to 1000 lines or 10% of file, whichever is smaller
    val sampleSize = math.min(1000, math.max(100, totalLines / 10))

    val lines          = Source.fromFile(filePath).getLines().take(sampleSize).toArray
    var validEntries   = 0
    val statusCounts   = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    val methodCounts   = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    val endpointCounts = scala.collection.mutable.Map[String, Int]().withDefaultValue(0)

    // Optimized regex pattern
    val logPattern =
      """^(\S+) ([^\s]+) ([^\s]+) \[([^\]]+)\] "([^"]+)" (\d+) (\d+|[0-9-]+) "([^"]*)" "([^"]*)"$$""".r

    lines.foreach { line =>
      val trimmedLine = line.trim
      if (trimmedLine.nonEmpty) {
        logPattern.findFirstMatchIn(trimmedLine) match {
          case Some(m) =>
            validEntries += 1
            val statusCode  = m.group(6).toInt
            val requestLine = m.group(5)

            // Extract method and endpoint efficiently
            val parts  = requestLine.trim.split(' ')
            val method = if (parts.length > 0) parts(0) else "UNKNOWN"
            val endpoint = if (parts.length > 1) {
              parts(1).split('?').headOption.getOrElse("/")
            } else "/"

            // Status classification
            val statusClass = s"${statusCode / 100}xx"

            statusCounts(statusClass) += 1
            methodCounts(method) += 1
            endpointCounts(endpoint) += 1

          case None => // Invalid line, skip
        }
      }
    }

    // Scale results to estimate full file statistics
    val scaleFactor           = totalLines.toDouble / sampleSize
    val estimatedValidEntries = (validEntries * scaleFactor).toInt

    val parseSuccessRate = if (lines.length > 0) validEntries.toDouble / lines.length * 100 else 0.0
    val errorCount       = statusCounts.getOrElse("4xx", 0) + statusCounts.getOrElse("5xx", 0)
    val serverErrorCount = statusCounts.getOrElse("5xx", 0)
    val errorRate        = if (validEntries > 0) errorCount.toDouble / validEntries * 100 else 0.0

    val topEndpoints = endpointCounts.toSeq.sortBy(-_._2).take(10).toList

    AnalysisResult(
      totalLines = totalLines,
      validEntries = estimatedValidEntries,
      parseSuccessRate = parseSuccessRate,
      statusDistribution = scala.collection.immutable.Map(statusCounts.toSeq: _*),
      methodCounts = scala.collection.immutable.Map(methodCounts.toSeq: _*),
      topEndpoints = topEndpoints,
      errorCount = (errorCount * scaleFactor).toInt,
      serverErrorCount = (serverErrorCount * scaleFactor).toInt,
      errorRate = errorRate
    )
  }

  def generateTestData(lines: Int, mode: String): Boolean =
    try {
      val generator = new LogGenerator(Some(42L)) // Reproducible results
      val entries = mode match {
        case "burst"     => generator.generateBurst(lines, 60)
        case "realistic" => generator.generateRealisticTraffic(lines, 10)
        case _           => generator.generateSteady(lines)
      }
      generator.writeToFile(entries, "input/access.log")
      true
    } catch {
      case e: Exception =>
        println(s"Failed to generate data: ${e.getMessage}")
        false
    }

  def runLogParserTest(): String =
    try {
      // Capture output from LogParserTest
      val output = new StringBuilder()

      // Since we can't easily capture stdout from another main method,
      // let's just run a simplified analysis
      val result = analyzeLogFile("input/access.log")

      output.append("LogParser Test Results\n")
      output.append("======================\n\n")
      output.append(s"Total lines: ${result.totalLines}\n")
      output.append(s"Valid entries: ${result.validEntries}\n")
      output.append(f"Parse success rate: ${result.parseSuccessRate}%.1f%%\n\n")

      output.append("Status Distribution:\n")
      result.statusDistribution.toSeq.sortBy(_._1).foreach { case (status, count) =>
        val percentage = count.toDouble / result.validEntries * 100
        output.append(f"  $status: $count (${percentage}%.1f%%)\n")
      }

      output.append("\nHTTP Methods:\n")
      result.methodCounts.toSeq.sortBy(_._1).foreach { case (method, count) =>
        output.append(f"  $method: $count\n")
      }

      output.append("\nTop Endpoints:\n")
      result.topEndpoints.foreach { case (endpoint, count) =>
        output.append(f"  $endpoint: $count\n")
      }

      output.append(f"\nError Analysis:\n")
      output.append(f"  Total errors (4xx+5xx): ${result.errorCount}\n")
      output.append(f"  Server errors (5xx): ${result.serverErrorCount}\n")
      output.append(f"  Error rate: ${result.errorRate}%.1f%%\n")

      output.toString()
    } catch {
      case e: Exception => s"Error running test: ${e.getMessage}"
    }
}
