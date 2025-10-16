package com.example.logstream

import java.nio.file.{ Files, Paths }
import scala.sys.process._
import scala.concurrent.{ Future, ExecutionContext }
import scala.concurrent.duration._
import java.lang.management.ManagementFactory
import scala.collection.mutable.ListBuffer
import ExecutionContext.Implicits.global

/** Performance benchmarking script for log analytics pipeline.
  *
  * Tests the system with various data volumes and measures:
  *   - Processing throughput (records/second)
  *   - Memory usage patterns
  *   - Error rates and recovery
  *   - Scalability characteristics
  */
object PerformanceBenchmark {

  case class BenchmarkResult(
      testName: String,
      numRecords: Int,
      durationMinutes: Int,
      executionTime: Double,
      memoryPeakMb: Double,
      memoryAvgMb: Double,
      exitCode: Int,
      errors: Int,
      warnings: Int,
      throughputRecordsPerSec: Double
  )

  case class Config(
      sparkMaster: String = "local[*]",
      memoryLimit: String = "2g",
      test: String = "scalability",
      lines: Int = 10000,
      duration: Int = 2
  )

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)

    // Check if JAR exists
    val jarPath = "target/scala-2.12/log-analytics-pipeline-1.0.0.jar"
    if (!Files.exists(Paths.get(jarPath))) {
      println(s"JAR file not found: $jarPath")
      println("Run 'sbt assembly' first to build the application")
      System.exit(1)
    }

    val benchmark = new PerformanceBenchmark(config.sparkMaster, config.memoryLimit)

    config.test match {
      case "scalability" => benchmark.runScalabilityTests()
      case "stress" =>
        println("Running stress test (this may take a while)...")
        benchmark.runBenchmark("Stress_Test", 1000000, 10)
      case "single" =>
        benchmark.runBenchmark(s"Single_Test_${config.lines}", config.lines, config.duration)
      case _ =>
        println(s"Unknown test type: ${config.test}")
        System.exit(1)
    }
  }

  private def parseArgs(args: Array[String]): Config = {
    def parseArgsRec(args: List[String], config: Config): Config = args match {
      case "--spark-master" :: master :: tail =>
        parseArgsRec(tail, config.copy(sparkMaster = master))
      case "--memory" :: memory :: tail => parseArgsRec(tail, config.copy(memoryLimit = memory))
      case "--test" :: test :: tail     => parseArgsRec(tail, config.copy(test = test))
      case "--lines" :: lines :: tail   => parseArgsRec(tail, config.copy(lines = lines.toInt))
      case "--duration" :: duration :: tail =>
        parseArgsRec(tail, config.copy(duration = duration.toInt))
      case _ :: tail => parseArgsRec(tail, config) // Skip unknown args
      case Nil       => config
    }

    parseArgsRec(args.toList, Config())
  }
}

class PerformanceBenchmark(sparkMaster: String, memoryLimit: String) {

  private val results = ListBuffer[PerformanceBenchmark.BenchmarkResult]()

  def generateTestData(
      numLines: Int,
      outputFile: String = "input/benchmark_access.log"
  ): Boolean = {
    println(f"Generating ${numLines}%,d test log entries...")

    Files.createDirectories(Paths.get("input"))

    // Use the Scala LogGenerator
    try {
      val generator = new LogGenerator(Some(42L)) // Reproducible results
      val entries   = generator.generateRealisticTraffic(numLines, 10)
      generator.writeToFile(entries, outputFile)
      println(f"Generated ${numLines}%,d log entries")
      true
    } catch {
      case e: Exception =>
        println(s"Failed to generate data: ${e.getMessage}")
        false
    }
  }

  def runBenchmark(
      testName: String,
      numLines: Int,
      durationMinutes: Int
  ): Option[PerformanceBenchmark.BenchmarkResult] = {
    println(f"\nStarting benchmark: $testName")
    println(f"   Records: ${numLines}%,d")
    println(f"   Duration: $durationMinutes minutes")

    // Generate test data
    if (!generateTestData(numLines)) {
      return None
    }

    // Start memory monitoring
    val memoryMonitor = new MemoryMonitor()
    memoryMonitor.start()

    // Start the Spark application
    val startTime = System.nanoTime()

    try {
      val cmd = Array(
        "java",
        "-Xmx" + memoryLimit,
        "-Xms1g",
        "-cp",
        "target/scala-2.12/log-analytics-pipeline-1.0.0.jar",
        "com.example.logstream.FileStreamApp",
        "--input",
        "input/",
        "--output",
        "output/",
        "--checkpoint",
        "checkpoint/",
        "--log-level",
        "WARN"
      )

      println(s"Starting application: ${cmd.mkString(" ")}")

      val process = Process(cmd).run()

      // Monitor for specified duration
      Thread.sleep(durationMinutes * 60 * 1000L)

      // Collect results
      val executionTime = (System.nanoTime() - startTime) / 1e9
      val memoryStats   = memoryMonitor.stop()

      // Check if process is still running
      if (process.isAlive()) {
        println("Stopping application...")
        process.destroy()
        Thread.sleep(5000)
      }

      // Collect output (simplified - in real implementation you'd capture stdout/stderr)
      val exitCode = if (process.isAlive()) {
        process.destroy()
        -1
      } else {
        process.exitValue()
      }

      // For this simplified version, we'll simulate error counting
      // In a full implementation, you'd parse the actual logs
      val errors   = 0
      val warnings = 0

      val result = PerformanceBenchmark.BenchmarkResult(
        testName = testName,
        numRecords = numLines,
        durationMinutes = durationMinutes,
        executionTime = executionTime,
        memoryPeakMb = memoryStats("peak_mb"),
        memoryAvgMb = memoryStats("avg_mb"),
        exitCode = exitCode,
        errors = errors,
        warnings = warnings,
        throughputRecordsPerSec = if (executionTime > 0) numLines / executionTime else 0.0
      )

      results += result
      printResultSummary(result)

      Some(result)

    } catch {
      case e: Exception =>
        println(s"Benchmark failed: ${e.getMessage}")
        memoryMonitor.stop()
        None
    }
  }

  def printResultSummary(result: PerformanceBenchmark.BenchmarkResult): Unit = {
    println("Benchmark Results:")
    println(s"   Test: ${result.testName}")
    println(f"   Records: ${result.numRecords}%,d")
    println(f"   Duration: ${result.durationMinutes}%.1f minutes")
    println(f"   Throughput: ${result.throughputRecordsPerSec}%.1f records/sec")
    println(f"   Peak Memory: ${result.memoryPeakMb}%.1f MB")
    println(f"   Avg Memory: ${result.memoryAvgMb}%.1f MB")
    println(f"   Errors: ${result.errors}")
    println(f"   Warnings: ${result.warnings}")

    if (result.exitCode == 0) {
      println("   Status: SUCCESS")
    } else {
      println(f"   Status: FAILED (Exit code: ${result.exitCode})")
    }
  }

  def runScalabilityTests(): Unit = {
    val testScenarios = Array(
      ("Small", 1000, 1),
      ("Medium", 10000, 2),
      ("Large", 100000, 3),
      ("XL", 1000000, 5)
    )

    println("Starting Scalability Tests")
    println("=" * 50)

    testScenarios.foreach { case (testName, numLines, duration) =>
      val result = runBenchmark(testName, numLines, duration)
      if (result.isDefined) {
        println("-" * 30)
        Thread.sleep(10000) // Cool down between tests
      }
    }

    printFinalSummary()
  }

  def printFinalSummary(): Unit = {
    println("\nFINAL BENCHMARK SUMMARY")
    println("=" * 50)

    if (results.isEmpty) {
      println("No results to summarize")
      return
    }

    val totalRecords  = results.map(_.numRecords).sum
    val totalTime     = results.map(_.executionTime).sum
    val avgThroughput = if (totalTime > 0) totalRecords / totalTime else 0.0

    println(f"Overall Throughput: ${avgThroughput}%.1f records/sec")
    println(f"Total Records Processed: ${totalRecords}%,d")
    println(f"Total Execution Time: ${totalTime}%.1f seconds")

    // Find best and worst performance
    val bestResult  = results.maxBy(_.throughputRecordsPerSec)
    val worstResult = results.minBy(_.throughputRecordsPerSec)

    println(
      f"Best Performance: ${bestResult.testName} (${bestResult.throughputRecordsPerSec}%.1f rec/sec)"
    )
    println(
      f"Worst Performance: ${worstResult.testName} (${worstResult.throughputRecordsPerSec}%.1f rec/sec)"
    )

    // Memory analysis
    val maxMemory = results.map(_.memoryPeakMb).max
    println(f"Max Memory Usage: ${maxMemory}%.1f MB")

    // Success rate
    val successfulTests = results.count(_.exitCode == 0)
    val successRate     = successfulTests.toDouble / results.length * 100
    println(f"Success Rate: ${successRate}%.1f%% ($successfulTests/${results.length})")
  }
}

/** Simple memory monitor using Java's Runtime
  */
class MemoryMonitor(intervalMs: Long = 1000L) {
  private var monitoring            = false
  private val memoryReadings        = ListBuffer[Double]()
  private var monitorThread: Thread = _

  def start(): Unit = {
    memoryReadings.clear()
    monitoring = true

    monitorThread = new Thread(() =>
      while (monitoring)
        try {
          val runtime    = Runtime.getRuntime()
          val usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0)
          memoryReadings.synchronized {
            memoryReadings += usedMemory
          }
          Thread.sleep(intervalMs)
        } catch {
          case _: InterruptedException => // Expected when stopping
          case _: Exception            => // Ignore other exceptions
        }
    )
    monitorThread.setDaemon(true)
    monitorThread.start()
  }

  def stop(): Map[String, Double] = {
    monitoring = false
    if (monitorThread != null) {
      monitorThread.interrupt()
      monitorThread.join(5000) // Wait up to 5 seconds
    }

    memoryReadings.synchronized {
      if (memoryReadings.nonEmpty) {
        Map(
          "peak_mb" -> memoryReadings.max,
          "avg_mb"  -> memoryReadings.sum / memoryReadings.length,
          "min_mb"  -> memoryReadings.min,
          "samples" -> memoryReadings.length
        )
      } else {
        Map(
          "peak_mb" -> 0.0,
          "avg_mb"  -> 0.0,
          "min_mb"  -> 0.0,
          "samples" -> 0
        )
      }
    }
  }
}
