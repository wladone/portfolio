package com.example.ecommerce.dashboard

import com.example.ecommerce.model.Schemas
import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import spray.json._
import spray.json.DefaultJsonProtocol._
import scala.io.StdIn
import scala.concurrent.{ Future, ExecutionContext }
import scala.concurrent.duration._
import java.nio.file.{ Files, Paths }
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters._
import java.util.concurrent.ConcurrentHashMap
import scala.util.{ Try, Success, Failure }

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._

/** E-Commerce Analytics Dashboard
  *
  * Web dashboard for analyzing e-commerce product data
  * with real-time metrics and visualizations.
  */
object EcommerceDashboard {

  // Data structures for JSON responses
  case class ProductStats(
      totalProducts: Int,
      totalBrands: Int,
      totalCategories: Int,
      avgPrice: Double,
      avgRating: Double,
      priceRange: scala.collection.immutable.Map[String, Int],
      topBrands: List[(String, Int)],
      topCategories: List[(String, Int)],
      ratingDistribution: scala.collection.immutable.Map[String, Int],
      lastUpdated: Long = System.currentTimeMillis()
  )

  case class AnalysisResult(
      stats: ProductStats,
      sampleProducts: List[scala.collection.immutable.Map[String, JsValue]],
      lastUpdated: Long = System.currentTimeMillis()
  )

  // JSON formats
  implicit val productStatsFormat: RootJsonFormat[ProductStats] = jsonFormat10(ProductStats.apply)
  implicit val analysisResultFormat: RootJsonFormat[AnalysisResult] = jsonFormat3(AnalysisResult.apply)

  // Cache for analysis results
  private val analysisCache = new ConcurrentHashMap[String, (AnalysisResult, Long)]()
  private val CACHE_DURATION_MS = 30000L // 30 seconds cache

  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem(Behaviors.empty, "ecommerce-dashboard")
    implicit val executionContext = system.executionContext

    val route =
      pathSingleSlash {
        get {
          getFromResource("static/ecommerce.html")
        }
      } ~
        path("api" / "analyze") {
          get {
            parameters("source".optional, "run_date".optional) { (sourceParam, runDateParam) =>
              val source = sourceParam.getOrElse("dummyjson")
              val runDate = runDateParam.getOrElse("latest")
              val futureResult = getCachedAnalysis(source, runDate)
              onSuccess(futureResult) { result =>
                complete(result)
              }
            }
          }
        } ~
        path("api" / "sources") {
          get {
            val sources = getAvailableSources()
            complete(scala.collection.immutable.Map("sources" -> sources))
          }
        } ~
        pathPrefix("static") {
          getFromResourceDirectory("static")
        }

    println("Attempting to bind server to 0.0.0.0:8081...")

    val bindingFuture = Http().newServerAt("0.0.0.0", 8081).bind(route)

    // Wait for binding to complete
    import scala.concurrent.Await
    val binding = try {
      val result = Await.result(bindingFuture, 30.seconds)
      println(s"✅ Server successfully bound to ${result.localAddress}")
      result
    } catch {
      case ex: Exception =>
        println(s"❌ Server binding failed: ${ex.getMessage}")
        ex.printStackTrace()
        System.exit(1)
        null
    }

    println("🛒 E-Commerce Analytics Dashboard online at http://localhost:8081/")
    println("Dashboard will run for 5 minutes...")

    // Run for 5 minutes to allow testing
    Thread.sleep(300000)

    println("Shutting down server...")
    binding.unbind()
    system.terminate()
  }

  /** Get cached analysis result or compute new one */
  def getCachedAnalysis(source: String, runDate: String)(
      implicit ec: ExecutionContext
  ): Future[AnalysisResult] = Future {
    val cacheKey = s"$source:$runDate"
    val now = System.currentTimeMillis()

    // Check cache
    val cached = analysisCache.get(cacheKey)
    if (cached != null && (now - cached._2) < CACHE_DURATION_MS) {
      cached._1
    } else {
      // Compute new analysis
      val result = analyzeEcommerceData(source, runDate)

      // Cache the result
      analysisCache.put(cacheKey, (result, now))

      // Clean old cache entries
      val cutoff = now - CACHE_DURATION_MS
      analysisCache.entrySet().removeIf(_.getValue._2 < cutoff)

      result
    }
  }

  /** Analyze e-commerce data from Parquet files */
  def analyzeEcommerceData(source: String, runDate: String): AnalysisResult = {
    println(s"Starting analysis for source: $source, runDate: $runDate")

    val spark = SparkSession.builder()
      .appName("Ecommerce Analysis")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false") // Disable adaptive query execution for compatibility
      .getOrCreate()

    try {
      println("Spark session created successfully")
      // Determine data path
      val dataPath = if (runDate == "latest") {
        s"data/ecommerce/silver/products/ingest_date=*"
      } else {
        s"data/ecommerce/silver/products/ingest_date=$runDate"
      }

      // Read data
      println(s"Trying to read data from path: $dataPath")
      val df = Try(spark.read.parquet(dataPath)) match {
        case Success(data) =>
          val count = data.count()
          println(s"Successfully read $count records from Parquet")
          if (count > 0) data else throw new Exception("No data in Parquet")
        case Failure(parquetEx) =>
          println(s"Parquet read failed: ${parquetEx.getMessage}")
          // Fallback to raw NDJSON data
          val rawPath = s"data/ecommerce/raw/source=$source/run_date=*/*.ndjson"
          println(s"Trying fallback to raw data: $rawPath")
          Try(spark.read.json(rawPath)) match {
            case Success(data) =>
              val count = data.count()
              println(s"Successfully read $count records from raw JSON")
              if (count > 0) {
                // Add missing metadata fields for raw data
                data.withColumn("_source", F.lit(source))
                   .withColumn("run_date", F.lit(runDate).cast("string"))
                   .withColumn("_ingest_ts", F.lit(System.currentTimeMillis()))
              } else {
                throw new Exception("No data in raw JSON")
              }
            case Failure(jsonEx) =>
              println(s"JSON read also failed: ${jsonEx.getMessage}")
              // Return empty analysis
              return AnalysisResult(
                ProductStats(0, 0, 0, 0.0, 0.0, scala.collection.immutable.Map.empty[String, Int], List.empty, List.empty, scala.collection.immutable.Map.empty[String, Int]),
                List.empty
              )
          }
      }

      // Calculate statistics
      val totalProducts = df.count().toInt
      val totalBrands = df.select("brand").distinct().count().toInt
      val totalCategories = df.select("category").distinct().count().toInt
      val avgPrice = df.agg(F.avg("price")).first().getDouble(0)
      val avgRating = df.agg(F.avg("rating")).first().getDouble(0)

      // Price ranges
      val priceRanges = df.withColumn("price_range",
        F.when(F.col("price") < 10, "< $10")
         .when(F.col("price") < 50, "$10-$49")
         .when(F.col("price") < 100, "$50-$99")
         .when(F.col("price") < 500, "$100-$499")
         .otherwise("$500+")
      ).groupBy("price_range").count()
       .collect()
       .map(row => row.getString(0) -> row.getLong(1).toInt)
       .toMap

      // Top brands
      val topBrands = df.groupBy("brand")
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
        .collect()
        .map(row => row.getString(0) -> row.getLong(1).toInt)
        .toList

      // Top categories
      val topCategories = df.groupBy("category")
        .count()
        .orderBy(F.desc("count"))
        .limit(10)
        .collect()
        .map(row => row.getString(0) -> row.getLong(1).toInt)
        .toList

      // Rating distribution
      val ratingDist = df.withColumn("rating_range",
        F.when(F.col("rating") >= 4.5, "4.5-5.0")
         .when(F.col("rating") >= 4.0, "4.0-4.4")
         .when(F.col("rating") >= 3.5, "3.5-3.9")
         .when(F.col("rating") >= 3.0, "3.0-3.4")
         .otherwise("< 3.0")
      ).groupBy("rating_range").count()
       .collect()
       .map(row => row.getString(0) -> row.getLong(1).toInt)
       .toMap

      // Sample products
      val sampleProducts = df.limit(5)
        .collect()
        .map(row => Map(
          "id" -> JsNumber(row.getInt(0)),
          "title" -> JsString(row.getString(1)),
          "price" -> JsNumber(row.getDouble(3)),
          "rating" -> JsNumber(row.getDouble(5)),
          "brand" -> JsString(Option(row.getString(6)).getOrElse("")),
          "category" -> JsString(row.getString(7))
        ))
        .toList

      val stats = ProductStats(
        totalProducts,
        totalBrands,
        totalCategories,
        avgPrice,
        avgRating,
        priceRanges.toMap,
        topBrands,
        topCategories,
        ratingDist.toMap
      )

      AnalysisResult(stats, sampleProducts.map(_.toMap))

    } catch {
      case ex: Exception =>
        println(s"Analysis failed with exception: ${ex.getMessage}")
        ex.printStackTrace()
        // Return empty analysis on error
        AnalysisResult(
          ProductStats(0, 0, 0, 0.0, 0.0, scala.collection.immutable.Map.empty[String, Int], List.empty, List.empty, scala.collection.immutable.Map.empty[String, Int]),
          List.empty
        )
    } finally {
      try {
        spark.stop()
        println("Spark session stopped")
      } catch {
        case ex: Exception =>
          println(s"Error stopping Spark session: ${ex.getMessage}")
      }
    }
  }

  /** Get available data sources */
  def getAvailableSources(): List[String] = {
    val rawPath = Paths.get("data/ecommerce/raw")
    if (Files.exists(rawPath)) {
      import scala.collection.JavaConverters._
      Files.list(rawPath).iterator().asScala
        .filter(Files.isDirectory(_))
        .map(_.getFileName.toString)
        .filter(_.startsWith("source="))
        .map(_.substring(7)) // Remove "source=" prefix
        .toList
    } else {
      List("dummyjson", "fakestore")
    }
  }
}