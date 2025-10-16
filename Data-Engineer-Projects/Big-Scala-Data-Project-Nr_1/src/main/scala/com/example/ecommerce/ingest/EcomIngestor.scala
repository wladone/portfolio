package com.example.ecommerce.ingest

import com.example.ecommerce.http.{HttpClient, HttpClientConfig}
import com.example.ecommerce.model.ProductJsonProtocol._
import spray.json._
import spray.json.DefaultJsonProtocol._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.util.{Failure, Success, Try}
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.time.{LocalDate, ZoneOffset}
import java.time.format.DateTimeFormatter
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer

case class Product(
  id: Int,
  title: String,
  description: Option[String],
  price: Double,
  discountPercentage: Option[Double],
  rating: Double,
  stock: Int,
  brand: Option[String],
  category: String,
  thumbnail: Option[String],
  images: List[String]
)

object ProductJsonProtocol extends DefaultJsonProtocol {
  implicit val productFormat: RootJsonFormat[Product] = jsonFormat11(Product)
}

case class ApiResponse(products: List[Product], total: Int, skip: Int, limit: Int)



sealed trait DataSource {
  def baseUrl: String
  def endpoint(limit: Int, skip: Int): String
  def hasMoreData(response: ApiResponse): Boolean
}

case object DummyJsonSource extends DataSource {
  val baseUrl = "https://dummyjson.com"
  def endpoint(limit: Int, skip: Int) = s"/products?limit=$limit&skip=$skip"
  def hasMoreData(response: ApiResponse) = response.skip + response.limit < response.total
}

case object FakeStoreSource extends DataSource {
  val baseUrl = "https://fakestoreapi.com"
  def endpoint(limit: Int, skip: Int) = s"/products?limit=$limit&offset=$skip"
  def hasMoreData(response: ApiResponse) = response.products.nonEmpty && response.products.size == response.limit
}

object DataSource {
  def fromString(source: String): DataSource = source.toLowerCase match {
    case "dummyjson" => DummyJsonSource
    case "fakestore" => FakeStoreSource
    case _ => throw new IllegalArgumentException(s"Unknown source: $source")
  }
}

case class IngestConfig(
  source: String = "dummyjson",
  outputDir: String = "data/ecommerce/raw",
  pageSize: Int = 100,
  maxPages: Int = 50,
  rps: Int = 2,
  runDate: LocalDate = LocalDate.now(ZoneOffset.UTC)
)

object EcomIngestor {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    implicit val ec: ExecutionContext = ExecutionContext.global

    val config = parseArgs(args)
    val source = DataSource.fromString(config.source)

    logger.info(s"Starting ingestion from ${config.source} with config: $config")

    val result = runIngestion(config, source)

    result match {
      case Success(stats) =>
        logger.info(f"Ingestion completed successfully: ${stats.rows} rows, ${stats.bytes}%,.0f bytes")
        System.exit(0)
      case Failure(ex) =>
        logger.error(s"Ingestion failed: ${ex.getMessage}", ex)
        System.exit(1)
    }
  }

  private def parseArgs(args: Array[String]): IngestConfig = {
    def parseHelper(remaining: List[String], config: IngestConfig): IngestConfig = remaining match {
      case "--source" :: source :: tail => parseHelper(tail, config.copy(source = source))
      case "--out" :: out :: tail => parseHelper(tail, config.copy(outputDir = out))
      case "--page-size" :: size :: tail => parseHelper(tail, config.copy(pageSize = size.toInt))
      case "--max-pages" :: max :: tail => parseHelper(tail, config.copy(maxPages = max.toInt))
      case "--rps" :: rps :: tail => parseHelper(tail, config.copy(rps = rps.toInt))
      case "--run-date" :: date :: tail =>
        val parsedDate = LocalDate.parse(date, DateTimeFormatter.ISO_LOCAL_DATE)
        parseHelper(tail, config.copy(runDate = parsedDate))
      case Nil => config
      case other => throw new IllegalArgumentException(s"Unknown argument: $other")
    }

    parseHelper(args.toList, IngestConfig())
  }

  case class IngestStats(rows: Int, bytes: Long)

  private def runIngestion(config: IngestConfig, source: DataSource)(
    implicit ec: ExecutionContext
  ): Try[IngestStats] = {
    import ProductJsonProtocol._

    // Define ApiResponse format after Product format is available
    implicit val apiResponseFormat: RootJsonFormat[ApiResponse] = new RootJsonFormat[ApiResponse] {
      def write(r: ApiResponse): JsValue = JsObject(
        "products" -> JsArray(Vector(r.products.map(_.toJson): _*)),
        "total" -> JsNumber(r.total),
        "skip" -> JsNumber(r.skip),
        "limit" -> JsNumber(r.limit)
      )

      def read(value: JsValue): ApiResponse = value match {
        case JsObject(fields) =>
          ApiResponse(
            products = fields("products").convertTo[List[Product]],
            total = fields("total").convertTo[Int],
            skip = fields("skip").convertTo[Int],
            limit = fields("limit").convertTo[Int]
          )
        case _ => deserializationError("ApiResponse expected")
      }
    }

    val httpConfig = HttpClientConfig(
      baseUrl = source.baseUrl,
      maxRps = config.rps
    )

    val system = akka.actor.ActorSystem("ecommerce-ingestor")
    implicit val materializer: akka.stream.Materializer = akka.stream.Materializer(system)

    try {
      val client = HttpClient(httpConfig)(system, materializer, ec)

      val outputPath = Paths.get(config.outputDir, s"source=${config.source}",
        s"run_date=${config.runDate.format(DateTimeFormatter.ISO_LOCAL_DATE)}")
      Files.createDirectories(outputPath)

      val partFile = outputPath.resolve("part-00000.ndjson").toFile
      val writer = new PrintWriter(partFile)

      var totalRows = 0
      var page = 0
      var hasMore = true

      while (hasMore && page < config.maxPages) {
        val skip = page * config.pageSize
        val url = source.endpoint(config.pageSize, skip)

        logger.info(s"Fetching page ${page + 1}, skip=$skip, limit=${config.pageSize}")

        val responseFuture = client.get(url).map(_.parseJson.convertTo[ApiResponse])

        val response = Await.result(responseFuture, 60.seconds)

        response.products.foreach { product =>
          val json = product.toJson.compactPrint
          writer.println(json)
          totalRows += 1
        }

        hasMore = source.hasMoreData(response)
        page += 1

        logger.info(s"Processed page ${page}, ${response.products.size} products, total rows: $totalRows")
      }

      writer.close()

      val bytes = partFile.length()
      logger.info(f"Wrote $totalRows rows (${bytes}%,.0f bytes) to ${partFile.getPath}")

      Success(IngestStats(totalRows, bytes))
    } catch {
      case ex: Exception =>
        logger.error("Ingestion failed", ex)
        Failure(ex)
    } finally {
      system.terminate()
    }
  }
}