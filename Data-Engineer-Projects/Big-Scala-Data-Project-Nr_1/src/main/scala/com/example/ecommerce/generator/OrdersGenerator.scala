package com.example.ecommerce.generator

import spray.json._
import spray.json.DefaultJsonProtocol._
import scala.util.Random
import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.UUID
import org.slf4j.LoggerFactory
import scala.collection.mutable.ListBuffer

case class OrderEvent(
  order_id: String,
  user_id: String,
  product_id: Int,
  quantity: Int,
  price: Double,
  event_time: Long
)

object OrderEventJsonProtocol extends DefaultJsonProtocol {
  implicit val orderEventFormat: RootJsonFormat[OrderEvent] = jsonFormat6(OrderEvent)
}

case class GeneratorConfig(
  rate: Int = 5, // events per second
  burst: Int = 0, // burst factor (0 = steady)
  outputDir: String = "data/ecommerce/orders_incoming",
  duration: Option[Int] = None // seconds to run, None = infinite
)

object OrdersGenerator {
  private val logger = LoggerFactory.getLogger(getClass)
  private val random = new Random()

  // Sample data
  private val users = (1 to 1000).map(i => f"user_$i%04d").toArray
  private val products = (1 to 100).toArray
  private val prices = Array(9.99, 19.99, 29.99, 49.99, 79.99, 99.99, 149.99, 199.99, 299.99, 499.99)

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    logger.info(s"Starting OrdersGenerator with config: $config")

    runGenerator(config)
  }

  private def parseArgs(args: Array[String]): GeneratorConfig = {
    def parseHelper(remaining: List[String], config: GeneratorConfig): GeneratorConfig = remaining match {
      case "--rate" :: rate :: tail => parseHelper(tail, config.copy(rate = rate.toInt))
      case "--burst" :: burst :: tail => parseHelper(tail, config.copy(burst = burst.toInt))
      case "--out" :: out :: tail => parseHelper(tail, config.copy(outputDir = out))
      case "--duration" :: duration :: tail => parseHelper(tail, config.copy(duration = Some(duration.toInt)))
      case Nil => config
      case other => throw new IllegalArgumentException(s"Unknown argument: $other")
    }

    parseHelper(args.toList, GeneratorConfig())
  }

  private def runGenerator(config: GeneratorConfig): Unit = {
    Files.createDirectories(Paths.get(config.outputDir))

    val startTime = System.currentTimeMillis()
    var eventCount = 0
    var fileIndex = 0

    val durationMs = config.duration.map(_ * 1000L).getOrElse(Long.MaxValue)

    try {
      while (System.currentTimeMillis() - startTime < durationMs) {
        val eventsPerSecond = if (config.burst > 0) {
          config.rate + random.nextInt(config.burst + 1)
        } else {
          config.rate
        }

        val secondStart = System.currentTimeMillis()

        // Generate events for this second
        val events = generateEvents(eventsPerSecond, secondStart)
        writeEventsToFile(config.outputDir, fileIndex, events)

        eventCount += events.size
        fileIndex += 1

        logger.info(s"Generated ${events.size} events (total: $eventCount)")

        // Sleep to maintain rate
        val elapsed = System.currentTimeMillis() - secondStart
        val targetMs = 1000L
        if (elapsed < targetMs) {
          Thread.sleep(targetMs - elapsed)
        }
      }

      logger.info(s"Generator completed. Total events: $eventCount")

    } catch {
      case ex: InterruptedException =>
        logger.info("Generator interrupted")
      case ex: Exception =>
        logger.error("Generator failed", ex)
        throw ex
    }
  }

  private def generateEvents(count: Int, baseTime: Long): Seq[OrderEvent] = {
    (1 to count).map { _ =>
      val eventTime = baseTime + random.nextInt(1000) // Spread within the second
      OrderEvent(
        order_id = UUID.randomUUID().toString,
        user_id = users(random.nextInt(users.length)),
        product_id = products(random.nextInt(products.length)),
        quantity = random.nextInt(5) + 1, // 1-5 items
        price = prices(random.nextInt(prices.length)),
        event_time = eventTime
      )
    }
  }

  private def writeEventsToFile(outputDir: String, fileIndex: Int, events: Seq[OrderEvent]): Unit = {
    import OrderEventJsonProtocol._

    val filename = f"orders_$fileIndex%05d.ndjson"
    val filePath = Paths.get(outputDir, filename).toFile
    val writer = new PrintWriter(filePath)

    try {
      events.foreach { event =>
        val json = event.toJson.compactPrint
        writer.println(json)
      }
    } finally {
      writer.close()
    }

    logger.debug(s"Wrote ${events.size} events to ${filePath.getName}")
  }
}