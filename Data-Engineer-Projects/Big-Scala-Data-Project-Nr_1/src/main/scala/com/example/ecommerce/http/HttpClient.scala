package com.example.ecommerce.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.Materializer
import spray.json._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory

case class HttpClientConfig(
  baseUrl: String = sys.env.getOrElse("ECOM_API_BASE", "https://dummyjson.com"),
  apiKey: Option[String] = sys.env.get("ECOM_API_KEY"),
  maxRps: Int = sys.env.get("RPS").flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(2),
  timeout: FiniteDuration = sys.env.get("TIMEOUT").flatMap(s => scala.util.Try(s.toInt).toOption).map(_.seconds).getOrElse(30.seconds),
  maxRetries: Int = 3,
  initialRetryDelay: FiniteDuration = 1.second
)

class HttpClient(config: HttpClientConfig = HttpClientConfig())(
  implicit system: ActorSystem,
  materializer: Materializer,
  ec: ExecutionContext
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val requestCount = new AtomicLong(0)
  private val lastRequestTime = new AtomicLong(System.nanoTime())

  private def rateLimit(): Unit = {
    val now = System.nanoTime()
    val timeSinceLastRequest = now - lastRequestTime.get()
    val minIntervalNanos = (1.0 / config.maxRps * 1e9).toLong

    if (timeSinceLastRequest < minIntervalNanos) {
      val sleepTime = minIntervalNanos - timeSinceLastRequest
      Thread.sleep(sleepTime / 1000000) // Convert to milliseconds
    }
    lastRequestTime.set(System.nanoTime())
  }

  private def exponentialBackoff(attempt: Int): FiniteDuration = {
    config.initialRetryDelay * math.pow(2, attempt - 1).toInt
  }

  private def buildRequest(url: String): HttpRequest = {
    val fullUrl = if (url.startsWith("http")) url else s"${config.baseUrl}$url"
    val request = HttpRequest(uri = fullUrl)
      .withMethod(HttpMethods.GET)
      .withHeaders(
        config.apiKey.map(key => RawHeader("Authorization", s"Bearer $key")).toList: _*
      )

    logger.debug(s"Built request: ${request.method} ${request.uri}")
    request
  }

  def get(url: String): Future[String] = {
    rateLimit()

    def attempt(attemptNum: Int): Future[String] = {
      val request = buildRequest(url)

      Http().singleRequest(request)
        .flatMap { response =>
          val entityFuture = response.entity.toStrict(config.timeout).map(_.data.utf8String)

          entityFuture.map { body =>
            if (response.status.isSuccess()) {
              logger.debug(s"Request successful: ${response.status} for $url")
              body
            } else {
              logger.warn(s"Request failed: ${response.status} for $url, body: $body")
              throw new RuntimeException(s"HTTP ${response.status}: $body")
            }
          }
        }
        .recoverWith {
          case ex: Exception if attemptNum < config.maxRetries =>
            val delay = exponentialBackoff(attemptNum)
            logger.warn(s"Request failed (attempt $attemptNum), retrying in $delay: ${ex.getMessage}")
            Thread.sleep(delay.toMillis)
            attempt(attemptNum + 1)

          case ex: Exception =>
            logger.error(s"Request failed after ${config.maxRetries} attempts: ${ex.getMessage}")
            Future.failed(ex)
        }
    }

    attempt(1)
  }

  def shutdown(): Future[Unit] = {
    Http().shutdownAllConnectionPools().map(_ => ())
  }
}

object HttpClient {
  def apply(config: HttpClientConfig = HttpClientConfig())(
    implicit system: ActorSystem,
    materializer: Materializer,
    ec: ExecutionContext
  ): HttpClient = new HttpClient(config)
}