package com.example.ecommerce.model

import org.apache.spark.sql.types._
import spray.json._
import spray.json.DefaultJsonProtocol._
import java.time.Instant

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
  images: List[String],
  _source: String,
  run_date: String,
  _ingest_ts: Long
)

case class Order(
  order_id: String,
  user_id: String,
  product_id: Int,
  quantity: Int,
  price: Double,
  event_time: Long
)

object ProductJsonProtocol extends DefaultJsonProtocol {
  implicit val productFormat: RootJsonFormat[Product] = new RootJsonFormat[Product] {
    def write(p: Product): JsValue = JsObject(
      "id" -> JsNumber(p.id),
      "title" -> JsString(p.title),
      "description" -> p.description.map(JsString(_)).getOrElse(JsNull),
      "price" -> JsNumber(p.price),
      "discountPercentage" -> p.discountPercentage.map(JsNumber(_)).getOrElse(JsNull),
      "rating" -> JsNumber(p.rating),
      "stock" -> JsNumber(p.stock),
      "brand" -> p.brand.map(JsString(_)).getOrElse(JsNull),
      "category" -> JsString(p.category),
      "thumbnail" -> p.thumbnail.map(JsString(_)).getOrElse(JsNull),
      "images" -> JsArray(Vector(p.images.map(JsString(_)): _*)),
      "_source" -> JsString(p._source),
      "run_date" -> JsString(p.run_date),
      "_ingest_ts" -> JsNumber(p._ingest_ts)
    )

    def read(value: JsValue): Product = value match {
      case JsObject(fields) =>
        Product(
          id = fields("id").convertTo[Int],
          title = fields("title").convertTo[String],
          description = fields.get("description").flatMap(_.convertTo[Option[String]]),
          price = fields("price").convertTo[Double],
          discountPercentage = fields.get("discountPercentage").flatMap(_.convertTo[Option[Double]]),
          rating = fields("rating").convertTo[Double],
          stock = fields("stock").convertTo[Int],
          brand = fields.get("brand").flatMap(_.convertTo[Option[String]]),
          category = fields("category").convertTo[String],
          thumbnail = fields.get("thumbnail").flatMap(_.convertTo[Option[String]]),
          images = fields("images").convertTo[List[String]],
          _source = fields("_source").convertTo[String],
          run_date = fields("run_date").convertTo[String],
          _ingest_ts = fields("_ingest_ts").convertTo[Long]
        )
      case _ => deserializationError("Product expected")
    }
  }
}

object OrderJsonProtocol extends DefaultJsonProtocol {
  implicit val orderFormat: RootJsonFormat[Order] = jsonFormat6(Order)
}

object Schemas {
  val productSchema: StructType = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("title", StringType, nullable = false),
    StructField("description", StringType, nullable = true),
    StructField("price", DoubleType, nullable = false),
    StructField("discountPercentage", DoubleType, nullable = false),
    StructField("rating", DoubleType, nullable = false),
    StructField("stock", IntegerType, nullable = false),
    StructField("brand", StringType, nullable = true),
    StructField("category", StringType, nullable = false),
    StructField("thumbnail", StringType, nullable = true),
    StructField("images", ArrayType(StringType), nullable = true),
    StructField("_source", StringType, nullable = false),
    StructField("run_date", StringType, nullable = false),
    StructField("_ingest_ts", LongType, nullable = false)
  ))

  val orderSchema: StructType = StructType(Seq(
    StructField("order_id", StringType, nullable = false),
    StructField("user_id", StringType, nullable = false),
    StructField("product_id", IntegerType, nullable = false),
    StructField("quantity", IntegerType, nullable = false),
    StructField("price", DoubleType, nullable = false),
    StructField("event_time", LongType, nullable = false)
  ))

  // Helper to parse raw JSON string to Product case class
  def parseProductJson(jsonStr: String, source: String, runDate: String): Option[Product] = {
    try {
      val rawProduct = jsonStr.parseJson.convertTo[Map[String, JsValue]]
      val product = Product(
        id = rawProduct("id").convertTo[Int],
        title = rawProduct("title").convertTo[String],
        description = rawProduct.get("description").flatMap(_.convertTo[Option[String]]),
        price = rawProduct("price").convertTo[Double],
        discountPercentage = rawProduct.get("discountPercentage").flatMap(_.convertTo[Option[Double]]),
        rating = rawProduct("rating").convertTo[Double],
        stock = rawProduct("stock").convertTo[Int],
        brand = rawProduct.get("brand").flatMap(_.convertTo[Option[String]]),
        category = rawProduct("category").convertTo[String],
        thumbnail = rawProduct.get("thumbnail").flatMap(_.convertTo[Option[String]]),
        images = rawProduct.get("images").fold(List.empty[String])(_.convertTo[List[String]]),
        _source = source,
        run_date = runDate,
        _ingest_ts = Instant.now().toEpochMilli
      )
      Some(product)
    } catch {
      case ex: Exception =>
        println(s"Failed to parse product JSON: ${ex.getMessage}")
        None
    }
  }

  // Helper to parse raw JSON string to Order case class
  def parseOrderJson(jsonStr: String): Option[Order] = {
    try {
      import OrderJsonProtocol._
      Some(jsonStr.parseJson.convertTo[Order])
    } catch {
      case ex: Exception =>
        println(s"Failed to parse order JSON: ${ex.getMessage}")
        None
    }
  }
}