package com.example.ecommerce.model

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._
import spray.json.DefaultJsonProtocol._

class SchemasSpec extends AnyFlatSpec with Matchers {

  import ProductJsonProtocol._

  "parseProductJson" should "parse valid product JSON correctly" in {
    val json = """{
      "id": 1,
      "title": "Test Product",
      "description": "A test product",
      "price": 29.99,
      "discountPercentage": 10.0,
      "rating": 4.5,
      "stock": 100,
      "brand": "TestBrand",
      "category": "electronics",
      "thumbnail": "http://example.com/thumb.jpg",
      "images": ["http://example.com/img1.jpg", "http://example.com/img2.jpg"]
    }"""

    val result = Schemas.parseProductJson(json, "dummyjson", "2024-01-01")

    result shouldBe defined
    val product = result.get
    product.id shouldBe 1
    product.title shouldBe "Test Product"
    product.price shouldBe 29.99
    product.discountPercentage shouldBe 10.0
    product.rating shouldBe 4.5
    product.stock shouldBe 100
    product.brand shouldBe "TestBrand"
    product.category shouldBe "electronics"
    product._source shouldBe "dummyjson"
    product.run_date shouldBe "2024-01-01"
    product._ingest_ts should be > 0L
  }

  it should "handle missing optional fields gracefully" in {
    val json = """{
      "id": 2,
      "title": "Minimal Product",
      "price": 19.99,
      "rating": 3.0,
      "stock": 50,
      "category": "books"
    }"""

    val result = Schemas.parseProductJson(json, "fakestore", "2024-01-02")

    result shouldBe defined
    val product = result.get
    product.id shouldBe 2
    product.title shouldBe "Minimal Product"
    product.description shouldBe ""
    product.price shouldBe 19.99
    product.discountPercentage shouldBe 0.0
    product.rating shouldBe 3.0
    product.stock shouldBe 50
    product.brand shouldBe ""
    product.category shouldBe "books"
    product.thumbnail shouldBe ""
    product.images shouldBe empty
  }

  it should "return None for invalid JSON" in {
    val invalidJson = """{"invalid": "json"}"""

    val result = Schemas.parseProductJson(invalidJson, "dummyjson", "2024-01-01")

    result shouldBe None
  }

  it should "return None for malformed JSON" in {
    val malformedJson = """{"id": "not_a_number", "title": "Test"}"""

    val result = Schemas.parseProductJson(malformedJson, "dummyjson", "2024-01-01")

    result shouldBe None
  }

  "parseOrderJson" should "parse valid order JSON correctly" in {
    val json = """{
      "order_id": "order-123",
      "user_id": "user-456",
      "product_id": 789,
      "quantity": 2,
      "price": 49.99,
      "event_time": 1640995200000
    }"""

    val result = Schemas.parseOrderJson(json)

    result shouldBe defined
    val order = result.get
    order.order_id shouldBe "order-123"
    order.user_id shouldBe "user-456"
    order.product_id shouldBe 789
    order.quantity shouldBe 2
    order.price shouldBe 49.99
    order.event_time shouldBe 1640995200000L
  }

  it should "return None for invalid order JSON" in {
    val invalidJson = """{"invalid": "order"}"""

    val result = Schemas.parseOrderJson(invalidJson)

    result shouldBe None
  }
}