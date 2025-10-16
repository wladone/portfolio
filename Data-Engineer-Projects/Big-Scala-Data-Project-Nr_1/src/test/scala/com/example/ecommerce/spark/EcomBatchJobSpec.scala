package com.example.ecommerce.spark

import com.example.ecommerce.model.Product
import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

class EcomBatchJobSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("EcomBatchJobTest")
      .master("local[1]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  override def beforeEach(): Unit = {
    // Clean up any test data
  }

  "deduplicate function" should "keep the latest record by ingest timestamp" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Create test data with duplicates
    val testData = Seq(
      Product(1, "Product A", Some("Desc A"), 10.0, Some(0.0), 4.0, 100, Some("Brand A"), "cat1", Some("thumb1"), List("img1"), "source1", "2024-01-01", 1000L),
      Product(1, "Product A", Some("Desc A Updated"), 10.0, Some(0.0), 4.0, 100, Some("Brand A"), "cat1", Some("thumb1"), List("img1"), "source1", "2024-01-01", 2000L), // Latest
      Product(2, "Product B", Some("Desc B"), 20.0, Some(0.0), 3.5, 50, Some("Brand B"), "cat2", Some("thumb2"), List("img2"), "source1", "2024-01-01", 1500L),
      Product(2, "Product B", Some("Desc B"), 20.0, Some(0.0), 3.5, 50, Some("Brand B"), "cat2", Some("thumb2"), List("img2"), "source1", "2024-01-01", 1200L) // Older
    )

    val df = testData.toDF()

    // Apply deduplication
    val dedupedDf = EcomBatchJob.deduplicate(df)

    // Collect results
    val results = dedupedDf.orderBy("_ingest_ts").collect()

    // Should have 2 records (one for each unique id)
    results.length shouldBe 2

    // Check that we kept the latest for id=1
    val product1 = results.find(_.getAs[Int]("id") == 1).get
    product1.getAs[Option[String]]("description") shouldBe Some("Desc A Updated")
    product1.getAs[Long]("_ingest_ts") shouldBe 2000L

    // Check that we kept the latest for id=2
    val product2 = results.find(_.getAs[Int]("id") == 2).get
    product2.getAs[Long]("_ingest_ts") shouldBe 1500L
  }

  it should "handle single records without duplicates" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val testData = Seq(
      Product(1, "Product A", Some("Desc A"), 10.0, Some(0.0), 4.0, 100, Some("Brand A"), "cat1", Some("thumb1"), List("img1"), "source1", "2024-01-01", 1000L),
      Product(2, "Product B", Some("Desc B"), 20.0, Some(0.0), 3.5, 50, Some("Brand B"), "cat2", Some("thumb2"), List("img2"), "source1", "2024-01-01", 1500L)
    )

    val df = testData.toDF()
    val dedupedDf = EcomBatchJob.deduplicate(df)
    val results = dedupedDf.collect()

    results.length shouldBe 2
  }

  it should "handle empty DataFrame" in {
    val sparkSession = spark
    import sparkSession.implicits._

    val emptyDf: Seq[Product] = Seq.empty
    val df = emptyDf.toDF()
    val dedupedDf = EcomBatchJob.deduplicate(emptyDf)
    val results = dedupedDf.collect()

    results.length shouldBe 0
  }

  it should "deduplicate across different sources correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._

    // Same product ID from different sources should be kept separate
    val testData = Seq(
      Product(1, "Product A", Some("Desc A Source1"), 10.0, Some(0.0), 4.0, 100, Some("Brand A"), "cat1", Some("thumb1"), List("img1"), "source1", "2024-01-01", 1000L),
      Product(1, "Product A", Some("Desc A Source2"), 10.0, Some(0.0), 4.0, 100, Some("Brand A"), "cat1", Some("thumb1"), List("img1"), "source2", "2024-01-01", 1500L)
    )

    val df = testData.toDF()
    val dedupedDf = EcomBatchJob.deduplicate(df)
    val results = dedupedDf.collect()

    // Should keep both since they have different sources
    results.length shouldBe 2
    val sources = results.map(_.getAs[String]("_source")).toSet
    sources shouldBe Set("source1", "source2")
  }
}