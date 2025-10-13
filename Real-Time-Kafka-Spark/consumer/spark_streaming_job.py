from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder.appName("KafkaSparkStreaming").getOrCreate()

schema = StructType().add("sensor_id", StringType()).add("value", StringType())

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor_data") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data"))
final_df = json_df.select("data.*")

query = final_df.writeStream.format("console").start()
query.awaitTermination()
