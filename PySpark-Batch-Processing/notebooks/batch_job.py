from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BatchProcessingExample").getOrCreate()

# Read CSV from mounted volume
df = spark.read.csv("/data/large_dataset.csv", header=True, inferSchema=True)

# Transformations
df = df.withColumnRenamed("old_column", "new_column").dropna()

# Write to Parquet
df.write.mode("overwrite").parquet("/output/parquet_data")

print("Processing done!")
