from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tiny-s3-write").getOrCreate()

df = spark.createDataFrame([(1, "ok")], ["id", "status"])
df.write.mode("overwrite").parquet("s3a://mike-apptio-spark-test-2026/debug/tiny_s3_write_input/")

print("WROTE_TINY_S3_WRITE_OK")
spark.stop()