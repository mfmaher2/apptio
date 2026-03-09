from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tiny-iceberg-write").getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS iceberg_data_test.dev LOCATION 's3a://mike-apptio-spark-test-2026/iceberg/'")

df = spark.createDataFrame([(1, "ok")], ["id", "status"])
df.writeTo("iceberg_data_test.dev.tiny_probe").createOrReplace()

print("TINY_ICEBERG_WRITE_OK")
spark.stop()