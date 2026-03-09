from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("probe-big-parquet").getOrCreate()

path = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"
df = spark.read.parquet(path)

print("COUNT:", df.count())
print("COLUMNS:", df.columns)
df.printSchema()
df.show(5, truncate=False)

spark.stop()
