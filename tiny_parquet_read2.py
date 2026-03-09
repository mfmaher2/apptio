from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tiny-parquet-read2").getOrCreate()

path = "s3a://mike-apptio-spark-test-2026/debug/tiny_s3_write_input/"
print("READING:", path)

df = spark.read.parquet(path)

print("ROWCOUNT:", df.count())
print("COLCOUNT:", len(df.columns))
print("COLUMNS:", df.columns)

df.printSchema()
df.show(5, truncate=False)

spark.stop()