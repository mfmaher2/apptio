from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tiny-end-to-end").getOrCreate()

S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/tiny_s3_write/"
DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"
CATALOG = "iceberg_data_test"
SCHEMA = "dev"
TABLE = "tiny_end_to_end"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{SCHEMA} LOCATION '{DB_LOCATION}'")

df = spark.read.parquet(S3_PATH)
df.writeTo(f"{CATALOG}.{SCHEMA}.{TABLE}").createOrReplace()

print("TINY_END_TO_END_OK")
spark.stop()