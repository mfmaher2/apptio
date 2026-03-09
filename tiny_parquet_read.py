from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tiny-parquet-read").getOrCreate()

path = "s3a://mike-apptio-spark-test-2026/parquet/"
print("READING:", path)

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .option("pathGlobFilter", "*.parquet")
    .parquet(path)
)

print("ROWCOUNT:", df.count())
print("COLCOUNT:", len(df.columns))
print(df.columns)

spark.stop()