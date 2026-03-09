from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    expr,
    concat,
    to_date,
    date_add,
    current_date,
    struct,
)

spark = (
    SparkSession.builder
    .appName("generate-big-parquet")
    .getOrCreate()
)

ROW_COUNT = 1_000_000
OUTPUT_PATH = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"

base = (
    spark.range(0, ROW_COUNT)
    .withColumnRenamed("id", "record_id")
    .withColumn("status", lit("ok"))
    .withColumn("accountid", concat(lit("acct-"), col("record_id").cast("string")))
    .withColumn("partitiondate", to_date(date_add(current_date(), expr("CAST(record_id % 30 AS INT)"))))
)

df = (
    base
    .withColumn(
        "costcurrencyvendorusd",
        struct(
            (col("record_id") * lit(0.01)).cast("double").alias("adjustedamortizedcost"),
            (col("record_id") * lit(0.02)).cast("double").alias("cost"),
            lit("USD").alias("currencycode"),
        ),
    )
    .withColumn(
        "costcurrencyvendorraw",
        struct(
            (col("record_id") * lit(0.03)).cast("double").alias("adjustedamortizedcost"),
            (col("record_id") * lit(0.04)).cast("double").alias("cost"),
            lit("USD").alias("currencycode"),
        ),
    )
)

df = df.repartition(16)
df.write.mode("overwrite").parquet(OUTPUT_PATH)

print(f"WROTE {ROW_COUNT} ROWS TO {OUTPUT_PATH}")
spark.stop()