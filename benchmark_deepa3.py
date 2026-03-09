#!/usr/bin/env python3
"""
benchmark_deepa3.py

Spark SQL–style benchmark pipeline aligned with benchmark_deepa2_marker.py.

Purpose:
- benchmark the Spark SQL refactor on the larger synthetic parquet input
- keep the benchmark apples-to-apples with the DataFrame benchmark
- skip DDL-based casting for the synthetic benchmark data
- measure read / lowercase / flatten / Iceberg write timings
- write a benchmark summary JSON to S3
"""

import json
import time
import logging
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("benchmark_deepa3")

# -----------------------------------------------------------------------------
# USER CONFIG
# -----------------------------------------------------------------------------
S3_BUCKET = "mike-apptio-spark-test-2026"
S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"
DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"

WXD_CATALOG = "iceberg_data_test"
WXD_SCHEMA = "dev"
# TARGET_TABLE_NAME = "deepa3_benchmark_v2"
TARGET_TABLE_NAME = "deepa3_benchmark_v3"

PARTITION_ON_PARTITIONDATE_IF_PRESENT = True

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def build_lowercase_select(schema):
    return [f"`{f.name}` AS `{f.name.lower()}`" for f in schema.fields]


def build_flatten_select(df):
    exprs = []
    for f in df.schema.fields:
        name = f.name
        if isinstance(f.dataType, StructType) and name in ("costcurrencyvendorusd", "costcurrencyvendorraw"):
            for sub in f.dataType.fields:
                exprs.append(f"`{name}`.`{sub.name}` AS `{name}_{sub.name.lower()}`")
        else:
            exprs.append(f"`{name}`")
    return exprs


def init_spark():
    spark = (
        SparkSession.builder
        .appName("benchmark-deepa3")
        .enableHiveSupport()
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_benchmark_summary(spark, summary: dict):
    path = "s3a://mike-apptio-spark-test-2026/debug/benchmarks/benchmark_deepa3/"
    text = json.dumps(summary, indent=2)
    spark.sparkContext.parallelize([text], 1).saveAsTextFile(path)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    timings = {}
    t0 = time.time()

    spark = init_spark()
    table = f"{WXD_CATALOG}.{WXD_SCHEMA}.{TARGET_TABLE_NAME}"

    print("STEP A: create database")
    t = time.time()
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {WXD_CATALOG}.{WXD_SCHEMA} LOCATION '{DB_LOCATION}'")
    timings["create_database_sec"] = round(time.time() - t, 2)

    print("STEP B: read parquet")
    t = time.time()
    df_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(S3_PATH)
    )
    raw_count = df_raw.count()
    df_raw.createOrReplaceTempView("raw_input")
    timings["read_parquet_sec"] = round(time.time() - t, 2)

    print("STEP C: lowercase via SQL")
    t = time.time()
    lowercase_sql = ",\n        ".join(build_lowercase_select(df_raw.schema))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_lower AS
        SELECT
        {lowercase_sql}
        FROM raw_input
    """)
    df_lower = spark.table("v_lower")
    timings["lowercase_sec"] = round(time.time() - t, 2)

    print("STEP D: flatten via SQL")
    t = time.time()
    flatten_sql = ",\n        ".join(build_flatten_select(df_lower))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_flat AS
        SELECT
        {flatten_sql}
        FROM v_lower
    """)
    df_flat = spark.table("v_flat")
    timings["flatten_sec"] = round(time.time() - t, 2)

    print("STEP E: prepare output")
    t = time.time()
    # Keep benchmark aligned with benchmark_deepa2.py:
    # skip DDL-based cast logic for the synthetic benchmark data.
    df_out = df_flat
    df_out.createOrReplaceTempView("v_out")
    timings["prepare_output_sec"] = round(time.time() - t, 2)

    print("STEP F: write iceberg via SQL")
    t = time.time()

    if PARTITION_ON_PARTITIONDATE_IF_PRESENT and "partitiondate" in df_out.columns:
        non_partition_cols = [c for c in df_out.columns if c != "partitiondate"]
        select_list = ",\n                ".join([f"`{c}`" for c in non_partition_cols])

        spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW v_final AS
            SELECT
                {select_list},
                CAST(partitiondate AS DATE) AS partitiondate
            FROM v_out
        """)

        spark.sql(f"""
            CREATE OR REPLACE TABLE {table}
            USING iceberg
            PARTITIONED BY (partitiondate)
            TBLPROPERTIES (
              'format-version'='2',
              'write.format.default'='parquet',
              'write.parquet.compression-codec'='zstd',
              'write.metadata.compression-codec'='none',
              'write.metadata.delete-after-commit.enabled'='true',
              'write.metadata.previous-versions-max'='1',
              'commit.manifest-merge.enabled'='true',
              'write.spark.fanout.enabled'='true'
            )
            AS
            SELECT * FROM v_final
        """)
    else:
        spark.sql(f"""
            CREATE OR REPLACE TABLE {table}
            USING iceberg
            TBLPROPERTIES (
              'format-version'='2',
              'write.format.default'='parquet',
              'write.parquet.compression-codec'='zstd',
              'write.metadata.compression-codec'='none',
              'write.metadata.delete-after-commit.enabled'='true',
              'write.metadata.previous-versions-max'='1',
              'commit.manifest-merge.enabled'='true',
              'write.spark.fanout.enabled'='true'
            )
            AS
            SELECT * FROM v_out
        """)

    timings["write_iceberg_sec"] = round(time.time() - t, 2)
    timings["total_sec"] = round(time.time() - t0, 2)

    summary = {
        "script": "benchmark_deepa3.py",
        "table": table,
        "input_path": S3_PATH,
        "raw_count": raw_count,
        "output_count": -1,
        "columns_out": len(df_out.columns),
        "timings": timings,
    }

    print(json.dumps(summary, indent=2))
    write_benchmark_summary(spark, summary)
    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        raise