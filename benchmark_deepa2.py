#!/usr/bin/env python3
"""
benchmark_deepa2.py

DataFrame-style benchmark pipeline for larger parquet input.

Purpose:
- benchmark the DataFrame-based refactor path
- avoid DDL/cast mismatches for synthetic benchmark data
- measure read / lowercase / flatten / Iceberg write timings
- write a benchmark summary JSON to S3

Notes:
- This version intentionally skips the DDL-based cast step for benchmarking
  because the synthetic big_test_input schema does not match the Snowflake DDL.
- It still exercises the important parts of the DataFrame pipeline:
    parquet read -> lowercase -> flatten -> Iceberg write
"""

import json
import time
import logging
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("benchmark_deepa2")

# -----------------------------------------------------------------------------
# USER CONFIG
# -----------------------------------------------------------------------------
S3_BUCKET = "mike-apptio-spark-test-2026"
S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"
DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"

WXD_CATALOG = "iceberg_data_test"
WXD_SCHEMA = "dev"
TARGET_TABLE_NAME = "deepa2_benchmark_v2"

PARTITION_ON_PARTITIONDATE_IF_PRESENT = True

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def lowercase_all(df):
    def recurse(schema, parent=None):
        out = []
        for f in schema.fields:
            c = col(f.name) if parent is None else parent.getField(f.name)
            if isinstance(f.dataType, StructType):
                out.append(struct(*recurse(f.dataType, c)).alias(f.name.lower()))
            else:
                out.append(c.alias(f.name.lower()))
        return out

    return df.select(*recurse(df.schema))


def flatten_cost_structs(df):
    schema = df.schema

    def flatten_struct(df_in, col_name):
        if col_name not in df_in.columns:
            return df_in

        field = schema[col_name]
        if not isinstance(field.dataType, StructType):
            return df_in

        for f in field.dataType.fields:
            flat_col_name = f"{col_name}_{f.name.lower()}"
            df_in = df_in.withColumn(flat_col_name, col(f"{col_name}.{f.name}"))

        return df_in.drop(col_name)

    df = flatten_struct(df, "costcurrencyvendorusd")
    df = flatten_struct(df, "costcurrencyvendorraw")
    return df


def init_spark():
    spark = (
        SparkSession.builder
        .appName("benchmark-deepa2")
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
    path = "s3a://mike-apptio-spark-test-2026/debug/benchmarks/benchmark_deepa2/"
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
    print("STEP A DONE")

    print("STEP B: read parquet")
    t = time.time()
    df_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(S3_PATH)
    )
    print("STEP B RAW SCHEMA")
    df_raw.printSchema()
    raw_count = df_raw.count()
    timings["read_parquet_sec"] = round(time.time() - t, 2)
    print(f"STEP B DONE raw_count={raw_count}")

    print("STEP C: lowercase")
    t = time.time()
    df_lower = lowercase_all(df_raw)
    print("STEP C LOWER SCHEMA")
    df_lower.printSchema()
    timings["lowercase_sec"] = round(time.time() - t, 2)
    print("STEP C DONE")

    print("STEP D: flatten")
    t = time.time()
    df_flat = flatten_cost_structs(df_lower)
    print("STEP D FLAT SCHEMA")
    df_flat.printSchema()
    timings["flatten_sec"] = round(time.time() - t, 2)
    print("STEP D DONE")

    print("STEP E: prepare output")
    t = time.time()
    df_out = df_flat
    print(f"STEP E columns_out={len(df_out.columns)}")
    timings["prepare_output_sec"] = round(time.time() - t, 2)
    print("STEP E DONE")

    print("STEP F: show sample")
    df_out.show(5, truncate=False)
    print("STEP F DONE")

    print("STEP G: write iceberg")
    t = time.time()
    if PARTITION_ON_PARTITIONDATE_IF_PRESENT and "partitiondate" in df_out.columns:
        df_out = df_out.withColumn("partitiondate", col("partitiondate").cast("date"))
        writer = df_out.writeTo(table).partitionedBy("partitiondate")
    else:
        writer = df_out.writeTo(table)

    (
        writer
        .tableProperty("format-version", "2")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("write.parquet.compression-codec", "zstd")
        .tableProperty("write.metadata.compression-codec", "none")
        .tableProperty("write.metadata.delete-after-commit.enabled", "true")
        .tableProperty("write.metadata.previous-versions-max", "1")
        .tableProperty("commit.manifest-merge.enabled", "true")
        .tableProperty("write.spark.fanout.enabled", "true")
        .createOrReplace()
    )
    timings["write_iceberg_sec"] = round(time.time() - t, 2)
    print("STEP G DONE")

    timings["total_sec"] = round(time.time() - t0, 2)

    summary = {
        "script": "benchmark_deepa2.py",
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