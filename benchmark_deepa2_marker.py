#!/usr/bin/env python3

import json
import time
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct
from pyspark.sql.types import StructType

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

S3_BUCKET = "mike-apptio-spark-test-2026"

S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"

DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"

WXD_CATALOG = "iceberg_data_test"
WXD_SCHEMA = "dev"
TARGET_TABLE_NAME = "deepa2_benchmark_debug"

PARTITION_ON_PARTITIONDATE_IF_PRESENT = True

# -------------------------------------------------------------------
# SPARK INIT
# -------------------------------------------------------------------

def init_spark():

    spark = (
        SparkSession.builder
        .appName("benchmark-deepa2-debug")
        .enableHiveSupport()
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint","s3.us-east-1.amazonaws.com")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# -------------------------------------------------------------------
# MARKER WRITER
# -------------------------------------------------------------------

def write_marker(spark, step, message):

    path = f"s3a://{S3_BUCKET}/debug/benchmarks/deepa2_markers/{step}/"

    df = spark.createDataFrame([(step, message)], ["step","message"])

    df.write.mode("overwrite").parquet(path)


# -------------------------------------------------------------------
# LOWERCASE
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# FLATTEN
# -------------------------------------------------------------------

def flatten_cost_structs(df):

    schema = df.schema

    def flatten(df_in, col_name):

        if col_name not in df_in.columns:
            return df_in

        field = schema[col_name]

        if not isinstance(field.dataType, StructType):
            return df_in

        for f in field.dataType.fields:

            new_name = f"{col_name}_{f.name.lower()}"

            df_in = df_in.withColumn(new_name, col(f"{col_name}.{f.name}"))

        return df_in.drop(col_name)

    df = flatten(df,"costcurrencyvendorusd")

    df = flatten(df,"costcurrencyvendorraw")

    return df


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():

    spark = init_spark()

    table = f"{WXD_CATALOG}.{WXD_SCHEMA}.{TARGET_TABLE_NAME}"

    write_marker(spark,"step0_start","script started")

    # ---------------------------------------------------------------
    print("STEP 1 CREATE DB")

    spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS {WXD_CATALOG}.{WXD_SCHEMA}
    LOCATION '{DB_LOCATION}'
    """)

    write_marker(spark,"step1_db_created","db created")

    # ---------------------------------------------------------------
    print("STEP 2 READ PARQUET")

    df_raw = (
        spark.read
        .option("recursiveFileLookup","true")
        .option("pathGlobFilter","*.parquet")
        .parquet(S3_PATH)
    )

    write_marker(spark,"step2_parquet_read",f"cols={len(df_raw.columns)}")

    # ---------------------------------------------------------------
    print("STEP 3 LOWERCASE")

    df_lower = lowercase_all(df_raw)

    write_marker(spark,"step3_lowercase",f"cols={len(df_lower.columns)}")

    # ---------------------------------------------------------------
    print("STEP 4 FLATTEN")

    df_flat = flatten_cost_structs(df_lower)

    write_marker(spark,"step4_flatten",f"cols={len(df_flat.columns)}")

    # ---------------------------------------------------------------
    print("STEP 5 PREPARE")

    df_out = df_flat

    write_marker(spark,"step5_prepare",f"cols={len(df_out.columns)}")

    # ---------------------------------------------------------------
    print("STEP 6 WRITE ICEBERG")

    if PARTITION_ON_PARTITIONDATE_IF_PRESENT and "partitiondate" in df_out.columns:

        df_out = df_out.withColumn("partitiondate", col("partitiondate").cast("date"))

        writer = df_out.writeTo(table).partitionedBy("partitiondate")

    else:

        writer = df_out.writeTo(table)

    (
        writer
        .tableProperty("format-version","2")
        .tableProperty("write.format.default","parquet")
        .createOrReplace()
    )

    write_marker(spark,"step6_write_iceberg","success")

    spark.stop()


# -------------------------------------------------------------------
# ENTRY
# -------------------------------------------------------------------

if __name__ == "__main__":

    try:

        main()

    except Exception as e:

        print("TOP LEVEL ERROR")

        traceback.print_exc()

        raise