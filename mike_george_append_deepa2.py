#!/usr/bin/env python3
"""
S3 Parquet -> Iceberg (Spark) checkpoint script

This version:
- reads DDL via Spark
- reads parquet from a known-good tiny parquet input path
- lowercases / flattens / casts
- writes to a scratch Iceberg table
- writes marker files after each successful step
"""

import os
import re
import logging
import traceback
from typing import List, Tuple
from datetime import datetime
import uuid

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, from_unixtime, when, length
from pyspark.sql.types import (
    StructType, LongType, IntegerType, StringType, DoubleType,
    BooleanType, DateType, DecimalType, TimestampNTZType
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("s3_to_iceberg")

# -----------------------------------------------------------------------------
# USER CONFIG
# -----------------------------------------------------------------------------
S3_BUCKET = "mike-apptio-spark-test-2026"
# S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/tiny_s3_write_input/"
S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"
OUTPUT_PATH = "s3a://mike-apptio-spark-test-2026/debug/deepa2_output/"
DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"

DDL_OBJECT_KEY = "ddl/cost_and_usage_all.sql"
DDL_FILE_PATH = "/home/spark/shared/cost_and_usage_all.sql"

WXD_CATALOG = "iceberg_data_test"
WXD_SCHEMA = "dev"
TARGET_TABLE_NAME = "deepa_checkpoint_test"

PARTITION_ON_PARTITIONDATE_IF_PRESENT = True

# -----------------------------------------------------------------------------
# AWS CREDENTIALS
# -----------------------------------------------------------------------------
AWS_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
def write_debug(spark, msg, bucket="mike-apptio-spark-test-2026", prefix="debug"):
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "-" + str(uuid.uuid4())[:8]
    out = f"s3a://{bucket}/{prefix}/{run_id}.txt"
    spark.sparkContext.parallelize([msg], 1).saveAsTextFile(out)
    print("WROTE DEBUG TO:", out)
    return out


def format_bytes(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def count_and_size_parquet_files(s3_path: str) -> Tuple[int, int]:
    s3_path = s3_path.replace("s3a://", "").replace("s3://", "")
    bucket, prefix = (s3_path.split("/", 1) + [""])[:2]
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="us-east-1",
    )

    file_count = 0
    total_size = 0

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].lower().endswith(".parquet"):
                    file_count += 1
                    total_size += obj["Size"]
        return file_count, total_size
    except ClientError as e:
        logger.error(f"S3 list error: {e}")
        return 0, 0


def download_ddl_from_s3(bucket: str, key: str, local_path: str) -> str:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name="us-east-1",
    )
    try:
        s3.download_file(bucket, key, local_path)
        with open(local_path, "r") as f:
            return f.read()
    except (NoCredentialsError, ClientError) as e:
        raise RuntimeError(f"Failed to download DDL s3://{bucket}/{key}: {e}") from e


def parse_snowflake_ddl(ddl_content: str) -> List[Tuple[str, str]]:
    m = re.search(r"TABLE[^(]*\((.*?)\)\s*;?\s*$", ddl_content, re.IGNORECASE | re.DOTALL)
    if not m:
        m = re.search(r"CLUSTER\s+BY\s*\([^)]+\)\s*\((.*?)\)\s*;?\s*$", ddl_content, re.IGNORECASE | re.DOTALL)
    if not m:
        raise RuntimeError("Could not parse column list from DDL.")

    cols = []
    for raw in m.group(1).splitlines():
        line = raw.strip().rstrip(",")
        if not line:
            continue

        qm = re.match(r'"([^"]+)"\s+([A-Za-z0-9_(),\s]+)', line)
        if qm:
            cols.append((qm.group(1), qm.group(2).strip()))
            continue

        um = re.match(r"([A-Za-z0-9_]+)\s+([A-Za-z0-9_(),\s]+)", line)
        if um:
            name = um.group(1)
            if name.upper() not in {"CONSTRAINT", "PRIMARY", "FOREIGN", "KEY", "INDEX"}:
                cols.append((name, um.group(2).strip()))
    return cols


def snowflake_to_spark_type(sf_type: str):
    sf_type = sf_type.upper().strip()

    if sf_type.startswith("FLOAT") or sf_type.startswith("DOUBLE") or sf_type == "REAL":
        return DoubleType()

    if sf_type in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
        return LongType()

    if (
        sf_type.startswith("VARCHAR")
        or sf_type.startswith("STRING")
        or sf_type == "TEXT"
        or sf_type.startswith("CHAR")
    ):
        return StringType()

    if sf_type == "BOOLEAN":
        return BooleanType()

    if sf_type == "DATE":
        return DateType()

    if sf_type.startswith("TIMESTAMP"):
        return TimestampNTZType()

    nm = re.match(r"NUMBER\((\d+)(?:,\s*(\d+))?\)", sf_type)
    if nm:
        p = int(nm.group(1))
        s = int(nm.group(2)) if nm.group(2) else 0
        if s == 0:
            if p <= 9:
                return IntegerType()
            if p <= 18:
                return LongType()
            return DecimalType(min(p, 38), 0)
        return DecimalType(min(p, 38), min(s, 38))

    dm = re.match(r"(?:DECIMAL|NUMERIC)\((\d+)(?:,\s*(\d+))?\)", sf_type)
    if dm:
        p = int(dm.group(1))
        s = int(dm.group(2)) if dm.group(2) else 0
        return DecimalType(min(p, 38), min(s, 38))

    if sf_type == "NUMBER":
        return LongType()

    logger.warning(f"Unknown Snowflake type '{sf_type}', defaulting to StringType")
    return StringType()


def epoch_int64_to_timestamp_ntz(cname: str):
    v = col(cname).cast("long")
    s = v.cast("string")

    seconds = (
        when(length(s) == 19, v / 1_000_000_000)
        .when(length(s) == 16, v / 1_000_000)
        .when(length(s) == 13, v / 1_000)
        .when(length(s) == 10, v)
        .otherwise(v / 1_000)
    ).cast("double")

    seconds = when(seconds.between(946684800, 4102444800), seconds)

    return from_unixtime(seconds).cast("timestamp_ntz")


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
            flat_col_name = f"{col_name}_{f.name}"
            df_in = df_in.withColumn(flat_col_name, col(f"{col_name}.{f.name}"))
        df_in = df_in.drop(col_name)
        return df_in

    df = flatten_struct(df, "costcurrencyvendorusd")
    df = flatten_struct(df, "costcurrencyvendorraw")
    return df


def cast_and_fix(df, ddl_cols):
    type_map = {n.lower(): snowflake_to_spark_type(t) for n, t in ddl_cols}
    exprs = []

    for f in df.schema.fields:
        name = f.name
        cur = f.dataType
        exp = type_map.get(name)

        if exp is None:
            logger.warning(f"Column '{name}' not found in DDL, keeping original type: {cur}")
            exprs.append(col(name))
            continue

        if isinstance(exp, TimestampNTZType):
            if isinstance(cur, (LongType, IntegerType)):
                logger.info(f"Converting epoch column '{name}' from {cur} to TimestampNTZ")
                exprs.append(epoch_int64_to_timestamp_ntz(name).alias(name))
            else:
                exprs.append(col(name).cast("timestamp_ntz").alias(name))
        elif cur != exp:
            logger.info(f"Casting column '{name}' from {cur} to {exp}")
            exprs.append(col(name).cast(exp).alias(name))
        else:
            exprs.append(col(name))

    df_cols = {f.name for f in df.schema.fields}
    missing_cols = set(type_map.keys()) - df_cols
    if missing_cols:
        logger.warning(f"DDL columns not found in DataFrame: {missing_cols}")

    return df.select(*exprs)


def init_spark():
    spark = (
        SparkSession.builder
        .appName("S3->Iceberg Presto Safe")
        .enableHiveSupport()
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.threads.max", "64")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def write_marker(spark, step_name, message):
    marker_path = f"s3a://{S3_BUCKET}/debug/markers/{step_name}/"
    marker_df = spark.createDataFrame([(step_name, message)], ["step", "message"])
    marker_df.write.mode("overwrite").parquet(marker_path)


def main():
    print("MAIN: entered main()")
    spark = init_spark()

    write_marker(spark, "step0_start", "script started")

    print("STEP_1 create database")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {WXD_CATALOG}.{WXD_SCHEMA} LOCATION '{DB_LOCATION}'")
    write_marker(spark, "step1_db_created", "database created")

    print("STEP_2 read ddl via spark")
    ddl_path = f"s3a://{S3_BUCKET}/{DDL_OBJECT_KEY}"
    ddl_text = "\n".join([r.value for r in spark.read.text(ddl_path).collect()])
    ddl = parse_snowflake_ddl(ddl_text)
    write_marker(spark, "step2_ddl_read", f"ddl columns parsed: {len(ddl)}")

    print("STEP_3 read parquet")
    df_raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.parquet")
        .parquet(S3_PATH)
    )
    write_marker(spark, "step3_parquet_read", f"raw columns: {len(df_raw.columns)}")

    print("STEP_4 lowercase")
    df_lower = lowercase_all(df_raw)
    write_marker(spark, "step4_lowercase", f"lowercase columns: {len(df_lower.columns)}")

    print("STEP_5 flatten")
    df_flat = flatten_cost_structs(df_lower)
    write_marker(spark, "step5_flatten", f"flatten columns: {len(df_flat.columns)}")

    print("STEP_6 cast starting")
    df_out = cast_and_fix(df_flat, ddl)
    print("STEP_6 cast finished")
    write_marker(spark, "step6_cast", f"cast columns: {len(df_out.columns)}")

    print("STEP_7 reached")

    print("STEP_7 basic info")
    print(f"S3_PATH={S3_PATH}")
    print(f"OUTPUT_PATH={OUTPUT_PATH}")
    print(f"df_out columns={len(df_out.columns)}")
    print(f"df_out partitions={df_out.rdd.getNumPartitions()}")

    print("STEP_7 printing schema")
    df_out.printSchema()
    print("STEP_7 schema printed successfully")

    print("STEP_7 showing rows")
    df_out.show(5, truncate=False)
    print("STEP_7 show completed")

    table = f"{WXD_CATALOG}.{WXD_SCHEMA}.{TARGET_TABLE_NAME}"

    print("STEP_7 writing iceberg")
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

    print("STEP_7 iceberg write completed")
    write_marker(spark, "step7_write_ok", f"iceberg write succeeded to {table}")

    spark.stop()


if __name__ == "__main__":
    try:
        print("TOP: starting main()")
        main()
        print("TOP: main() completed successfully")
    except Exception as e:
        print(f"TOP LEVEL EXCEPTION: {e}")
        traceback.print_exc()

        try:
            with open("deepa2_top_level_error.log", "w") as f:
                f.write(f"TOP LEVEL EXCEPTION: {e}\n")
                f.write(traceback.format_exc())
        except Exception as file_err:
            print(f"FAILED TO WRITE LOCAL ERROR LOG: {file_err}")

        raise