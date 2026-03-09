#!/usr/bin/env python3
"""
S3 Parquet -> Iceberg (Spark) for Presto (NO LTZ, correct epoch timestamps)

- Lowercases all columns (incl. nested structs)
- Reads Parquet from S3
- Downloads Snowflake DDL from S3
- Converts INT64 epoch timestamps (sec/ms/us/ns) -> TIMESTAMP_NTZ
- Writes Iceberg v2 parquet + zstd
"""

import os
import re
import time
import logging
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, from_unixtime, when, lit, length
from pyspark.sql.types import (
    StructType, LongType, IntegerType, StringType, DoubleType,
    BooleanType, DateType, DecimalType, TimestampNTZType
)
from pyspark.sql.utils import AnalysisException


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("s3_to_iceberg")

# -----------------------------------------------------------------------------
# USER CONFIG
# -----------------------------------------------------------------------------
S3_PATH = "s3a://apptio-watsonx-poc/archive/production/cost/113330/{2025-09,2025-10,2025-11,2025-12}"
S3_BUCKET = "apptio-watsonx-poc"

DDL_OBJECT_KEY = "cost_and_usage_all.sql"
DDL_FILE_PATH = "/home/spark/shared/cost_and_usage_all.sql"

WXD_CATALOG = "iceberg_data"
WXD_SCHEMA = "live"
#TARGET_TABLE_NAME = "test_mss_202511202512"
TARGET_TABLE_NAME = "apptio_test_table_4_append_test"

DB_LOCATION = f"s3a://{S3_BUCKET}/{WXD_SCHEMA}/{TARGET_TABLE_NAME}"
PARTITION_ON_PARTITIONDATE_IF_PRESENT = True

# -----------------------------------------------------------------------------
# AWS CREDENTIALS (YOUR REQUESTED VERSION)
# -----------------------------------------------------------------------------
AWS_ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID', 'xxx')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'xxx')

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
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
    """Parse Snowflake DDL with improved type parsing to handle commas in type definitions"""
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

        # Match quoted column names with type (including commas and spaces in type)
        qm = re.match(r'"([^"]+)"\s+([A-Za-z0-9_(),\s]+)', line)
        if qm:
            cols.append((qm.group(1), qm.group(2).strip()))
            continue

        # Match unquoted column names with type (including commas and spaces in type)
        um = re.match(r"([A-Za-z0-9_]+)\s+([A-Za-z0-9_(),\s]+)", line)
        if um:
            name = um.group(1)
            if name.upper() not in {"CONSTRAINT","PRIMARY","FOREIGN","KEY","INDEX"}:
                cols.append((name, um.group(2).strip()))
    return cols


def snowflake_to_spark_type(sf_type: str):
    """Convert Snowflake data type to Spark data type."""
    sf_type = sf_type.upper().strip()
    
    # Numeric types - floating point
    if sf_type.startswith("FLOAT") or sf_type.startswith("DOUBLE") or sf_type == "REAL":
        return DoubleType()
    
    # Integer types
    if sf_type in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
        return LongType()
    
    # String types
    if (sf_type.startswith("VARCHAR") or sf_type.startswith("STRING") or
        sf_type == "TEXT" or sf_type.startswith("CHAR")):
        return StringType()
    
    # Boolean
    if sf_type == "BOOLEAN":
        return BooleanType()
    
    # Date/Time types
    if sf_type == "DATE":
        return DateType()
    if sf_type.startswith("TIMESTAMP"):
        return TimestampNTZType()
    
    # NUMBER with optional scale - handles NUMBER(p) and NUMBER(p,s)
    nm = re.match(r"NUMBER\((\d+)(?:,\s*(\d+))?\)", sf_type)
    if nm:
        p = int(nm.group(1))
        s = int(nm.group(2)) if nm.group(2) else 0
        if s == 0:
            if p <= 9: return IntegerType()
            if p <= 18: return LongType()
            return DecimalType(min(p, 38), 0)
        return DecimalType(min(p, 38), min(s, 38))
    
    # DECIMAL/NUMERIC with optional scale
    dm = re.match(r"(?:DECIMAL|NUMERIC)\((\d+)(?:,\s*(\d+))?\)", sf_type)
    if dm:
        p = int(dm.group(1))
        s = int(dm.group(2)) if dm.group(2) else 0
        return DecimalType(min(p, 38), min(s, 38))
    
    # Plain NUMBER without precision
    if sf_type == "NUMBER":
        return LongType()
    
    # Default fallback for unknown types
    logger.warning(f"Unknown Snowflake type '{sf_type}', defaulting to StringType")
    return StringType()


def epoch_int64_to_timestamp_ntz(cname: str):
    v = col(cname).cast("long")
    s = v.cast("string")

    seconds = (
        when(length(s)==19, v/1_000_000_000)
        .when(length(s)==16, v/1_000_000)
        .when(length(s)==13, v/1_000)
        .when(length(s)==10, v)
        .otherwise(v/1_000)
    ).cast("double")

    seconds = when(seconds.between(946684800,4102444800), seconds)

    return from_unixtime(seconds).cast("timestamp_ntz")


def lowercase_all(df):
    def recurse(schema,parent=None):
        out=[]
        for f in schema.fields:
            c=col(f.name) if parent is None else parent.getField(f.name)
            if isinstance(f.dataType,StructType):
                out.append(struct(*recurse(f.dataType,c)).alias(f.name.lower()))
            else:
                out.append(c.alias(f.name.lower()))
        return out
    return df.select(*recurse(df.schema))

def flatten_cost_structs(df):
    """
    Flatten costcurrencyvendorusd and costcurrencyvendorraw structs into
    columns named like snowflake DDL:
      costcurrencyvendorusd_<fieldname>
      costcurrencyvendorraw_<fieldname>
    """
    schema = df.schema

    # Helper to flatten one struct column
    def flatten_struct(df, col_name):
        if col_name not in df.columns:
            return df
        field = schema[col_name]
        if not isinstance(field.dataType, StructType):
            return df

        for f in field.dataType.fields:
            flat_col_name = f"{col_name}_{f.name}"  # e.g. costcurrencyvendorusd_adjustedamortizedcost
            df = df.withColumn(flat_col_name, col(f"{col_name}.{f.name}"))
        # Optional: drop the original struct if you don't need it anymore
        df = df.drop(col_name)
        return df

    df = flatten_struct(df, "costcurrencyvendorusd")
    df = flatten_struct(df, "costcurrencyvendorraw")
    return df

def cast_and_fix(df, ddl_cols):
    """Cast DataFrame columns to match DDL types and fix epoch timestamps."""
    type_map = {n.lower(): snowflake_to_spark_type(t) for n, t in ddl_cols}
    exprs = []
    
    for f in df.schema.fields:
        name = f.name
        cur = f.dataType
        exp = type_map.get(name)
        
        # Warn if column not in DDL
        if exp is None:
            logger.warning(f"Column '{name}' not found in DDL, keeping original type: {cur}")
            exprs.append(col(name))
            continue
        
        # Handle timestamp conversions from integer types (epoch timestamps)
        if isinstance(exp, TimestampNTZType):
            if isinstance(cur, (LongType, IntegerType)):
                logger.info(f"Converting epoch column '{name}' from {cur} to TimestampNTZ")
                exprs.append(epoch_int64_to_timestamp_ntz(name).alias(name))
            else:
                exprs.append(col(name).cast("timestamp_ntz").alias(name))
        # Cast if types differ
        elif cur != exp:
            logger.info(f"Casting column '{name}' from {cur} to {exp}")
            exprs.append(col(name).cast(exp).alias(name))
        else:
            exprs.append(col(name))
    
    # Warn about DDL columns not found in DataFrame
    df_cols = {f.name for f in df.schema.fields}
    missing_cols = set(type_map.keys()) - df_cols
    if missing_cols:
        logger.warning(f"DDL columns not found in DataFrame: {missing_cols}")
    
    return df.select(*exprs)


def init_spark():
    spark=(
        SparkSession.builder
        .appName("S3->Iceberg Presto Safe")
        .enableHiveSupport()
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint","s3.us-east-1.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key",AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.hadoop.fs.s3a.threads.max", "64")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def table_exists(spark, full_table_name: str) -> bool:
    # works in most Spark deployments
    try:
        return spark.catalog.tableExists(full_table_name)
    except Exception:
        # fallback for catalogs where tableExists may be flaky
        cat, sch, tbl = full_table_name.split(".", 2)
        rows = spark.sql(f"SHOW TABLES IN {cat}.{sch} LIKE '{tbl}'").collect()
        return len(rows) > 0

def main():
    spark = init_spark()

    # Better DB location (schema-level, not table-level)
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {WXD_CATALOG}.{WXD_SCHEMA}
        LOCATION 's3a://{S3_BUCKET}/{WXD_SCHEMA}'
    """)

    ddl = parse_snowflake_ddl(
        download_ddl_from_s3(S3_BUCKET, DDL_OBJECT_KEY, DDL_FILE_PATH)
    )

    table = f"{WXD_CATALOG}.{WXD_SCHEMA}.{TARGET_TABLE_NAME}"
    exists = table_exists(spark, table)

    # ---- LIST YOUR MONTHS HERE ----
    months = ["2025-09","2025-10","2025-11","2025-12"]

    for month in months:
        print(f"\n========== Processing month: {month} ==========")

        month_path = f"s3a://apptio-watsonx-poc/archive/production/cost/113330/{month}"

        print("=== Reading parquet ===")
        df_raw = spark.read \
            .option("recursiveFileLookup", "true") \
            .option("pathGlobFilter", "*.parquet") \
            .parquet(month_path)

        print("Input partitions:", df_raw.rdd.getNumPartitions())

        print("=== Lowercase ===")
        df_lower = lowercase_all(df_raw)

        print("=== Flatten cost structs ===")
        df_flat = flatten_cost_structs(df_lower)

        print("=== Cast + timestamp fix ===")
        df_out = cast_and_fix(df_flat, ddl)

        print("Output partitions before write:", df_out.rdd.getNumPartitions())

        print("=== Write Iceberg ===")

        if PARTITION_ON_PARTITIONDATE_IF_PRESENT and "partitiondate" in df_out.columns:
            df_out = df_out.repartition(32, col("partitiondate")).sortWithinPartitions(col("partitiondate"))
            writer = (
                df_out
                .writeTo(table)
                .partitionedBy("partitiondate")
            )
        else:
            writer = df_out.writeTo(table)

        writer = (
            writer
            .tableProperty("format-version", "2")
            .tableProperty("write.format.default", "parquet")
            .tableProperty("write.parquet.compression-codec", "zstd")
            .tableProperty("write.distribution-mode", "hash")
            .tableProperty("write.target-file-size-bytes", "134217728")
            .tableProperty("commit.manifest-merge.enabled", "true")
        )

        if not exists:
            print(f"Table {table} does not exist — creating.")
            writer.create()
            exists = True
        else:
            print(f"Appending data for month {month}.")
            writer.append()

        print(f"Finished month: {month}")

    print("\nDONE")
    spark.stop()


if __name__=="__main__":
    main()
