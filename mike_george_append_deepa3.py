#!/usr/bin/env python3
"""
Spark SQL–driven checkpoint pipeline:
- creates Iceberg database
- reads Snowflake DDL from S3 via Spark
- reads parquet from S3
- lowercases columns
- flattens known cost structs
- casts columns based on parsed DDL
- writes to Iceberg using Spark SQL
- writes S3 marker files after each successful stage
"""

import os
import re
import logging
import traceback
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("s3_to_iceberg_sql")

# -----------------------------------------------------------------------------
# USER CONFIG
# -----------------------------------------------------------------------------
S3_BUCKET = "mike-apptio-spark-test-2026"
# S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/tiny_s3_write_input/"
S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/big_test_input/"
DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"

DDL_OBJECT_KEY = "ddl/cost_and_usage_all.sql"

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


def spark_type_sql(sf_type: str) -> str:
    sf_type = sf_type.upper().strip()

    if sf_type.startswith("FLOAT") or sf_type.startswith("DOUBLE") or sf_type == "REAL":
        return "DOUBLE"

    if sf_type in ("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT"):
        return "BIGINT"

    if (
        sf_type.startswith("VARCHAR")
        or sf_type.startswith("STRING")
        or sf_type == "TEXT"
        or sf_type.startswith("CHAR")
    ):
        return "STRING"

    if sf_type == "BOOLEAN":
        return "BOOLEAN"

    if sf_type == "DATE":
        return "DATE"

    if sf_type.startswith("TIMESTAMP"):
        return "TIMESTAMP_NTZ"

    nm = re.match(r"NUMBER\((\d+)(?:,\s*(\d+))?\)", sf_type)
    if nm:
        p = int(nm.group(1))
        s = int(nm.group(2)) if nm.group(2) else 0
        if s == 0:
            if p <= 9:
                return "INT"
            if p <= 18:
                return "BIGINT"
            return f"DECIMAL({min(p,38)},0)"
        return f"DECIMAL({min(p,38)},{min(s,38)})"

    dm = re.match(r"(?:DECIMAL|NUMERIC)\((\d+)(?:,\s*(\d+))?\)", sf_type)
    if dm:
        p = int(dm.group(1))
        s = int(dm.group(2)) if dm.group(2) else 0
        return f"DECIMAL({min(p,38)},{min(s,38)})"

    if sf_type == "NUMBER":
        return "BIGINT"

    return "STRING"


def build_lowercase_select(schema):
    exprs = []
    for f in schema.fields:
        exprs.append(f"`{f.name}` AS `{f.name.lower()}`")
    return exprs


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


def build_cast_select(df, ddl_cols):
    ddl_map = {name.lower(): spark_type_sql(dtype) for name, dtype in ddl_cols}
    exprs = []

    for f in df.schema.fields:
        name = f.name
        target = ddl_map.get(name)

        if target is None:
            logger.warning(f"Column '{name}' not found in DDL, keeping original type")
            exprs.append(f"`{name}`")
            continue

        if target == "TIMESTAMP_NTZ":
            exprs.append(f"""
CASE
  WHEN LENGTH(CAST(`{name}` AS STRING)) = 19 THEN CAST(from_unixtime(CAST(`{name}` AS DOUBLE) / 1000000000) AS TIMESTAMP_NTZ)
  WHEN LENGTH(CAST(`{name}` AS STRING)) = 16 THEN CAST(from_unixtime(CAST(`{name}` AS DOUBLE) / 1000000) AS TIMESTAMP_NTZ)
  WHEN LENGTH(CAST(`{name}` AS STRING)) = 13 THEN CAST(from_unixtime(CAST(`{name}` AS DOUBLE) / 1000) AS TIMESTAMP_NTZ)
  WHEN LENGTH(CAST(`{name}` AS STRING)) = 10 THEN CAST(from_unixtime(CAST(`{name}` AS DOUBLE)) AS TIMESTAMP_NTZ)
  ELSE CAST(`{name}` AS TIMESTAMP_NTZ)
END AS `{name}`
""".strip())
        else:
            exprs.append(f"CAST(`{name}` AS {target}) AS `{name}`")

    return exprs


def init_spark():
    spark = (
        SparkSession.builder
        .appName("S3->Iceberg SQL Safe")
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

    table = f"{WXD_CATALOG}.{WXD_SCHEMA}.{TARGET_TABLE_NAME}"

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
    df_raw.createOrReplaceTempView("raw_input")
    write_marker(spark, "step3_parquet_read", f"raw columns: {len(df_raw.columns)}")

    print("STEP_4 lowercase via SQL")
    lowercase_sql = ",\n".join(build_lowercase_select(df_raw.schema))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_lower AS
        SELECT
        {lowercase_sql}
        FROM raw_input
    """)
    df_lower = spark.table("v_lower")
    write_marker(spark, "step4_lowercase", f"lowercase columns: {len(df_lower.columns)}")

    print("STEP_5 flatten via SQL")
    flatten_sql = ",\n".join(build_flatten_select(df_lower))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_flat AS
        SELECT
        {flatten_sql}
        FROM v_lower
    """)
    df_flat = spark.table("v_flat")
    write_marker(spark, "step5_flatten", f"flatten columns: {len(df_flat.columns)}")

    print("STEP_6 cast via SQL")
    cast_sql = ",\n".join(build_cast_select(df_flat, ddl))
    spark.sql(f"""
        CREATE OR REPLACE TEMP VIEW v_cast AS
        SELECT
        {cast_sql}
        FROM v_flat
    """)
    df_out = spark.table("v_cast")
    write_marker(spark, "step6_cast", f"cast columns: {len(df_out.columns)}")

    print("STEP_7 basic info")
    print(f"S3_PATH={S3_PATH}")
    print(f"df_out columns={len(df_out.columns)}")
    print(f"df_out partitions={df_out.rdd.getNumPartitions()}")

    print("STEP_7 printing schema")
    df_out.printSchema()
    print("STEP_7 schema printed successfully")

    print("STEP_7 showing rows")
    df_out.show(5, truncate=False)
    print("STEP_7 show completed")

    print("STEP_7 write iceberg via SQL")
    if PARTITION_ON_PARTITIONDATE_IF_PRESENT and "partitiondate" in df_out.columns:
        spark.sql("""
            CREATE OR REPLACE TEMP VIEW v_final AS
            SELECT
                * EXCEPT (partitiondate),
                CAST(partitiondate AS DATE) AS partitiondate
            FROM v_cast
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
            SELECT * FROM v_cast
        """)

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
        raise