#!/usr/bin/env python3
import json
import traceback
from pyspark.sql import SparkSession

S3_BUCKET = "mike-apptio-spark-test-2026"
DDL_OBJECT_KEY = "ddl/cost_and_usage_all.sql"
S3_PATH = "s3a://mike-apptio-spark-test-2026/parquet/"
DB_LOCATION = "s3a://mike-apptio-spark-test-2026/iceberg/"
WXD_CATALOG = "iceberg_data"
WXD_SCHEMA = "dev"

def emit(spark, lines, out_path):
    spark.sparkContext.parallelize(lines, 1).saveAsTextFile(out_path)

spark = SparkSession.builder.appName("probe-deepa").getOrCreate()

results = []

# 1) boto3 S3 access
try:
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    resp = s3.head_object(Bucket=S3_BUCKET, Key=DDL_OBJECT_KEY)
    results.append("BOTO3_HEAD_OK")
    results.append(json.dumps({
        "ContentLength": resp.get("ContentLength"),
        "LastModified": str(resp.get("LastModified"))
    }))
except Exception as e:
    results.append("BOTO3_HEAD_FAIL")
    results.append(str(e))
    results.append(traceback.format_exc())

# 2) Spark S3A read of DDL
try:
    ddl_path = f"s3a://{S3_BUCKET}/{DDL_OBJECT_KEY}"
    ddl_lines = spark.read.text(ddl_path).limit(5).collect()
    results.append("SPARK_DDL_READ_OK")
    results.extend([r.value for r in ddl_lines])
except Exception as e:
    results.append("SPARK_DDL_READ_FAIL")
    results.append(str(e))
    results.append(traceback.format_exc())

# 3) Spark parquet read
try:
    df = spark.read.option("recursiveFileLookup", "true").option("pathGlobFilter", "*.parquet").parquet(S3_PATH)
    results.append("SPARK_PARQUET_READ_OK")
    results.append(f"columns={len(df.columns)}")
except Exception as e:
    results.append("SPARK_PARQUET_READ_FAIL")
    results.append(str(e))
    results.append(traceback.format_exc())

# 4) Catalog / DB create
try:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {WXD_CATALOG}.{WXD_SCHEMA} LOCATION '{DB_LOCATION}'")
    results.append("CATALOG_CREATE_DB_OK")
except Exception as e:
    results.append("CATALOG_CREATE_DB_FAIL")
    results.append(str(e))
    results.append(traceback.format_exc())

emit(spark, results, "s3a://mike-apptio-spark-test-2026/debug/probe_deepa")

spark.stop()