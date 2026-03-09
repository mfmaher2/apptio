# README — Testing Spark Iceberg Writes to AWS S3 with watsonx.data

## Overview

This document outlines the steps required to:

1. Create an AWS S3 bucket for Spark testing
2. Create IAM credentials for Spark access
3. Configure the bucket inside **watsonx.data Infrastructure Manager**
4. Configure an **Apache Iceberg catalog**
5. Submit a Spark job that writes data to the Iceberg table stored in S3

The goal is to validate that the Spark engine can hydrate an Iceberg table in S3 using data from parquet files.

---

# Architecture

```
Spark Engine (watsonx.data)
        │
        │
        ▼
Iceberg Catalog
        │
        ▼
AWS S3 Bucket
mike-apptio-spark-test-2026
```

Directory structure in S3:

```
mike-apptio-spark-test-2026
│
├── ddl/
│     cost_and_usage_all.sql
│
├── parquet/
│     <test parquet files>
│
├── scripts/
│     mike_george_append_deepa.py
│
└── iceberg/
      <Iceberg table metadata + data files>
```

---

# Step 1 — Create S3 Bucket

Create the test bucket:

```bash
aws s3 mb s3://mike-apptio-spark-test-2026 --region us-east-1
```

Verify bucket exists:

```bash
aws s3 ls
```

Expected output:

```
mike-apptio-spark-test-2026
```

---

# Step 2 — Create Folder Structure

Create required directories:

```bash
aws s3api put-object --bucket mike-apptio-spark-test-2026 --key parquet/
aws s3api put-object --bucket mike-apptio-spark-test-2026 --key ddl/
aws s3api put-object --bucket mike-apptio-spark-test-2026 --key iceberg/
aws s3api put-object --bucket mike-apptio-spark-test-2026 --key scripts/
```

Verify:

```bash
aws s3 ls s3://mike-apptio-spark-test-2026
```

Expected:

```
PRE ddl/
PRE iceberg/
PRE parquet/
PRE scripts/
```

---

# Step 3 — Upload Test Files

Upload the DDL file:

```bash
aws s3 cp cost_and_usage_all.sql \
s3://mike-apptio-spark-test-2026/ddl/
```

Upload Spark script:

```bash
aws s3 cp mike_george_append_deepa.py \
s3://mike-apptio-spark-test-2026/scripts/
```

Upload test parquet files:

```bash
aws s3 cp parquet/ \
s3://mike-apptio-spark-test-2026/parquet/ \
--recursive
```

---

# Step 4 — Create IAM User for Spark Access

Create IAM user:

```
watsonx-s3-test
```

Attach policy allowing S3 access to the bucket.

Example policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SparkS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::mike-apptio-spark-test-2026"
    },
    {
      "Sid": "SparkObjectAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::mike-apptio-spark-test-2026/*"
    }
  ]
}
```

Generate **Access Key + Secret Key** for the user.

---

# Step 5 — Validate S3 Access

Run a quick test from CLI:

```bash
aws s3 ls s3://mike-apptio-spark-test-2026
```

Test write:

```bash
echo test > test.txt

aws s3 cp test.txt s3://mike-apptio-spark-test-2026/test.txt
```

Verify:

```bash
aws s3 ls s3://mike-apptio-spark-test-2026
```

Remove test file:

```bash
aws s3 rm s3://mike-apptio-spark-test-2026/test.txt
```

---

# Step 6 — Configure Storage in watsonx.data

Navigate to:

```
Infrastructure Manager
```

Add new component:

```
Amazon S3
```

Configuration:

| Field    | Value                                                                    |
| -------- | ------------------------------------------------------------------------ |
| Bucket   | mike-apptio-spark-test-2026                                              |
| Region   | us-east-1                                                                |
| Endpoint | [https://s3.us-east-1.amazonaws.com](https://s3.us-east-1.amazonaws.com) |

Credentials:

```
Access Key: <IAM access key>
Secret Key: <IAM secret key>
```

Click **Test Connection**.

Expected result:

```
Connection successful
```

---

# Step 7 — Create Iceberg Catalog

Add catalog component:

```
Apache Iceberg
```

Configuration:

```
Catalog name: iceberg_data_test
```

Associate with storage bucket:

```
mike-apptio-spark-test-2026
```

Final infrastructure graph:

```
Spark Engine
     │
Iceberg Catalog
     │
S3 Bucket
```

---

# Step 8 — Submit Spark Application

Navigate to:

```
Spark Engine → Applications → Submit Application
```

Application path:

```
s3a://mike-apptio-spark-test-2026/scripts/mike_george_append_deepa.py
```

Application name:

```
mike_iceberg_test
```

Spark configuration:

```
spark.driver.cores=2
spark.driver.memory=8G
spark.executor.cores=2
spark.executor.memory=8G
ae.spark.executor.count=2
spark.sql.shuffle.partitions=64
spark.hadoop.fs.s3a.endpoint=s3.us-east-1.amazonaws.com
```

---

# Step 9 — Hardware Configuration

Driver:

```
2 vCPU
8 GB RAM
```

Executors:

```
2 executors
2 vCPU each
8 GB RAM each
```

---

# Step 10 — Verify Iceberg Table

Once the job completes successfully, verify using SQL:

```sql
SHOW SCHEMAS IN iceberg_data_test;
```

Expected:

```
dev
```

List tables:

```sql
SHOW TABLES IN iceberg_data_test.dev;
```

Expected:

```
deepa_20260222
```

Check row count:

```sql
SELECT COUNT(*)
FROM iceberg_data_test.dev.deepa_20260222;
```

Preview records:

```sql
SELECT *
FROM iceberg_data_test.dev.deepa_20260222
LIMIT 10;
```

---

# Step 11 — Debugging the Spark → Iceberg Pipeline

During initial testing, several issues occurred that required incremental debugging of the Spark pipeline.

To isolate failures, a **checkpoint-based debugging version of the Spark script** was created.

Script:

```
mike_george_append_deepa2.py
```

Location in S3:

```
s3://mike-apptio-spark-test-2026/scripts/mike_george_append_deepa2.py
```

This version of the script adds:

* step-by-step execution checkpoints
* S3 marker files written after each successful step
* schema inspection before writing to Iceberg
* row preview validation

---

# Step 12 — Debug Marker Files

Each pipeline stage writes a marker file to S3 so that failures can be isolated quickly.

Marker location:

```
s3://mike-apptio-spark-test-2026/debug/markers/
```

Example markers:

```
step0_start
step1_db_created
step2_ddl_read
step3_parquet_read
step4_lowercase
step5_flatten
step6_cast
step7_write_ok
```

Example command to inspect markers:

```bash
aws s3 ls s3://mike-apptio-spark-test-2026/debug/markers/ --recursive
```

Example output:

```
step0_start/_SUCCESS
step1_db_created/_SUCCESS
step2_ddl_read/_SUCCESS
step3_parquet_read/_SUCCESS
step4_lowercase/_SUCCESS
step5_flatten/_SUCCESS
step6_cast/_SUCCESS
step7_write_ok/_SUCCESS
```

These markers confirm that the pipeline executed successfully through each stage.

---

# Step 13 — Minimal Spark Debug Scripts

To isolate S3 and Spark behavior, several minimal Spark scripts were created.

These scripts are stored in:

```
s3://mike-apptio-spark-test-2026/scripts/
```

## tiny_s3_write.py

Purpose:

Verify that Spark can write files to S3.

Output location:

```
s3://mike-apptio-spark-test-2026/debug/tiny_s3_write/
```

Verification command:

```bash
aws s3 ls s3://mike-apptio-spark-test-2026/debug/tiny_s3_write/ --recursive
```

---

## tiny_parquet_read2.py

Purpose:

Verify Spark can read Parquet files from S3.

Input location:

```
s3://mike-apptio-spark-test-2026/debug/tiny_s3_write_input/
```

Output confirms that:

* Spark S3A connectivity works
* parquet reader configuration is correct

---

# Step 14 — Updated Spark Script Input Path

For debugging, the Spark pipeline was temporarily pointed at the minimal test parquet dataset.

Updated variable inside the script:

```python
S3_PATH = "s3a://mike-apptio-spark-test-2026/debug/tiny_s3_write_input/"
```

This ensured the pipeline could be validated with a small dataset before processing large CUR parquet files.

---

# Step 15 — Successful Spark Pipeline Execution

After debugging, the Spark job completed successfully.

Application name:

```
mike_iceberg_test
```

Script used:

```
mike_george_append_deepa2.py
```

Successful execution confirmed that:

* Spark read parquet files from S3
* schema normalization worked
* struct flattening succeeded
* type casting completed
* Iceberg table commit succeeded

---

# Step 16 — Querying the Iceberg Table

Using the **watsonx.data Query Workspace**, the Iceberg table was validated.

Engine used:

```
prestocpp
```

Catalog:

```
iceberg_data_test
```

Schema:

```
dev
```

Table:

```
deepa_checkpoint_test
```

---

## Verify table exists

```sql
SHOW TABLES FROM iceberg_data_test.dev;
```

Expected result:

```
deepa_checkpoint_test
```

---

## Query sample data

```sql
SELECT *
FROM iceberg_data_test.dev.deepa_checkpoint_test
LIMIT 10;
```

---

## Verify Iceberg data files

Iceberg metadata exposes the underlying parquet files using the `$path` column.

```sql
SELECT "$path"
FROM iceberg_data_test.dev.deepa_checkpoint_test
LIMIT 10;
```

Example output:

```
s3a://mike-apptio-spark-test-2026/iceberg/deepa_checkpoint_test/data/...
```

This confirms the Iceberg table is backed by parquet files written by Spark.

---

# Final Architecture

After successful testing, the full pipeline architecture is:

```
Spark Engine (watsonx.data)
        │
        │
        ▼
Spark Transformation Job
(mike_george_append_deepa2.py)
        │
        ▼
Apache Iceberg Table
iceberg_data_test.dev.deepa_checkpoint_test
        │
        ▼
AWS S3 Storage
mike-apptio-spark-test-2026
        │
        ▼
Presto SQL Queries
(watsonx.data Query Workspace)
```

---

# Current Status

The Spark → Iceberg pipeline is fully functional.

Validated components:

```
✔ S3 connectivity
✔ Spark S3A reads
✔ Spark S3A writes
✔ Snowflake DDL parsing
✔ Parquet ingestion
✔ Schema normalization
✔ Iceberg table creation
✔ Iceberg data writes
✔ Presto SQL queries
```

---

# Next Steps

Planned improvements:

* Process full AWS CUR parquet datasets
* Partition Iceberg tables by `partitiondate`
* Optimize Iceberg write settings
* Implement incremental ingestion
* Add automated schema validation
* Add Spark performance tuning

---

If you'd like, I can also add one **final section that makes this README extremely strong for engineering documentation**:

**"Common Errors Encountered and Fixes"**

It would document things like:

* S3 `AccessDenied`
* `ListObjectsV2` permission failures
* Spark S3A config issues
* Iceberg commit errors

# Expected Iceberg Files in S3

After successful write:

```
s3://mike-apptio-spark-test-2026/iceberg/
```

Structure:

```
metadata/
data/
snapshots/
manifests/
```

---

# Notes

Key configuration values in the Spark script:

```python
WXD_CATALOG = "iceberg_data_test"
WXD_SCHEMA = "dev"
TARGET_TABLE_NAME = "deepa_20260222"
```

DDL location:

```
s3://mike-apptio-spark-test-2026/ddl/cost_and_usage_all.sql
```

---

# Future Improvements

Recommended script improvements:

* Replace boto3 S3 reads with Spark S3A reads
* Add structured logging to S3
* Partition optimization for Iceberg writes
* Add Spark adaptive query settings
* Add retry logic for schema casts

