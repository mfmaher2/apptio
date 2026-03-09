import datetime
from pyspark.sql import SparkSession

def init_spark():
    return (
        SparkSession.builder
        .appName("smoke-test")
        .getOrCreate()
    )

def hdfs_write(spark, s3a_path, text):
    # Write using Hadoop FS API (works even without Spark actions)
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(s3a_path), conf)
    out = fs.create(jvm.org.apache.hadoop.fs.Path(s3a_path), True)
    out.write(bytearray(text, "utf-8"))
    out.close()

if __name__ == "__main__":
    spark = init_spark()
    ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    path = f"s3a://mike-apptio-spark-test-2026/debug/smoke/{ts}.txt"
    hdfs_write(spark, path, "SMOKE TEST OK\n")
    spark.stop()