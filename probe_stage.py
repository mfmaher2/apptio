#!/usr/bin/env python3
import sys
print("STAGE_0: script started")

print("STAGE_1: importing pyspark")
from pyspark.sql import SparkSession
print("STAGE_2: imported pyspark")

print("STAGE_3: creating SparkSession")
spark = SparkSession.builder.appName("probe-stage").getOrCreate()
print("STAGE_4: SparkSession created")

print("STAGE_5: spark version:", spark.version)

print("STAGE_6: running trivial spark sql")
spark.sql("SELECT 1").show()

print("STAGE_7: done")
spark.stop()