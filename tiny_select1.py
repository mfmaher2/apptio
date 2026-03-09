from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tiny-select1").getOrCreate()
spark.sql("SELECT 1 AS ok").show()
print("TINY_SELECT1_OK")
spark.stop()