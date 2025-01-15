from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()

print(f"spark version: {spark.version}")

spark.stop()