from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Create a Spark session
spark = SparkSession.builder.appName("Test").getOrCreate()

checkpoint_dir = "D:/spark_checkpoints"

# Dummy data
data = [("Alice", 1), ("Bob", 2)]
columns = ["name", "value"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Set checkpoint directory
checkpoint_dir = "D:/test_checkpoint"
df.writeStream \
  .outputMode("append") \
  .format("console") \
  .option("checkpointLocation", checkpoint_dir) \
  .start() \
  .awaitTermination()
