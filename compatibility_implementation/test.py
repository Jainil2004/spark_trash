from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("spark_kafka_test") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.file.impl.disable.cache", "true") \
    .getOrCreate()

kafka_bootstrap_server = 'localhost:9092'
kafka_topic = "test_topic"

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_server) \
    .option("subscribe", kafka_topic) \
    .load()

# Cast Kafka messages to strings
df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Write the messages to a file
query = df \
    .writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", "D:/spark_output") \
    .option("checkpointLocation", "D:/spark_checkpoints") \
    .start()

query.awaitTermination()
