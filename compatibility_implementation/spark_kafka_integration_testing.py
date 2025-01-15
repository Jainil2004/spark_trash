from pyspark.sql import SparkSession

# Initialize Spark session with required configurations
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

# Print messages to console with checkpointing
query = df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "./spark_checkpoints") \
    .start()

query.awaitTermination()

spark.stop()