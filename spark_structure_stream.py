from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import time
from youtube_fetch_comments import fetch_comments

def start_streaming(api_key, video_id):
    spark = SparkSession.builder.appName("StreamApp").getOrCreate()

    comment_stream = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", 9999) \
        .load()

    words_df = comment_stream.select(explode(split(comment_stream.value, " ")).alias("word"))
    word_count = words_df.groupby("word").count()

    # output stream
    query = word_count.writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    # query = word_count.writeStream \
    #     .outputMode("complete") \
    #     .format("memory") \
    #     .start()
    
    query.awaitTermination()
