from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import time
from youtube_fetch_comments import fetch_comments

def start_streaming(api_key, video_id):
    spark = SparkSession.builder.appName("StreamApp").getOrCreate()

    # comments_df = spark.readStream.text("")

    data = []

    def get_comments():
        comments = fetch_comments(api_key, video_id)
        for comment in comments: 
            data.append((comment),)

# keep fetching comments and show the processed stuff
    while True:
        get_comments()
        df = spark.createDataFrame(data, ["Comment"])
        words_df = df.select(explode(split(df.comment, " ")).alias("word"))
        word_count = words_df.groupby("word").count()
        word_count.show()
        time.sleep(10)
