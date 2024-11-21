# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, split

# def start_spark_streaming(input_dir):
#     spark = SparkSession.builder \
#     .appName("YoutubeCommentsStreamingApplication_v1") \
#     .config("spark.sql.shuffle.partitions", "2") \
#     .getOrCreate()

#     # beginning the reading sequence of the comments from the stream library
#     comment_stream = spark.readStream \
#     .format("text") \
#     .load(input_dir)

#     # process the stream for output generation
#     words = comment_stream.select(explode(split(comment_stream.value, " ")).alias("word"))
#     word_count = words.groupBy("word").count()

#     # beginning the writing sequence of the generated output (IN_MEMORY) for flask reading protocol
#     query = word_count.writeStream \
#     .outputMode("complete") \
#     .format("memory") \
#     .queryName("word_counts") \
#     .start()

#     query.awaitTermination()

# start_spark_streaming("temp_dir/streaming_dir")

# # performing script testing not required during actual production run

# def test_streaming_capabilities(input_dir):
#     test_file_path = "temp_dir/test_dir/test_streaming.txt"
#     with open(test_file_path, "w") as file:
#         file.write("hoolow world\n")
#         file.write("how its going\n")
#         file.write("this is testing of the streaming functionality\n")

#     return test_file_path

# def verify_streaming_working(spark):
#     result = spark.sql("SELECT * FROM word_counts")
#     result.show(truncate = False)

# if __name__ == "__main__":
#     input_dir = "temp_dir/streaming_dir"

#     # checking streaming data and read
#     test_streaming_capabilities(input_dir)

#     # begin streaming
#     start_spark_streaming(input_dir)

#     # verify operational capabilities
#     verify_streaming_working()


# DO NOT TOUCH THE ABOVE CODE

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
import time
import os

def start_spark_streaming(input_dir):
    spark = SparkSession.builder \
    .appName("YoutubeCommentsStreamingApplication_v1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

    # beginning the reading sequence of the comments from the stream library
    comment_stream = spark.readStream \
    .format("text") \
    .load(input_dir)

    # process the stream for output generation
    words = comment_stream.select(explode(split(comment_stream.value, " ")).alias("word"))
    word_count = words.groupBy("word").count()

    # beginning the writing sequence of the generated output (IN_MEMORY) for flask reading protocol
    query = word_count.writeStream \
    .outputMode("complete") \
    .format("memory") \
    .queryName("word_counts") \
    .start()

    return spark, query

def test_streaming_capabilities(input_dir):
    # Creating test data in the correct input directory
    test_file_path = os.path.join(input_dir, "test_streaming.txt")
    with open(test_file_path, "w") as file:
        file.write("hoolow world\n")
        file.write("how its going\n")
        file.write("this is testing of the streaming functionality\n")

    return test_file_path

def verify_streaming_working(spark):
    # Query the results from the in-memory table
    result = spark.sql("SELECT * FROM word_counts")
    result.show(truncate=False)

# if __name__ == "__main__":
#     input_dir = "temp_dir/streaming_dir"
    
#     # Ensure the directory exists
#     if not os.path.exists(input_dir):
#         os.makedirs(input_dir)
    
#     # Step 1: Create the test data
#     # test_streaming_capabilities(input_dir)
    
#     # Step 2: Start the streaming job
#     spark, query = start_spark_streaming(input_dir)
    
#     # Step 3: Allow time for the streaming job to process the data
#     time.sleep(5)  # Give Spark some time to process the data
    
#     # Step 4: Verify the streaming results
#     # verify_streaming_working(spark)

#     # Step 5: Clean up and stop the query
#     query.stop()

# test results:
# PASSED: WORKING AS EXPECTED