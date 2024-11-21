from flask import Flask, render_template, jsonify
from threading import Thread
from pyspark.sql import SparkSession
from stream_comments_v1 import start_spark_streaming
from FetchComments_v2 import simulate_streaming

app = Flask(__name__)

processed_data = []

# initializing the spark session
spark = SparkSession.builder.appName("FlaskApplication_v1").getOrCreate()

@app.route("/")
def home():
    return render_template("index2.html")

# @app.route("/data")
# def get_data():
#     # perform the querying of the data from the memory
#     try:
#         df =  spark.sql("SELECT word, count from word_counts ORDER BY count DESC")
#         data = df.collect()

#         # converting the data into a serializable JSON object
#         result = [{"word": row.word, "count": row.count} for row in data]
#         return jsonify(result)
    
#     except Exception as e:
#         return jsonify({"error": str(e)})

# performing get_data testing
# @app.route("/data")
# def get_data():
#     try:
#         # Check if word_counts table exists
#         if "word_counts" in [table.name for table in spark.catalog.listTables()]:
#             df = spark.sql("SELECT word, count from word_counts ORDER BY count DESC")
#             data = df.collect()
#             result = [{"word": row.word, "count": row.count} for row in data]
#             return jsonify(result)
#         else:
#             return jsonify({"error": "word_counts table not found yet."})
    
#     except Exception as e:
#         return jsonify({"error": str(e)})

# performing more redundant testing
@app.route("/data")
def get_data():
    try:
        # Wait until the 'word_counts' table is available in the in-memory catalog
        table_found = False
        while not table_found:
            table_found = spark.catalog.tableExists("word_counts")
            if not table_found:
                time.sleep(1)  # Wait 1 second and check again

        # Once the table is available, query it
        df = spark.sql("SELECT word, count from word_counts ORDER BY count DESC")
        data = df.collect()

        # Convert the rows to dictionaries with proper values, not methods
        result = [{"word": row["word"], "count": row["count"]} for row in data]
        
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)})


def start_spark_streaming_in_background():
    # Start the spark streaming job
    API_KEY = "AIzaSyBBzGdX-KNdJut80l1Kri7EfsrRmXzAWV0"
    VIDEO_ID = "GIUWA5PDnAo"
    # start_spark_streaming(API_KEY, VIDEO_ID)
    start_spark_streaming("temp_dir/streaming_dir")

def start_fetching_comments_in_background():
    # Start fetching comments job
    API_KEY = "AIzaSyBBzGdX-KNdJut80l1Kri7EfsrRmXzAWV0"
    VIDEO_ID = "GIUWA5PDnAo"
    simulate_streaming(API_KEY, VIDEO_ID, "temp_dir/streaming_dir")

if __name__ == "__main__":
    # Run background jobs in separate threads
    spark_fetch_comments_thread = Thread(target=start_fetching_comments_in_background)
    spark_streaming_thread = Thread(target=start_spark_streaming_in_background)
    spark_fetch_comments_thread.start()
    spark_streaming_thread.start()

    # Start the Flask app in the main thread
    app.run(debug=True)