from flask import Flask, render_template, jsonify
from threading import Thread
from spark_streaming import start_streaming
# from spark_structure_stream import start_streaming

app = Flask(__name__)

processed_data = []


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/data")
def data():
    API_KEY = "AIzaSyBBzGdX-KNdJut80l1Kri7EfsrRmXzAWV0"
    VIDEO_ID = "GIUWA5PDnAo"

    comment_thread = Thread(target=start_streaming, args=(API_KEY, VIDEO_ID))
    comment_thread.start()
    
    return jsonify(processed_data)


if __name__ == "__main__":
    app.run(debug=True)