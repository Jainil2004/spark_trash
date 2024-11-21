import requests
import time
import os, uuid

# WORKING AS EXPECTED

def fetch_comments(api_key, video_id, output_dir):
    URL = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part" : "snippet",
        "videoId" : video_id,
        "key" : api_key,
        "maxResults" : 20
    }

    comments = []

    response = requests.get(URL, params)
    if response.status_code == 200:
        comments = [
            item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            for item in response.json().get("items", [])
        ]

        # write these comments to a file so that spark can stream from those files
        if comments:
            file_path = os.path.join(output_dir, f"{uuid.uuid4()}.txt")
            with open(file_path, "w") as f:
                for comment in comments:
                    f.write(comment + "\n")
    else:
        print(f"there was some error fetching the comments: {response.status_code}")

def simulate_streaming(api_key, video_id, output_dir):
    while True:
        fetch_comments(api_key, video_id, output_dir)
        time.sleep(10)

API_KEY = "AIzaSyBBzGdX-KNdJut80l1Kri7EfsrRmXzAWV0"
VIDEO_ID = "GIUWA5PDnAo"

# simulate_streaming(API_KEY, VIDEO_ID, "temp_dir/streaming_dir")