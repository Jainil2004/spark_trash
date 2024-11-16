import requests

def fetch_comments(api_key, video_id):
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part" : "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults" : 20
    }

    comments = []

    response = requests.get(url, params)
    if response.status_code == 200:
        for item in response.json().get("items", []):
            comment = (item["snippet"]["topLevelComment"]["snippet"]["textDisplay"])
            comments.append(comment)
    else:
        print(f"error: {response.json()}")
    
    return comments

# VIDEO_URL = "https://www.youtube.com/watch?v=GIUWA5PDnAo"
API_KEY = "AIzaSyBBzGdX-KNdJut80l1Kri7EfsrRmXzAWV0"
VIDEO_ID = "GIUWA5PDnAo"
# fetch_comments(API_KEY, VIDEO_ID)
