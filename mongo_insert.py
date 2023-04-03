from pymongo import MongoClient
import json


# Connecting to database
try:
    conn = MongoClient()
    print("Connected successfully")
except:
    print("Could not connect to MongoDB")

# database
db = conn.database
collection = db.tweets_data

with open("json_files/sample_json_file.json", "r") as f:
    data = json.load(f)


keys = [
    "id",
    "id_str",
    "text",
    "created_at",
    "entities",
    "retweet_count",
    "favorite_count",
    "lang",
]

for index in data:
    obj = {}
    for key in keys:
        obj[key] = index[key]
    obj["user_id"] = index["user"]["id"]
    collection.insert_one(obj)
