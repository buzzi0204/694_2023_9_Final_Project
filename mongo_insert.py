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

def mongo_insertor(index, keys):
    """
    Args:
        index ([type]): [description]
        keys ([type]): [description]

    Returns:
        [type]: [description]
    """
    obj = {}
    for key in keys:
        obj[key] = index[key]
    obj['user_id'] = index['user']['id']
    return obj


for index in data:
    if 'retweeted_status' in index.keys():
        obj = mongo_insertor(index['retweeted_status'], keys)
        collection.insert_one(obj)
        
    if 'quoted_status' in index.keys():
        obj = mongo_insertor(index['quoted_status'], keys)
        collection.insert_one(obj)
    
    
    obj = mongo_insertor(index, keys)
    collection.insert_one(obj)
