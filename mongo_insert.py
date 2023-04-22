from pymongo import MongoClient
import json
from db_insert_user import extract_source

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
    "truncated",
    "is_quote_status",
    "qoute_count",
    "reply_count",
    "entities",
    "retweet_count",
    "favorite_count",
    "lang",
    "timestamp_ms",
    "geo",
]


def mongo_insertor(index, keys):
    """

    Args:
        index ([type]): [description]
        keys ([type]): [description]

    Returns:
        [type]: [description]
    """
    obj = {
        "_id": index['id'],
        "source": extract_source(index['source'])
        }
        

    for key in keys:
        try:
            obj[key] = index[key]
        except:
            pass

    if 'extended_tweet' in index.keys():
        obj['text'] = index['extended_tweet']['full_text']
    
    
    obj['user_id'] = index['user']['id']

    obj['popularity'] = index['quote_count'] + index['reply_count'] + index['retweet_count'] + index['favorite_count']
    return obj



for index in data:
    if 'retweeted_status' in index.keys():
        obj = mongo_insertor(index['retweeted_status'], keys)
        try:
            collection.insert_one(obj)
        except Exception as e:
            print(e)
            pass
        
    if 'quoted_status' in index.keys():
        obj = mongo_insertor(index['quoted_status'], keys)
        try:
            collection.insert_one(obj)
        except Exception as e:
            print(e)
            pass
    
    
    obj = mongo_insertor(index, keys)
    try:
        collection.insert_one(obj)
    except Exception as e:
        print(e)
        pass