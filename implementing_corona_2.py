# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 17:38:50 2023

@author: athar
"""

import json
import MySQLdb
import pandas as pd
from pymongo import MongoClient

###########################################################################
## Functions
############################################################################
# making file into json format
def make_json(filepath, output_filename):
    with open(filepath, 'r') as f:
        json_data = f.read()
    
    json_objects = []
    brace_count = 0
    start_index = None
    for i, c in enumerate(json_data):
        if c == '{':
            brace_count += 1
            if brace_count == 1:
                start_index = i
        elif c == '}':
            brace_count -= 1
            if brace_count == 0:
                json_objects.append(json_data[start_index:i+1])

    json_data = ','.join(json_objects)

    data = json.loads('[' + json_data + ']')

    with open(output_filename, 'w') as f:
        json.dump(data, f)
        

make_json("corona-out-2", "./json_files/corona-out-2.json")



with open("./json_files/corona-out-2.json", "r") as f:
    data = json.loads(f.read())


db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="root",  # your password
                     db="trial")
cur = db.cursor()


###########################################################################
####### Inserting user data into sql
#############################################################################

key_list = ['id', 'id_str', 'screen_name', 'name', 'verified', 'description', 'location', 'url',
            'created_at', 'followers_count', 'friends_count', 'favourites_count', 'statuses_count',
            'lang']


query_insert = "INSERT INTO user_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

val_dict = {}
rt_val_dict = {}
qt_val_dict = {}

for i in range(len(data)):
    val_list = []

    for key in key_list:
        if key == 'created_at':
            data[i]['user'][key] = pd.to_datetime(data[i]['user'][key])
        val_list.append(data[i]['user'][key])
    
    val_dict[i] = tuple(val_list)
    
    # SQL Query
    try:
        cur.execute(query_insert, val_list)
    except Exception as e:
        print(e)
        pass

    if 'retweeted_status' in data[i]:
        rt_val_list = []
        for key in key_list:
            if key == 'created_at':
                data[i]['retweeted_status']['user'][key] = pd.to_datetime(data[i]['retweeted_status']['user'][key])
            rt_val_list.append(data[i]['retweeted_status']['user'][key])
    else:
        continue
    
    rt_val_dict[i] = tuple(rt_val_list)
    
    try:
        cur.execute(query_insert, rt_val_list)
    except Exception as e:
        print(e)
        pass

    if 'quoted_status' in data[i]:
        qt_val_list = []
        for key in key_list:
            if key == 'created_at':
                data[i]['quoted_status']['user'][key] = pd.to_datetime(data[i]['quoted_status']['user'][key])
            qt_val_list.append(data[i]['quoted_status']['user'][key])
    else:
        continue

    qt_val_dict[i] = tuple(qt_val_list)
    
    try:
        cur.execute(query_insert, qt_val_list)
    except Exception as e:
        print(e)
        pass

db.commit()

print(len(val_dict))
print(len(rt_val_dict))
print(len(qt_val_dict))

###############################################################################
######### Inserting retweets
##############################################################################

query_insert = "INSERT INTO retweets VALUES(%s,%s,%s,%s)"

val_dict = {}

for i in range(len(data)):
    if 'retweeted_status' in data[i]:
        val_list = []

        val_list.append(data[i]['id'])
        val_list.append(data[i]['retweeted_status']['id'])
        val_list.append(data[i]['user']['id'])
        val_list.append(pd.to_datetime(data[i]['created_at']))

        val_dict[i] = tuple(val_list)

        try:
            cur.execute(query_insert, val_list)
        except Exception as e:
            print(e)
            pass
    else:
        continue


db.commit()

print(len(val_dict))


###############################################################################
######### Inserting quoted tweets
##############################################################################

query_insert = "INSERT INTO quoted_tweets VALUES(%s,%s,%s,%s)"
val_dict = {}

for i in range(len(data)):
    if 'quoted_status' in data[i]:
        val_list = []

        val_list.append(data[i]['id'])
        val_list.append(data[i]['quoted_status']['id'])
        val_list.append(data[i]['user']['id'])
        val_list.append(pd.to_datetime(data[i]['quoted_status']['created_at']))

        val_dict[i] = tuple(val_list)

        try:
            cur.execute(query_insert, val_list)
        except Exception as e:
            print(e)
            pass
    
    else:
        continue
db.commit()
print(len(val_dict))


###############################################################################
########## Inserting tweets
###############################################################################

try:
    conn = MongoClient()
    print("Connected successfully")
except:  
    print("Could not connect to MongoDB")
  
# database
db = conn.trial
collection = db.tweets_data

with open("json_files/corona-out-2.json", "r") as f:
    data = json.load(f)
    
keys = ['id', 'id_str', 'text', 'created_at', 'truncated', 'is_quote_status','qoute_count', 'reply_count', 'entities', 'retweet_count', 'favorite_count', 'lang', 'timestamp_ms', 'geo']

def extract_source(input_string):
    sources = ['iPhone', 'Android', 'WebApp', 'Instagram']
    
    for source in sources:
        if source in input_string:
            extracted_source = source
            return extracted_source

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
    
    obj['user_id'] = index['user']['id']
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