# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 17:38:50 2023

@author: athar
"""

import json
import MySQLdb

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
        val_list.append(data[i]['user'][key])
    
    val_dict[i] = tuple(val_list)
    
    # SQL Query
    cur.execute(query_insert, val_list)

    if 'retweeted_status' in data[i]:
        rt_val_list = []
        for key in key_list:
            rt_val_list.append(data[i]['retweeted_status']['user'][key])
    else:
        continue
    
    rt_val_dict[i] = tuple(rt_val_list)
    cur.execute(query_insert, rt_val_list)

    if 'quoted_status' in data[i]:
        qt_val_list = []
        for key in key_list:
            qt_val_list.append(data[i]['quoted_status']['user'][key])
    else:
        continue

    qt_val_dict[i] = tuple(qt_val_list)
    cur.execute(query_insert, qt_val_list)

print(len(val_dict))
print(len(rt_val_dict))
print(len(qt_val_dict))






