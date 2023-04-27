import json
import MySQLdb

with open("json_files/is_retweet.json", "r") as f:
    data = json.loads(f.read())

def extract_source(input_string):
    sources = ['iPhone', 'Android', 'WebApp', 'Instagram']
    
    for source in sources:
        if source in input_string:
            extracted_source = source
            return extracted_source

val_dict = {}

for i in range(len(data)):
    val_list = []
    key_list = list(data[i]['user'].keys())
    for key in key_list:                       # ipynb file has key_list[0:16]
        val_list.append(data[i]['user'][key])
    val_dict[i] = tuple(val_list)

print(len(val_dict))