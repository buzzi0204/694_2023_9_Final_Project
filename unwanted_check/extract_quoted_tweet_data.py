import json
import pandas as pd

with open("make_json_trial.json", "r") as f:
    data = json.loads(f.read())

val_dict = {}

for i in range(len(data)):
    if 'quoted_status' in data[i]:
        val_list = []

        val_list.append(data[i]['id'])
        val_list.append(data[i]['quoted_status']['id'])
        val_list.append(data[i]['user']['id'])
        val_list.append(pd.to_datetime(data[i]['quoted_status']['created_at']))

        val_dict[i] = tuple(val_list)

        # try:
        #     cur.execute(query_insert, val_list)
        # except Exception as e:
        #     print(e)
        #     pass
    
    else:
        continue

print(len(val_dict))