import json

with open("json_files/tweet_retweet_quote_trial.json", "r") as f:
    data = json.loads(f.read())

key_list = ['id', 'id_str', 'screen_name', 'name', 'verified', 'description', 'location', 'url',
            'created_at', 'followers_count', 'friends_count', 'favourites_count', 'statuses_count',
            'lang']

val_dict = {}
rt_val_dict = {}
qt_val_dict = {}

for i in range(len(data)):
    val_list = []

    for key in key_list:
        val_list.append(data[i]['user'][key])
    
    val_dict[i] = tuple(val_list)

    if 'retweeted_status' in data[i]:
        rt_val_list = []
        for key in key_list:
            rt_val_list.append(data[i]['retweeted_status']['user'][key])
    else:
        continue
    
    rt_val_dict[i] = tuple(rt_val_list)

    if 'quoted_status' in data[i]:
        qt_val_list = []
        for key in key_list:
            qt_val_list.append(data[i]['quoted_status']['user'][key])
    else:
        continue

    qt_val_dict[i] = tuple(qt_val_list)

print(len(val_dict))
print(len(rt_val_dict))
print(len(qt_val_dict))

# print(qt_val_dict)