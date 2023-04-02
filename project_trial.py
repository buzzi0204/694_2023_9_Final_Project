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


# for extracting source from source object

def extract_source(input_string):
    sources = ['iPhone', 'Android', 'WebApp', 'Instagram']
    
    for source in sources:
        if source in input_string:
            extracted_source = source
            return extracted_source
        

make_json("corona-out-2","corona_2.json")
#make_json("corona-out-3")

###########################################
## Making SQL DB Connection
############################################

db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="root",  # your password
                     db="trial")        # name of the data base
###########################################
## Reading Json files to dictionary
############################################

# file = open("output_file.json")
# json_data = json.load(file)

# lables = list(json_data[0].keys())

#### trial with sample file
with open("sample.txt", "r") as f:
    data = json.loads(f.read())

##### changing data source to corona-out-two
with open("corona_2.json", "r") as f:
    data = json.loads(f.read())

#########################################################
## inserting to database sql
#######################################################
cur = db.cursor()


# cur.execute(f"Create table trial ({lables[0]} varchar(10))")
# value = json_data[0][lables[0]]
# cur.execute(f"insert into trial values('{value}')")
# db.commit()
# cur.execute("select * from trial")
# for row in cur:
#     print(row[0])

# trying some columns
columns = ('created_at', 'id', 'id_str', 'source', 'truncated')
val1 = data[0]['created_at']
val2 = data[0]['id']
val3 = data[0]['id_str']
val4 = extract_source(data[0]['source'])
val5 = data[0]['truncated']
values = (val1, val2, val3, val4, val5)

'''
Inserting user data
'''

cur.execute("CREATE TABLE tweet (created_at VARCHAR(100),id BIGINT(20) NOT NULL,id_str VARCHAR(100),\
            source VARCHAR(50),truncated BOOLEAN)")
db.commit()

query = "CREATE TABLE user_data (id BIGINT(20),id_str TEXT,name TEXT,username TEXT,location TEXT,url TEXT,description TEXT,translator_type TEXT,protected BOOLEAN,verified BOOLEAN,followers_count BIGINT(50),friends_count BIGINT(50),listed_count BIGINT(50),favourites_count BIGINT(50),no_tweets BIGINT(50),created_at TEXT)"
cur.execute(query)

db.commit()


# val_dict = {}

query_insert = "INSERT INTO user_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

for i in range(len(data)):
    val_list = []
    key_list = list(data[i]['user'].keys())
    for key in key_list[0:16]:
        val_list.append(data[i]['user'][key])
    #val_dict[i] = tuple(val_list)
    cur.execute(query_insert, tuple(val_list))


db.commit()

query = "INSERT INTO tweet (created_at, id, id_str, source, truncated) VALUES\
    (%s,%s,%s,%s,%s)"
    
cur.execute(query, values)
db.commit()


#######################################################
## inserting in mongo db
#############################################


