import json
import MySQLdb

# making file into json format
def make_json(filepath, output_filename="output_file.json"):
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
        
        
#make_json("corona-out-2")
make_json("corona-out-3")

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

file = open("output_file.json")
json_data = json.load(file)

lables = list(json_data[0].keys())   


#########################################################
## inserting to database sql
#######################################################
cur = db.cursor()


cur.execute(f"Create table trial ({lables[0]} varchar(10))")
value = json_data[0][lables[0]]
cur.execute(f"insert into trial values('{value}')")
db.commit()
cur.execute("select * from trial")
for row in cur:
    print(row[0])
    

#######################################################
## inserting in mongo db
#############################################


