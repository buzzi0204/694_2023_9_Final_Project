# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 17:38:50 2023

@author: athar
"""

import json
import MySQLdb
import pandas as pd
from pymongo import MongoClient
from implementing_cache import Cache
from bson import json_util, Int64
import time


###############################################################################
# Functions
###############################################################################

# This function reads the json data from a file, separates json objects from the file content
# and then saves the resulting json data to a new file.
def make_json(filepath, output_filename):
    
    """
    Reads json data from a file, separates json objects from the file content,
    and saves the resulting json data to a new file.

    Args:
    - filepath (str): The path of the input file containing json data.
    - output_filename (str): The name of the output file to be created with the resulting json data.

    Returns:
    - None
    """
    
    # Open the file with read mode
    with open(filepath, 'r') as f:
        json_data = f.read()

    # Separate json objects from the file content
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

    # Join the separated json objects and convert to valid json string
    json_data = ','.join(json_objects)
    data = json.loads('[' + json_data + ']')

    # Write the resulting json data to a new file
    with open(output_filename, 'w') as f:
        json.dump(data, f)

# Call the make_json function with input file path and output file path arguments
make_json("corona-out-2", "./json_files/corona-out-2.json")

# Open the newly created file and load the json data into a variable named 'data'
with open("./json_files/corona-out-2.json", "r") as f:
    data = json.loads(f.read())

# Create a connection to a MySQL database and get a cursor object to execute SQL statements
db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="root",  # your password
                     db="trial")
cur = db.cursor()


###############################################################################
# Inserting user data into sql
###############################################################################
"""
This script extracts user account information from a Twitter data list and stores it in a MySQL database.
It also creates dictionaries to store the user information for retweets and quoted tweets separately.

Variables:
----------
key_list : list
    A list of keys that correspond to the attributes of a user account.
query_insert : str
    A SQL insert query statement for inserting the user data into the database.
val_dict : dict
    A dictionary to store user account information for original tweets.
rt_val_dict : dict
    A dictionary to store user account information for retweets.
qt_val_dict : dict
    A dictionary to store user account information for quoted tweets.

Returns:
--------
None
"""

# Define a list of keys that correspond to the attributes of a user account
key_list = ['id', 'id_str', 'screen_name', 'name', 'verified', 'description', 'location', 'url',
            'created_at', 'followers_count', 'friends_count', 'favourites_count', 'statuses_count',
            'lang']

# Define a SQL insert query statement
query_insert = "INSERT INTO user_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

# Initialize dictionaries to store user account information for retweets and quoted tweets
val_dict = {}
rt_val_dict = {}
qt_val_dict = {}

# Iterate over the twitter data list to extract user account information
for i in range(len(data)):
    val_list = []

    # Iterate over the list of keys and append the corresponding value to the val_list
    # For 'created_at' key, convert the value to datetime object using pandas.to_datetime
    for key in key_list:
        if key == 'created_at':
            data[i]['user'][key] = pd.to_datetime(data[i]['user'][key])
        val_list.append(data[i]['user'][key])

    # Store the values in a dictionary with the index as the key
    val_dict[i] = tuple(val_list)

    # Execute the SQL insert statement with the values from the val_list
    try:
        cur.execute(query_insert, val_list)
    except Exception as e:
        print(e)
        pass

    # Check if the tweet is a retweet and extract the retweeted user's account information
    if 'retweeted_status' in data[i]:
        rt_val_list = []
        for key in key_list:
            if key == 'created_at':
                data[i]['retweeted_status']['user'][key] = pd.to_datetime(
                    data[i]['retweeted_status']['user'][key])
            rt_val_list.append(data[i]['retweeted_status']['user'][key])
    else:
        continue

    # Store the retweeted user's information in a dictionary with the index as the key
    rt_val_dict[i] = tuple(rt_val_list)

    # Execute the SQL insert statement with the values from the rt_val_list
    try:
        cur.execute(query_insert, rt_val_list)
    except Exception as e:
        print(e)
        pass

    # Check if the tweet is a quoted tweet and extract the quoted user's account information
    if 'quoted_status' in data[i]:
        qt_val_list = []
        for key in key_list:
            if key == 'created_at':
                data[i]['quoted_status']['user'][key] = pd.to_datetime(
                    data[i]['quoted_status']['user'][key])
            qt_val_list.append(data[i]['quoted_status']['user'][key])
    else:
        continue

    # Store the quoted user's information in a dictionary with the index as the key
    qt_val_dict[i] = tuple(qt_val_list)

    # Execute the SQL insert statement with the values from the qt_val_list
    try:
        cur.execute(query_insert, qt_val_list)
    except Exception as e:
        print(e)
        pass

# Commit the changes made to the database
db.commit()

# Print the length of each dictionary that stores the user information
print(len(val_dict))
print(len(rt_val_dict))
print(len(qt_val_dict))

###############################################################################
# Inserting retweets
###############################################################################
"""
This script extracts retweet information from a Twitter data list and stores it in a MySQL database. 
It creates a dictionary to store the retweet information for each retweet.

Variables:
----------
query_insert : str
    A SQL insert query statement for inserting the retweet data into the database.
val_dict : dict
    A dictionary to store retweet data.

Returns:
--------
None
"""

# Define a SQL insert query statement
query_insert = "INSERT INTO retweets VALUES(%s,%s,%s,%s)"

# Initialize dictionaries to store retweet data
val_dict = {}


for i in range(len(data)):
    if 'retweeted_status' in data[i]:
        val_list = []
        
        # Append values of 'id', 'retweeted_status id', 'user id' and 'created_at' to the list
        val_list.append(data[i]['id'])
        val_list.append(data[i]['retweeted_status']['id'])
        val_list.append(data[i]['user']['id'])
        val_list.append(pd.to_datetime(data[i]['created_at']))
        
        # Add the tuple of values to the dictionary with the index i as the key
        val_dict[i] = tuple(val_list)

        # Execute the SQL query to insert the values into the table
        try:
            cur.execute(query_insert, val_list)
        except Exception as e:
            print(e)
            pass
    else:
        continue

# Commit the changes made to the database
db.commit()

# Print the length of each dictionary that stores the information
print(len(val_dict))


###############################################################################
# Inserting quoted tweets
###############################################################################
"""
This script inserts data into a 'quoted_tweets' table in a SQL database. It checks if each element of a given 'data' list
contains a 'quoted_status' key, and if so, creates a row with four values (tweet ID, quoted tweet ID, user ID, and created time of the quoted tweet).
It then executes an SQL query to insert the values into the database, and prints the number of rows that were successfully inserted.

The SQL query is:
    INSERT INTO quoted_tweets VALUES(%s,%s,%s,%s)

Parameters:
    - data (list of dict): A list of JSON objects containing tweet data.
    - cur (cursor object): A cursor object that allows interaction with the SQL database.
    - db (database connection object): A connection object that represents a connection to the SQL database.

Returns:
    None
"""
# This is an SQL query that will insert data into the 'quoted_tweets' table
query_insert = "INSERT INTO quoted_tweets VALUES(%s,%s,%s,%s)"

# Create an empty dictionary to store values for each row to be inserted into the table
val_dict = {}

# Loop through each data element and check if it contains a 'quoted_status' key
for i in range(len(data)):
    if 'quoted_status' in data[i]:
        # Create a list to store the values for this row
        val_list = []

        # Append the required values to the list
        val_list.append(data[i]['id'])
        val_list.append(data[i]['quoted_status']['id'])
        val_list.append(data[i]['user']['id'])
        val_list.append(pd.to_datetime(data[i]['quoted_status']['created_at']))

        # Convert the list to a tuple and store it in the dictionary with the index as the key
        val_dict[i] = tuple(val_list)

        # Execute the SQL query with the values from the list
        try:
            cur.execute(query_insert, val_list)
        except Exception as e:
            print(e)
            pass

    # If the 'quoted_status' key is not present, skip this iteration of the loop
    else:
        continue

# Commit the changes to the database
db.commit()

# Print the number of rows that were successfully inserted into the table
print(len(val_dict))

###############################################################################
# Inserting tweets
###############################################################################
"""
This script extracts data from a JSON file containing tweet data, and inserts the data into a MongoDB database. It loops through each tweet in the data object and extracts the tweet's ID, text, created time, source, user ID, language, and popularity score (the sum of the counts for quote, reply, retweet, and favorite). The extracted data is then inserted into the 'tweets_data' collection in the 'trial' database in MongoDB.

The MongoDB connection is established using the MongoClient class from the PyMongo library. The connection is tested by attempting to create a MongoClient object.

The JSON file is opened using Python's built-in open function, and the data is loaded into a Python object using the json.load function.

The extract_source function takes a string input and returns the source of the tweet, which is extracted from the 'source' field of the tweet.

The mongo_insertor function takes a tweet object and a list of keys to extract from the tweet. It creates a dictionary object containing the extracted data, and returns the dictionary.

Parameters:
    - None

Returns:
    None
"""

# Try to establish a connection to MongoDB
try:
    conn = MongoClient()
    print("Connected successfully")
except:
    print("Could not connect to MongoDB")

# Connect to the 'trial' database and the 'tweets_data' collection
db = conn.trial
collection = db.tweets_data

# Open the specified JSON file and load the data into a Python object
with open("json_files/corona-out-2.json", "r") as f:
    data = json.load(f)

# Define the keys that will be used to extract data from the JSON object
keys = ['id', 'id_str', 'text', 'created_at', 'is_quote_status', 'quote_count',
        'reply_count', 'entities', 'retweet_count', 'favorite_count', 'lang', 'timestamp_ms', 'geo']

# Define a function to extract the source of a tweet from its source string
def extract_source(input_string):
    
    """
    This function extracts the source of a tweet from its source string.

    Parameters:
    input_string (str): The source string of the tweet.

    Returns:
    str: The source of the tweet (either 'iPhone', 'Android', 'WebApp', or 'Instagram').
    If the source is not found, returns None.
    """
    
    sources = ['iPhone', 'Android', 'WebApp', 'Instagram']

    for source in sources:
        if source in input_string:
            extracted_source = source
            return extracted_source

# Define a function to insert a tweet into MongoDB
def mongo_insertor(index, keys):
    
    """
    This function takes a JSON object representing a tweet and inserts it into a MongoDB collection.
    It extracts the specified keys from the JSON object and adds them to a dictionary, along with
    other relevant information such as the tweet's source, user ID, and popularity score.

    Parameters:
    index (dict): A JSON object representing a tweet.
    keys (list): A list of keys to extract from the tweet JSON object.

    Returns:
    dict: A dictionary containing the relevant information extracted from the tweet JSON object.
    """
    
    obj = {
        "_id": Int64(index['id']),
        "source": extract_source(index['source'])
    }

    # Extract the specified keys from the JSON object and add them to the 'obj' dictionary
    for key in keys:
        try:
            obj[key] = index[key]
        except:
            pass

    # If the tweet has an extended tweet, replace the 'text' field with the full text
    if 'extended_tweet' in index.keys():
        obj['text'] = index['extended_tweet']['full_text']

    obj['user_id'] = Int64(index['user']['id'])

    # Calculate the tweet's popularity score by adding the counts for quote, reply, retweet, and favorite
    obj['popularity'] = index['quote_count'] + index['reply_count'] + \
        index['retweet_count'] + index['favorite_count']
    return obj

# Loop through each tweet in the data object
for index in data:
    # If the tweet is a retweet, extract the data from the 'retweeted_status' field
    if 'retweeted_status' in index.keys():
        obj = mongo_insertor(index['retweeted_status'], keys)
        try:
            collection.insert_one(obj)
        except Exception as e:
            print(e)
            pass

    # If the tweet is a quoted tweet, extract the data from the 'quoted_status' field
    if 'quoted_status' in index.keys():
        obj = mongo_insertor(index['quoted_status'], keys)
        try:
            collection.insert_one(obj)
        except Exception as e:
            print(e)
            pass

    # If the tweet is neither a retweet nor a quoted tweet, extract the data from the top-level fields
    obj = mongo_insertor(index, keys)
    try:
        collection.insert_one(obj)
    except Exception as e:
        print(e)
        pass


###############################################################################
# Implementing Cache
###############################################################################

# Creating a cache object with checkpoint file 'cache_checkpoint.pickle' and checkpoint interval of 2
twitter_cache = Cache(checkpoint_file='cache_checkpoint.pickle', checkpoint_interval=2)

###############################################################################

# Defining a SQL query to fetch user ID of a specific username from 'user_data' table
query_find = "select user_id from user_data where username ='NolteNC';"

# Executing the SQL query using 'cur' cursor object
cur.execute(query_find)

# Fetching the result of the SQL query using 'fetchall' method
result = cur.fetchall()

# Creating a dictionary with the fetched user ID
query_find = {"user_id": result[0][0]}

# Querying a MongoDB collection named 'collection' to get documents based on the user ID
results = collection.find(query_find)

# Converting the queried documents to Python objects using json_util
documents = [json_util.loads(json_util.dumps(doc)) for doc in results]

###############################################################################
# functions for search
###############################################################################

# Define a function named 'get_hashtag' that takes a 'hashtag' parameter
def get_hashtag(hashtag):
    
    """
    Queries a MongoDB collection and a MySQL database to retrieve relevant data based on the provided hashtag.

    Parameters:
    hashtag (str): The hashtag to search for.

    Returns:
    pandas.DataFrame: A DataFrame containing the results of the query. The DataFrame has the following columns:
                      'user_id', 'username', 'tweet_id', 'tweet_text', and 'popularity'.

                      'user_id': The user ID of the user who posted the tweet.
                      'username': The username of the user who posted the tweet.
                      'tweet_id': The ID of the tweet.
                      'tweet_text': The text of the tweet.
                      'popularity': The number of likes and retweets for the tweet.

                      If no documents are found for the hashtag, a message is printed and None is returned.
    """
    
    # Check if 'hashtag' parameter is not a string, then convert it to a string
    if type(hashtag) != str:
        hashtag = str(hashtag)

    # Create a target_key tuple to store the function name and hashtag parameter for caching
    target_key = (__name__, 'get_hashtag', hashtag)

    # Check if the target_key exists in cache, if yes, return the cached value
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    else:
        try:
            # Define a query using regex to find the hashtags in the MongoDB collection
            query = {'entities.hashtags.text': {'$regex': f'.{hashtag}.', '$options': 'i'}}

            # Create two empty dataframes to store the results from the MongoDB collection
            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

            # Define a list of keys to extract from each document in the MongoDB collection
            keys_to_extract = ["user_id", "_id", "text", "popularity"]

            # Query the MongoDB collection and store the results in the 'documents' list after converting them to Python objects
            results = collection.find(query)
            documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                            for doc in results]

            # Extract the required data from the documents and store it in the dataframes
            for i in range(len(documents)):
                df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                documents[i]['text'], documents[i]['popularity']]

            # Query the MySQL database to get the usernames for the user IDs and store the results in the 'df1' dataframe
            results = []
            for i in range(len(documents)):
                query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                cur.execute(query_find)
                result = cur.fetchall()
                results.append(result)

            for i in range(len(results)):
                if len(results[i]) > 0:
                    df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
                else:
                    continue
                
            # Set the 'user_id' column as the index for both dataframes
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            # Join the two dataframes and sort the values by popularity in descending order
            df3 = df1.join(df2, on='user_id', how='inner')
            df3.sort_values(by='popularity', ascending=False, inplace=True)

            # If no documents are found for the hashtag, print a message. Otherwise, cache the result and return it
            if len(documents) == 0:
                print(f"Hashtag {hashtag} not found")
            else:
                twitter_cache.set(target_key, df3)
                return df3

        # If any error occurs, print the error message
        except Exception as e:
            print(f"Error: {e}")


# Prompt the user to enter a hashtag and call the 'get_hashtag' function with the entered hashtag
hasht = input("enter hastag: ")
get_hashtag(hasht)


###############################################################################

# This function fetches tweets containing a given word and returns the result in a dataframe.
def get_word(word):
    
    """
    Fetch tweets containing a given word and return the result in a dataframe.

    Args:
        word (str): The word to search for in the tweets.

    Returns:
        pandas.DataFrame: A dataframe containing the user_id, username, tweet_text, and popularity of the tweets that match the search term.

    Raises:
        Exception: If an error occurs during the database query or dataframe creation.
    """

    # Check if the input word is a string, if not, convert it to a string.
    if type(word) != str:
        word = str(word)

    # Create a tuple to use as a key in the cache.
    target_key = (__name__, 'get_word', word)

    # Check if the result for the given word is present in the cache. If yes, return the result from the cache.
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)

    # If the result is not present in the cache, fetch it from the database and store it in the cache.
    else:
        try:
            # Prepare a MongoDB query to fetch tweets containing the given word.
            query = {'text': {'$regex': f'.*{word}.*', '$options': 'i'}}

            # Create two empty dataframes to store user data and tweet data.
            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', 'tweet_text', 'popularity'])

            # Define a list of keys to extract from the MongoDB documents.
            keys_to_extract = ["user_id", "text", "popularity"]

            # Fetch the tweets containing the given word from the database.
            results = collection.find(query)
            
            # Extract the required fields from the MongoDB documents and store them in a list of dictionaries.
            documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                            for doc in results]

            # Store the extracted tweet data in the first dataframe.
            for i in range(len(documents)):
                df2.loc[len(df2)] = [documents[i]['user_id'],
                                                documents[i]['text'], documents[i]['popularity']]
            
            # Fetch user data from the database for each user who posted the tweets containing the given word.
            results = []
            for i in range(len(documents)):
                query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                cur.execute(query_find)
                result = cur.fetchall()
                results.append(result)

            # Store the extracted user data in the second dataframe.
            for i in range(len(results)):
                if (len(results[i]) > 0):
                    df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
                else:
                    continue
            
            # Set the user_id column as the index for both dataframes.
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            # Join the two dataframes on the user_id column.
            df3 = df1.join(df2, on='user_id', how='inner')
            
            # Sort the resulting dataframe by popularity in descending order.
            df3.sort_values(by='popularity', ascending=False, inplace=True)

            # If the resulting dataframe is empty, print a message.
            if len(documents) == 0:
                print("Tweet(s) not found")
            else:
                # Store the resulting dataframe in the cache and return it.
                twitter_cache.set(target_key, df3)
                return df3

        # If an error occurs, print the error message.
        except Exception as e:
            print(f"Error: {e}")

# Get the input word from the user.
word = input("enter word: ")

# Call the get_word function with the input word.
get_word(word)


###############################################################################

# Define a function that takes a Twitter username as an argument and returns a dataframe with their tweet data
def get_username(username):
    
    """
    This function takes a Twitter username as an argument and returns a dataframe with their tweet data.
    The function first checks if the data for the given user is already present in the cache. If it is, the data is
    returned from the cache. Otherwise, the function retrieves the tweet data for the given user from a database,
    extracts the relevant keys from the tweet documents, and stores the data in a dataframe. The resulting dataframe is
    sorted by popularity and cached for future use.

    Parameters:
    username (str): A string representing the Twitter username to retrieve tweet data for.

    Returns:
    pandas.DataFrame: A dataframe containing the tweet data for the given user, sorted by popularity.
    """

    # Convert the input username to a string, if it isn't already
    if type(username) != str:
        username = str(username)

    # Create a key to use for the cache lookup based on the function name and username
    target_key = (__name__, 'get_username', username)

    # If the data for this user is in the cache, return it
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    else:
        try:
            # Construct a SQL query to retrieve tweet data for the given user from a database
            query = f"SELECT user_id,username FROM user_data WHERE full_name LIKE \
                '%{username}%' OR username LIKE '%{username}%'"

            # Execute the query
            cur.execute(query)
            result_set = cur.fetchall()

            # Create an empty list to store the tweet data for each user
            documents = []

            # Create two empty dataframes to store the user data and tweet data separately
            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', 'tweet_text', 'popularity'])

            # Define a list of keys to extract from the tweet documents
            keys_to_extract = ["user_id", "text", "popularity"]

            # Loop over the results of the SQL query, retrieving the tweet data for each user
            for i in range(len(result_set)):
                # Add the user data to df1
                df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]
                # Construct a MongoDB query to retrieve the tweet data for this user
                query_find = {'user_id': Int64(result_set[i][0])}
                # Execute the query
                result_tweets = collection.find(query_find)
                # Extract the relevant keys from the tweet documents and add them to documents
                documents.append([json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                                  for doc in result_tweets])

            # Loop over the list of tweet data for each user, adding it to df2
            for j in range(len(documents)):
                df2.loc[len(df2)] = [documents[j][0]['user_id'],
                                     documents[j][0]['text'], documents[j][0]['popularity']]

            # Set the index of each dataframe to the user ID
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            # Join the two dataframes on the user ID and sort by popularity
            df3 = df1.join(df2, on='user_id', how='inner')
            df3.sort_values(by='popularity', ascending=False, inplace=True)

            # If no tweets were found for this user, print a message
            if len(documents) == 0:
                print("Tweet(s) not found")
            # Otherwise, add the data to the cache and return the dataframe
            else:
                twitter_cache.set(target_key, df3)
                return df3

        # If there's an error, print the error message
        except Exception as e:
            print(f"Error: {e}")

# Prompt the user to enter a username and call the function with the input
username = input("Enter username: ")
get_username(username)



###############################################################################

get_hashtag("prison")
twitter_cache.cache[('__main__', 'get_hashtag', 'prison')]

def get(self, key):
        if key in self.cache:
            return self.cache[key]
        else:
            return None


get_username("jack")
get_word("covid")

twitter_cache.cache
twitter_cache.cache.keys()

# This block of code measures the time taken to call the get_hashtag() function with and without caching
start_time = time.time()
get_hashtag('prison')
end_time = time.time()
without_cache = end_time - start_time

start_time = time.time()
get_hashtag('prison')
end_time = time.time()
with_cache = end_time - start_time

print(without_cache) # Prints the time taken to call get_hashtag() without caching
print(with_cache) # Prints the time taken to call get_hashtag() with caching
###############################################################################
# Implementing Indexes
###############################################################################

# Create an index on the 'username' column in the 'user_data' table
query_index = "CREATE INDEX username_idx ON user_data(username);"
cur.execute(query_index)

# Create an index on the 'user_id' field in a MongoDB collection named 'collection'
collection.create_index('user_id')

# Select the 'user_id' and 'username' fields from the 'user_data' table
query = f"SELECT user_id, username FROM user_data ORDER"
cur.execute(query)

# Fetch all the results from the executed query and store them in a variable named 'result_set'
result_set = cur.fetchall()


###############################################################################

# Define a function that retrieves the top 10 users based on followers count and total tweets
def get_top_10_users():
    
    """
    Retrieves the top 10 users based on their followers count and total number of tweets.

    Returns:
    pandas.DataFrame: A DataFrame containing the user_id and username of the top 10 users.
    """
    
    target_key = (__name__, 'get_top_10_users')

    # Check if the results for this function are cached and return them if they are
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    
    # If the results are not cached, execute a SQL query to retrieve the top 10 users
    else:
        try:
            query = "SELECT user_id, username from user_data \
                  ORDER BY followers_count DESC, total_tweets DESC LIMIT 10"

            cur.execute(query)
            result_set = cur.fetchall()

            # Convert the result set to a Pandas DataFrame
            df1 = pd.DataFrame(columns=['user_id', 'username'])

            for i in range(len(result_set)):
                df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]

            # Add the results to the cache
            twitter_cache.set(target_key, df1)
            return df1

        except Exception as e:
            print(f"Error: {e}")


# Call the function to retrieve the top 10 users
get_top_10_users()

# Retrieve the top 10 tweets from a MongoDB collection and print the text and popularity of each tweet
results = collection.find().sort("popularity", -1).limit(10)
for result in results:
    print(result['text'], result['popularity'])

# Create two empty Pandas DataFrames
df1 = pd.DataFrame(columns=['user_id', 'username'])
df2 = pd.DataFrame(columns=['user_id', 'tweet_id', 'tweet_text', 'popularity'])

# Define the keys to extract from each MongoDB document
keys_to_extract = ["_id", "user_id", "text", "popularity"]

# Retrieve the top 10 tweets from a MongoDB collection and add them to df2
results = collection.find().sort("popularity", -1).limit(10)
documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                for doc in results]
for i in range(len(documents)):
    df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                    documents[i]['text'], documents[i]['popularity']]
            
# Retrieve the usernames for each of the users who posted the top 10 tweets and add them to df1
results = []
for i in range(len(documents)):
    query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
    cur.execute(query_find)
    result = cur.fetchall()
    results.append(result)

for i in range(len(results)):
    if (len(results[i]) > 0):
        df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
    else:
        continue
            
# Join df1 and df2 on the 'user_id' column and print the resulting DataFrame
df1.set_index('user_id', inplace=True)
df2.set_index('user_id', inplace=True)

df3 = df1.join(df2, on='user_id', how='inner')
print(df3)

# df3.sort_values(by='popularity', ascending=False, inplace=True)


# define a function to get the top 10 tweets by popularity
def get_top_10_tweets():
    
    """
    Returns a dataframe of the top 10 tweets by popularity, including the tweet text, popularity, user ID, and username.

    Returns:
        pandas.DataFrame: A dataframe of the top 10 tweets by popularity, including the tweet text, popularity, user ID, and username.
    """
    
    # define a unique key for this function to use as a cache key
    target_key = (__name__, 'get_top_10_tweets')

    # check if the result is already cached
    if target_key in twitter_cache.cache.keys():
        # return cached result
        return twitter_cache.get(target_key)
    
    else:
        try:
            # create two empty dataframes for user data and tweet data
            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', 'tweet_id', 'tweet_text', 'popularity'])

            # specify the fields to extract from the MongoDB collection and sort by popularity
            keys_to_extract = ["_id", "user_id", "text", "popularity"]
            results = collection.find().sort("popularity", -1).limit(10)

            # extract the specified fields from the MongoDB collection and store them in a list of dictionaries
            documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                            for doc in results]
            
            # iterate over the list of dictionaries and populate the tweet dataframe
            for i in range(len(documents)):
                df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                documents[i]['text'], documents[i]['popularity']]
            
            # query the user_data table in the database to get the usernames for each user_id in the tweet dataframe
            results = []
            for i in range(len(documents)):
                query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                cur.execute(query_find)
                result = cur.fetchall()
                results.append(result)

            # iterate over the query results and populate the user dataframe
            for i in range(len(results)):
                if (len(results[i]) > 0):
                    df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
                else:
                    continue
                        
            # set the user_id column as the index for both dataframes
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            # join the two dataframes on the user_id column to create a final dataframe
            df3 = df1.join(df2, on='user_id', how='inner')
            
            # cache the result for future use
            twitter_cache.set(target_key, df3)
            
            # return the final dataframe
            return df3
        
        except Exception as e:
            # print an error message if an exception is raised
            print(f"Error: {e}")

# call the function to get the top 10 tweets
get_top_10_tweets()

# measure the time taken to call the get_username function without caching
start_time = time.time()
get_username("jack")
end_time = time.time()
elapsed_time = end_time - start_time

# print the elapsed time
print(f"The time taken before storing the result in cache is {elapsed_time}.")

# measure the time taken to call the get_username function with caching
start_time = time.time()
get_username("jack")
end_time = time.time()
elapsed_time = end_time - start_time

# print the elapsed time
print(f"The time taken after storing the result in cache is {elapsed_time}.")