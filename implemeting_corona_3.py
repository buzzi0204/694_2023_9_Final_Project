# -*- coding: utf-8 -*-
"""

@author: Atharva, Urjit & Nikhil 
"""

import json
import MySQLdb
import pandas as pd
from pymongo import MongoClient
from implementing_cache import Cache
from bson import json_util, Int64
from time import time

###############################################################################
## Functions
###############################################################################
# making file into json format


# define function to create a JSON file from a given file path
# and write the data to a specified output file
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

    # open the input file and read its contents into a variable
    with open(filepath, "r") as f:
        json_data = f.read()

    # create a list to store each JSON object in the file
    json_objects = []

    # keep track of the number of braces encountered
    brace_count = 0

    # keep track of the index of the start of each JSON object
    start_index = None

    # loop through each character in the JSON data string
    for i, c in enumerate(json_data):
        # increment the brace count when an opening brace is encountered
        if c == "{":
            brace_count += 1

            # record the index of the opening brace if this is the first one encountered
            if brace_count == 1:
                start_index = i

        # decrement the brace count when a closing brace is encountered
        elif c == "}":
            brace_count -= 1

            # if this is the closing brace of the current JSON object,
            # append the JSON object to the list of JSON objects
            if brace_count == 0:
                json_objects.append(json_data[start_index : i + 1])

    # join the JSON objects into a single string separated by commas
    json_data = ",".join(json_objects)

    # parse the JSON data string into a list of dictionaries
    data = json.loads("[" + json_data + "]")

    # open the output file and write the JSON data to it
    with open(output_filename, "w") as f:
        json.dump(data, f)


# call the make_json function with the input file path and output file path as arguments
make_json("corona-out-3", "./json_files/corona-out-3.json")

# read the JSON data from the output file into a variable
with open("./json_files/corona-out-3.json", "r") as f:
    data = json.loads(f.read())

# connect to a MySQL database using the MySQLdb module
db = MySQLdb.connect(host="localhost", user="root", passwd="root", db="twitter_db")

# create a cursor object to interact with the database
cur = db.cursor()

###############################################################################
####### Creating tables for tweets, retweets, quoted_tweets
###############################################################################

# define SQL query to create a table called "user_data"
create_table_query = "CREATE TABLE IF NOT EXISTS user_data(\
	user_id BIGINT PRIMARY KEY NOT NULL,\
    user_id_str VARCHAR(255),\
    username VARCHAR(255),\
    full_name VARCHAR(255),\
    verfied BOOLEAN,\
    bio TEXT,\
    location VARCHAR(255),\
    url TEXT(255),\
    created_at TIMESTAMP,\
    followers_count INTEGER,\
    following_count INTEGER,\
    likes_count INTEGER,\
    total_tweets INTEGER,\
    lang VARCHAR(10),\
    INDEX(username)\
);"

# execute the SQL query to create the "user_data" table
cur.execute(create_table_query)


# define SQL query to create a table called "retweet_data"
create_table_query = "CREATE TABLE IF NOT EXISTS retweet_data(\
	retweet_id BIGINT PRIMARY KEY,\
    tweet_id VARCHAR(255),\
    user_id BIGINT,\
    created_at TIMESTAMP,\
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)\
);"

# execute the SQL query to create the "retweet_data" table
cur.execute(create_table_query)


# define SQL query to create a table called "quoted_tweet_data"
create_table_query = "CREATE TABLE IF NOT EXISTS quoted_tweet_data(\
	quoted_tweets_id BIGINT PRIMARY KEY,\
    tweet_id VARCHAR(255),\
    user_id BIGINT,\
    created_at TIMESTAMP,\
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)\
);"

# execute the SQL query to create the "quoted_tweet_data" table
cur.execute(create_table_query)

###############################################################################
############ Inserting data for tweets_data, retweet, quoted_tweet
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

# list of keys to be extracted from data dictionary
key_list = [
    "id",
    "id_str",
    "screen_name",
    "name",
    "verified",
    "description",
    "location",
    "url",
    "created_at",
    "followers_count",
    "friends_count",
    "favourites_count",
    "statuses_count",
    "lang",
]

# SQL query for inserting data into user_data table
query_insert = "INSERT INTO user_data VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
%s,%s,%s);"

# dictionaries to hold extracted values
val_dict = {}
rt_val_dict = {}
qt_val_dict = {}

# loop through the data and extract the necessary values for each user
for i in range(len(data)):
    val_list = []

    # loop through each key and extract the corresponding value
    for key in key_list:
        if key == "created_at":
            # convert the created_at field to a datetime object
            data[i]["user"][key] = pd.to_datetime(data[i]["user"][key])
        val_list.append(data[i]["user"][key])

    # store the extracted values as a tuple in the val_dict
    val_dict[i] = tuple(val_list)

    # execute the SQL query with the extracted values
    try:
        cur.execute(query_insert, val_list)
    except Exception as e:
        print(e)
        pass

    # if the tweet is a retweet, extract the necessary values from the retweeted_status field
    if "retweeted_status" in data[i]:
        rt_val_list = []
        for key in key_list:
            if key == "created_at":
                # convert the created_at field to a datetime object
                data[i]["retweeted_status"]["user"][key] = pd.to_datetime(
                    data[i]["retweeted_status"]["user"][key]
                )
            rt_val_list.append(data[i]["retweeted_status"]["user"][key])
    else:
        continue

    # store the extracted values as a tuple in the rt_val_dict
    rt_val_dict[i] = tuple(rt_val_list)

    # execute the SQL query with the extracted values
    try:
        cur.execute(query_insert, rt_val_list)
    except Exception as e:
        print(e)
        pass

    # if the tweet is a quote tweet, extract the necessary values from the quoted_status field
    if "quoted_status" in data[i]:
        qt_val_list = []
        for key in key_list:
            if key == "created_at":
                # convert the created_at field to a datetime object
                data[i]["quoted_status"]["user"][key] = pd.to_datetime(
                    data[i]["quoted_status"]["user"][key]
                )
            qt_val_list.append(data[i]["quoted_status"]["user"][key])
    else:
        continue

    # store the extracted values as a tuple in the qt_val_dict
    qt_val_dict[i] = tuple(qt_val_list)

    # execute the SQL query with the extracted values
    try:
        cur.execute(query_insert, qt_val_list)
    except Exception as e:
        print(e)
        pass

# commit the changes to the database
db.commit()

# print(len(val_dict))
# print(len(rt_val_dict))
# print(len(qt_val_dict))

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

# Define a SQL query to insert data into a table called retweet_data
query_insert = "INSERT INTO retweet_data VALUES(%s,%s,%s,%s);"

# Create an empty dictionary to store the values for each tweet
val_dict = {}

# Loop through each tweet in the 'data' list
for i in range(len(data)):
    # Check if the tweet is a retweet
    if "retweeted_status" in data[i]:
        # Create a list to store the values for the retweet
        val_list = []

        # Append the tweet ID, retweet ID, user ID, and creation date to the list
        val_list.append(data[i]["id"])
        val_list.append(data[i]["retweeted_status"]["id"])
        val_list.append(data[i]["user"]["id"])
        val_list.append(pd.to_datetime(data[i]["created_at"]))

        # Store the list of values for the retweet in the dictionary
        val_dict[i] = tuple(val_list)

        # Try to execute the SQL query with the list of values
        try:
            cur.execute(query_insert, val_list)

        # If there's an error executing the query, print the error message and continue to the next tweet
        except Exception as e:
            print(e)
            pass

    # If the tweet is not a retweet, continue to the next tweet
    else:
        continue

# Commit the changes to the database
db.commit()

# print(len(val_dict))

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

# Define a SQL query for inserting data into a table called "quoted_tweet_data"
query_insert = "INSERT INTO quoted_tweet_data VALUES(%s,%s,%s,%s)"

# Create an empty dictionary to store values for each row to be inserted
val_dict = {}

# Iterate over the list "data"
for i in range(len(data)):
    # Check if the current element in "data" has a "quoted_status" key
    if "quoted_status" in data[i]:
        # Create an empty list to store values for the current row to be inserted
        val_list = []

        # Append values to "val_list" based on keys in "data" and "quoted_status"
        val_list.append(data[i]["id"])  # tweet ID
        val_list.append(data[i]["quoted_status"]["id"])  # quoted tweet ID
        val_list.append(data[i]["user"]["id"])  # user ID
        val_list.append(
            pd.to_datetime(data[i]["quoted_status"]["created_at"])
        )  # creation time of quoted tweet

        # Store values for the current row as a tuple in "val_dict"
        val_dict[i] = tuple(val_list)

        # Try to execute the SQL query to insert values for the current row
        try:
            cur.execute(query_insert, val_list)

        # If an error occurs, print the error message and continue to the next row
        except Exception as e:
            print(e)
            pass

    # If the current element in "data" does not have a "quoted_status" key, skip it and move on to the next row
    else:
        continue

# Commit changes to the database
db.commit()

# print(len(val_dict))

###############################################################################
########## Inserting tweets in MongoDB
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

# Connect to the 'twitter_db' database and the 'tweets_data' collection
db = conn.twitter_db
collection = db.tweets_data

# Define the keys that will be used to extract data from the JSON object
keys = [
    "id",
    "id_str",
    "text",
    "created_at",
    "is_quote_status",
    "quote_count",
    "reply_count",
    "entities",
    "retweet_count",
    "favorite_count",
    "lang",
    "timestamp_ms",
    "geo",
]


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

    sources = ["iPhone", "Android", "WebApp", "Instagram"]

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

    obj = {"_id": Int64(index["id"]), "source": extract_source(index["source"])}

    for key in keys:
        try:
            obj[key] = index[key]
        except:
            pass

    if "extended_tweet" in index.keys():
        obj["text"] = index["extended_tweet"]["full_text"]

    obj["user_id"] = Int64(index["user"]["id"])

    obj["popularity"] = (
        index["quote_count"]
        + index["reply_count"]
        + index["retweet_count"]
        + index["favorite_count"]
    )
    return obj


# Loop through each item in the data list
for index in data:
    # Check if the current item has a "retweeted_status" key
    if "retweeted_status" in index.keys():
        # If it does, create a new object from the "retweeted_status" key using the "mongo_insertor" function
        obj = mongo_insertor(index["retweeted_status"], keys)
        # Attempt to insert the new object into the collection using the "insert_one" method
        try:
            collection.insert_one(obj)
        # If an exception is raised during insertion, print the error message and continue to the next item
        except Exception as e:
            print(e)
            pass

    # Check if the current item has a "quoted_status" key
    if "quoted_status" in index.keys():
        # If it does, create a new object from the "quoted_status" key using the "mongo_insertor" function
        obj = mongo_insertor(index["quoted_status"], keys)
        # Attempt to insert the new object into the collection using the "insert_one" method
        try:
            collection.insert_one(obj)
        # If an exception is raised during insertion, print the error message and continue to the next item
        except Exception as e:
            print(e)
            pass

    # Create a new object from the current item using the "mongo_insertor" function
    obj = mongo_insertor(index, keys)
    # Attempt to insert the new object into the collection using the "insert_one" method
    try:
        collection.insert_one(obj)
    # If an exception is raised during insertion, print the error message and continue to the next item
    except Exception as e:
        print(e)
        pass

# Create an index on the "user_id" field of the collection using the "create_index" method
collection.create_index("user_id")


###############################################################################
## Implementing search queries and adding caching as well
###############################################################################

# Creating a cache object with checkpoint file 'cache_checkpoint.pickle' and checkpoint interval of 2
twitter_cache = Cache(checkpoint_file="cache_checkpoint.pickle", checkpoint_interval=2)

###############################################################################
### Search by username
###############################################################################


# This function retrieves Twitter data for a given username from a database, and caches the results for future use.
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

    # If the given username is not already a string, convert it to one.
    if type(username) != str:
        username = str(username)

    # Define a key that uniquely identifies the function and username.
    target_key = (__name__, "get_username", username)

    # Check if the results for this username are already cached, and return them if so.
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)

    # If the results are not cached, query the database for relevant data and cache the results.
    else:
        try:
            # Define a SQL query to retrieve user data and tweets for the given username.
            query = f"SELECT user_id,username FROM user_data WHERE full_name LIKE \
                '%{username}%' OR username LIKE '%{username}%'"

            # Execute the SQL query and retrieve the result set.
            cur.execute(query)
            result_set = cur.fetchall()

            # Initialize empty lists to store documents and dataframes.
            documents = []
            df1 = pd.DataFrame(columns=["user_id", "username"])
            df2 = pd.DataFrame(
                columns=["user_id", "tweet_id", "tweet_text", "popularity"]
            )

            # Define a list of keys to extract from each document retrieved from the database.
            keys_to_extract = ["user_id", "_id", "text", "popularity"]

            # Loop over each result in the result set and retrieve tweets associated with that user.
            for i in range(len(result_set)):
                df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]
                query_find = {"user_id": Int64(result_set[i][0])}
                result_tweets = collection.find(query_find)
                # Convert each retrieved document to a JSON object and extract relevant keys.
                documents.append(
                    [
                        json_util.loads(
                            json_util.dumps(
                                {key: doc.get(key) for key in keys_to_extract}
                            )
                        )
                        for doc in result_tweets
                    ]
                )

            # Convert the retrieved documents to dataframes and join them together on user_id.
            for j in range(len(documents)):
                df2.loc[len(df2)] = [
                    documents[j][0]["user_id"],
                    documents[j][0]["_id"],
                    documents[j][0]["text"],
                    documents[j][0]["popularity"],
                ]
            df1.set_index("user_id", inplace=True)
            df2.set_index("user_id", inplace=True)
            df3 = df1.join(df2, on="user_id", how="inner")
            df3.sort_values(by="popularity", ascending=False, inplace=True)
            df3.drop_duplicates(subset=["tweet_id"], keep="first", inplace=True)

            # If no tweets were found for the given username, print an error message.
            if len(documents) == 0:
                print(f"No Tweet(s) with username or name {username} found")

            # If tweets were found, cache the results and return the dataframe.
            else:
                twitter_cache.set(target_key, df3)
                return df3

        # If an error occurs during execution, print an error message.
        except Exception as e:
            print(f"Error: {e}")


###############################################################################
### Search by hashtag
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

    # Checking if the hashtag is a string, if not converting it to a string
    if type(hashtag) != str:
        hashtag = str(hashtag)

    # Creating a tuple to use as a cache key for this hashtag search
    target_key = (__name__, "get_hashtag", hashtag)

    # Checking if the target_key exists in the cache, and returning it if it does
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    else:
        try:
            # Querying the collection for tweets containing the specified hashtag
            # and creating a dataframe to store the results
            query = {
                "entities.hashtags.text": {"$regex": f".{hashtag}.", "$options": "i"}
            }
            df1 = pd.DataFrame(columns=["user_id", "username"])
            df2 = pd.DataFrame(
                columns=["user_id", "tweet_id", "tweet_text", "popularity"]
            )
            keys_to_extract = ["user_id", "_id", "text", "popularity"]
            results = collection.find(query)
            # Extracting the required fields from the query results and storing them in documents list
            documents = [
                json_util.loads(
                    json_util.dumps({key: doc.get(key) for key in keys_to_extract})
                )
                for doc in results
            ]

            # Converting the documents list to a pandas dataframe to perform further operations
            # and storing it in df2 dataframe
            for i in range(len(documents)):
                df2.loc[len(df2)] = [
                    documents[i]["user_id"],
                    documents[i]["_id"],
                    documents[i]["text"],
                    documents[i]["popularity"],
                ]

            # Querying the user_data table to get the username associated with each user_id in df2
            # and storing the results in df1 dataframe
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

            # Setting the index of the dataframes to user_id for easy joining and sorting
            df1.set_index("user_id", inplace=True)
            df2.set_index("user_id", inplace=True)

            # Joining the two dataframes, sorting them by popularity and dropping any duplicate tweets
            df3 = df1.join(df2, on="user_id", how="inner")
            df3.sort_values(by="popularity", ascending=False, inplace=True)
            df3.drop_duplicates(subset=["tweet_id"], keep="first", inplace=True)

            # Adding the results to the cache and returning the dataframe
            if len(documents) == 0:
                print(f"Hashtag {hashtag} not found")
            else:
                twitter_cache.set(target_key, df3)
                return df3

        # Catching and printing any exceptions that occur during the execution of the code
        except Exception as e:
            print(f"Error: {e}")


###############################################################################
### Search by word
###############################################################################


# Create a function that returns the tweets with the keyword
def get_keyword(word):
    """
    Fetch tweets containing a given keyword and return the result in a dataframe.

    Args:
        word (str): The keyword to search for in the tweets.

    Returns:
        pandas.DataFrame: A dataframe containing the user_id, username, tweet_text, and popularity of the tweets that match the search term.

    Raises:
        Exception: If an error occurs during the database query or dataframe creation.
    """

    # Convert the word to string if it is not already a string
    if type(word) != str:
        word = str(word)

    # Define a target key to use for caching the results
    target_key = (__name__, "get_word", word)

    # Check if the target key exists in the cache
    if target_key in twitter_cache.cache.keys():
        # If it exists, return the cached results
        return twitter_cache.get(target_key)
    else:
        # If it doesn't exist, try to retrieve the results from the database
        try:
            # Define a query to retrieve tweets that contain the keyword
            query = {"text": {"$regex": f".*{word}.*", "$options": "i"}}

            # Define two empty dataframes to store the user and tweet information
            df1 = pd.DataFrame(columns=["user_id", "username"])
            df2 = pd.DataFrame(
                columns=["user_id", "tweet_id", "tweet_text", "popularity"]
            )

            # Define the keys to extract from the database
            keys_to_extract = ["user_id", "_id", "text", "popularity"]

            # Retrieve the documents that match the query from the database
            results = collection.find(query)
            documents = [
                json_util.loads(
                    json_util.dumps({key: doc.get(key) for key in keys_to_extract})
                )
                for doc in results
            ]

            # Iterate over the documents to extract the user and tweet information
            for i in range(len(documents)):
                df2.loc[len(df2)] = [
                    documents[i]["user_id"],
                    documents[i]["_id"],
                    documents[i]["text"],
                    documents[i]["popularity"],
                ]

            # Retrieve the usernames of the users who posted the tweets
            results = []
            for i in range(len(documents)):
                query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                cur.execute(query_find)
                result = cur.fetchall()
                results.append(result)

            # Iterate over the usernames to add them to the user dataframe
            for i in range(len(results)):
                if len(results[i]) > 0:
                    df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
                else:
                    continue

            # Set the index of the dataframes to the user_id column
            df1.set_index("user_id", inplace=True)
            df2.set_index("user_id", inplace=True)

            # Join the user and tweet dataframes on the user_id column
            df3 = df1.join(df2, on="user_id", how="inner")

            # Sort the combined dataframe by popularity in descending order
            df3.sort_values(by="popularity", ascending=False, inplace=True)

            # Remove duplicate tweet_ids, keeping only the first occurrence
            df3.drop_duplicates(subset=["tweet_id"], keep="first", inplace=True)

            # If there are no tweets with the keyword, print a message
            if len(documents) == 0:
                print(f"No Tweet(s) with word {word} found")
            else:
                # If there are tweets with the keyword, cache the results using the target key
                twitter_cache.set(target_key, df3)
                return df3

        # If there is an error, print the error message
        except Exception as e:
            print(f"Error: {e}")


###############################################################################
# Top 10 users
###############################################################################


# Define a function to get the top 10 users based on the number of followers and total tweets
def get_top_10_users():
    """
    Retrieves the top 10 users based on their followers count and total number of tweets.

    Returns:
    pandas.DataFrame: A DataFrame containing the user_id and username of the top 10 users.
    """

    # Create a tuple with the module name and function name
    target_key = (__name__, "get_top_10_users")

    # Check if the target_key exists in the cache
    if target_key in twitter_cache.cache.keys():
        # If it exists, return the value from the cache
        return twitter_cache.get(target_key)

    # If the target_key is not in the cache
    else:
        try:
            # Define a SQL query to fetch the user_id and username of the top 10 users
            query = "SELECT user_id, username from user_data \
                  ORDER BY followers_count DESC, total_tweets DESC LIMIT 10"

            # Execute the query using the database cursor
            cur.execute(query)

            # Fetch all the rows from the result set
            result_set = cur.fetchall()

            # Create an empty pandas DataFrame with two columns - user_id and username
            df1 = pd.DataFrame(columns=["user_id", "username"])

            # Iterate through the rows of the result set and append them to the DataFrame
            for i in range(len(result_set)):
                df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]

            # Add the result to the cache if it's not already in the cache
            twitter_cache.set(target_key, df1)

            # Return the result
            return df1

        except Exception as e:
            # Print the error message if an error occurs
            print(f"Error: {e}")


###############################################################################
# Top 10 tweets
###############################################################################


# Define a function to get the top 10 tweets based on popularity
def get_top_10_tweets():
    """
    Returns a dataframe of the top 10 tweets by popularity, including the tweet text, popularity, user ID, and username.

    Returns:
        pandas.DataFrame: A dataframe of the top 10 tweets by popularity, including the tweet text, popularity, user ID, and username.
    """

    # Create a tuple with the module name and function name
    target_key = (__name__, "get_top_10_tweets")

    # Check if the target_key exists in the cache
    if target_key in twitter_cache.cache.keys():
        # If it exists, return the value from the cache
        return twitter_cache.get(target_key)

    # If the target_key is not in the cache
    else:
        try:
            # Create two empty pandas DataFrames with appropriate columns
            df1 = pd.DataFrame(columns=["user_id", "username"])
            df2 = pd.DataFrame(
                columns=["user_id", "tweet_id", "tweet_text", "popularity"]
            )

            # Define a list of keys to extract from the MongoDB collection
            keys_to_extract = ["_id", "user_id", "text", "popularity"]

            # Fetch the top 10 tweets from the MongoDB collection sorted by popularity
            results = collection.find().sort("popularity", -1).limit(10)

            # Convert each MongoDB document to a dictionary with only the required keys
            documents = [
                json_util.loads(
                    json_util.dumps({key: doc.get(key) for key in keys_to_extract})
                )
                for doc in results
            ]

            # Iterate through the dictionaries and append the values to the DataFrame df2
            for i in range(len(documents)):
                df2.loc[len(df2)] = [
                    documents[i]["user_id"],
                    documents[i]["_id"],
                    documents[i]["text"],
                    documents[i]["popularity"],
                ]

            # Fetch the user details for each tweet and store them in a list
            results = []
            for i in range(len(documents)):
                query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                cur.execute(query_find)
                result = cur.fetchall()
                results.append(result)

            # Iterate through the user details and append them to the DataFrame df1
            for i in range(len(results)):
                if len(results[i]) > 0:
                    df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
                else:
                    continue

            # Set the user_id column as the index for both DataFrames
            df1.set_index("user_id", inplace=True)
            df2.set_index("user_id", inplace=True)

            # Join the two DataFrames on the user_id column and store the result in df3
            df3 = df1.join(df2, on="user_id", how="inner")

            # Add the result to the cache if it's not already in the cache
            twitter_cache.set(target_key, df3)

            # Return the result
            return df3

        except Exception as e:
            # Print the error message if an error occurs
            print(f"Error: {e}")


###############################################################################

# Calling the get_username function to get the username of the specified user and calculating the time taken
start_time = time()
get_username("jack")
end_time = time()
elapsed_time = end_time - start_time

# Printing the elapsed time
print(f"The time taken before storing the result in cache is {elapsed_time}.")

# Calling the get_username function again to get the username of the same user and calculating the time taken
start_time = time()
get_username("jack")
end_time = time()
elapsed_time = end_time - start_time

# Printing the elapsed time
print(f"The time taken after storing the result in cache is {elapsed_time}.")

# Calling the get_hashtag function to get the tweets containing the specified hashtag and calculating the time taken
start_time = time()
get_hashtag("prison")
end_time = time()
elapsed_time = end_time - start_time

# Printing the elapsed time
print(f"The time taken before storing the result in cache is {elapsed_time}.")

# Calling the get_hashtag function again to get the tweets containing the same hashtag and calculating the time taken
start_time = time()
get_hashtag("prison")
end_time = time()
elapsed_time = end_time - start_time

# Printing the elapsed time
print(f"The time taken after storing the result in cache is {elapsed_time}.")

# Calling the get_keyword function to get the tweets containing the specified keyword and calculating the time taken
start_time = time()
get_keyword("covid")
end_time = time()
elapsed_time = end_time - start_time

# Printing the elapsed time
print(f"The time taken before storing the result in cache is {elapsed_time}.")

# Calling the get_keyword function again to get the tweets containing the same keyword and calculating the time taken
start_time = time()
get_keyword("covid")
end_time = time()
elapsed_time = end_time - start_time

# Printing the elapsed time
print(f"The time taken after storing the result in cache is {elapsed_time}.")

# Calling the get_hashtag and get_username functions to store some results in cache
get_hashtag("corona")
get_username("john")
get_keyword("vaccine")

# Calling the get_hashtag, get_username, and get_keyword functions again to check if the cached results are retrieved
get_hashtag("covid")
get_username("atharva")
get_keyword("19")

# Calling the get_keyword and get_username functions to store some more results in cache
get_keyword("death")
get_username("gucci")

# Calling the get_keyword and get_username functions again to store some more results in cache
get_keyword("Corona go")
get_username("yashfoundation")

# Calling the get_top_10_tweets and get_top_10_users functions to retrieve the top 10 tweets and top 10 users
get_top_10_tweets()
get_top_10_users()

# Printing the number of keys in the cache and the keys
print(len(twitter_cache.cache.keys()))
print(twitter_cache.cache.keys())

# Printing the result of retrieving a key from the cache
print(twitter_cache.get(("__main__", "get_word", "covid")))

###############################################################################
