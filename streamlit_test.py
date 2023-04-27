import streamlit as st
from search_functions import get_hashtag, get_keyword, get_username, get_top_10_tweets, get_top_10_users

import pandas as pd
import MySQLdb
from pymongo import MongoClient
from bson import json_util, Int64
from implementing_cache import Cache

db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="root",
                     db="twitter_db")
cur = db.cursor()
# sql_conn = pymysql.connect(host='localhost', user='root', password='root', database='twitter_db')
# cursor = sql_conn.cursor()

try:
    conn = MongoClient()
    print("Connected successfully")
except:  
    print("Could not connect to MongoDB")
  
db = conn.twitter_db
collection = db.tweets_data
# mon_conn = pymongo.MongoClient()
# db = mon_conn.trial
# collection = db.tweets_data


twitter_cache = Cache(checkpoint_file='cache_checkpoint.pickle', 
                      checkpoint_interval=2)

@st.cache_resource
def get_hashtag(hashtag):
    if type(hashtag) != str:
        hashtag = str(hashtag)

    target_key = (__name__, 'get_hashtag', hashtag)

    # if target_key in cache return from cache
    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    else:
        try:
            query = {'entities.hashtags.text': {'$regex': f'.{hashtag}.',
                                                '$options': 'i'}}
            

            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

            keys_to_extract = ["user_id", "_id", "text", "popularity"]




            results = collection.find(query)
            documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                            for doc in results]
            

            for i in range(len(documents)):
                df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                documents[i]['text'], documents[i]['popularity']]


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
                
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            df3 = df1.join(df2, on='user_id', how='inner')
            df3.sort_values(by='popularity', ascending=False, inplace=True)
            df3.drop_duplicates(subset=['tweet_id'], keep='first', inplace=True)
            


            # if not add in cache

            if len(documents) == 0:
                print(f"Hashtag {hashtag} not found")
            else:
                twitter_cache.set(target_key, df3)
                return df3

        except Exception as e:
            print(f"Error: {e}")

print(twitter_cache.cache.keys())

def get_keyword(keyword):
    if type(keyword) != str:
        keyword = str(keyword)

    target_key = (__name__, 'get_keyword', keyword)

    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    else:
        try:
            query = {'text': {'$regex': f'.*{keyword}.*', '$options': 'i'}}

            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

            keys_to_extract = ["user_id", "_id", "text", "popularity"]
            # documents = []
            results = collection.find(query)
            documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                            for doc in results]
            for i in range(len(documents)):
                df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                documents[i]['text'], documents[i]['popularity']]
            
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
            
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            df3 = df1.join(df2, on='user_id', how='inner')
            df3.sort_values(by='popularity', ascending=False, inplace=True)
            df3.drop_duplicates(subset=['tweet_id'], keep='first', inplace=True)



        # add if not in cache
            if len(documents) == 0:
                print(f"No Tweet(s) with word {keyword} found")
            else:
                twitter_cache.set(target_key, df3)
                return df3

        except Exception as e:
            print(f"Error: {e}")

def get_username(username):
    if type(username) != str:
        username = str(username)

    target_key = (__name__, 'get_username', username)

    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    else:
        try:
            query = f"SELECT user_id,username FROM user_data WHERE full_name LIKE \
                '%{username}%' OR username LIKE '%{username}%'"

            cur.execute(query)
            result_set = cur.fetchall()

            documents = []

            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

            keys_to_extract = ["user_id", "_id", "text", "popularity"]

            for i in range(len(result_set)):
                df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]
                query_find = {'user_id': Int64(result_set[i][0])}
                result_tweets = collection.find(query_find)
                documents.append([json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                                  for doc in result_tweets])

            for j in range(len(documents)):
                df2.loc[len(df2)] = [documents[j][0]['user_id'], documents[j][0]['_id'],
                                     documents[j][0]['text'], documents[j][0]['popularity']]

            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            df3 = df1.join(df2, on='user_id', how='inner')
            df3.sort_values(by='popularity', ascending=False, inplace=True)
            df3.drop_duplicates(subset=['tweet_id'], keep='first', inplace=True)

            # add if not in cache
            if len(documents) == 0:
                print(f"No Tweet(s) with username or name {username} found")
            else:
                twitter_cache.set(target_key, df3)
                return df3

        except Exception as e:
            print(f"Error: {e}")

def get_top_10_users():
    target_key = (__name__, 'get_top_10_users')

    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    
    else:
        try:
            query = "SELECT user_id, username from user_data \
                  ORDER BY followers_count DESC, total_tweets DESC LIMIT 10"

            cur.execute(query)
            result_set = cur.fetchall()

            df1 = pd.DataFrame(columns=['user_id', 'username'])

            for i in range(len(result_set)):
                df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]

            # add if not in cache
            twitter_cache.set(target_key, df1)
            return df1

        except Exception as e:
            print(f"Error: {e}")

def get_top_10_tweets():
    target_key = (__name__, 'get_top_10_tweets')

    if target_key in twitter_cache.cache.keys():
        return twitter_cache.get(target_key)
    
    else:
        try:
            df1 = pd.DataFrame(columns=['user_id', 'username'])
            df2 = pd.DataFrame(columns=['user_id', 'tweet_id', 'tweet_text', 'popularity'])

            keys_to_extract = ["_id", "user_id", "text", "popularity"]
            results = collection.find().sort("popularity", -1).limit(10)
            documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                            for doc in results]
            
            for i in range(len(documents)):
                df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                documents[i]['text'], documents[i]['popularity']]
            
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
                        
            df1.set_index('user_id', inplace=True)
            df2.set_index('user_id', inplace=True)

            df3 = df1.join(df2, on='user_id', how='inner')
            twitter_cache.set(target_key, df3)
            return df3
        
        except Exception as e:
            print(f"Error: {e}")






option = st.sidebar.selectbox('Search Query',['Hashtag', 'Keyword', 'Username/Full name'])

if option == 'Hashtag':
    input_text = st.sidebar.text_input("Enter hashtag")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_hashtag(input_text)
            print(twitter_cache.cache.keys())
            # styled_data = data.style.set_table_styles(styles)
            st.table(data)

if option == 'Keyword':
    input_text = st.sidebar.text_input("Enter keyword")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_keyword(input_text)
            # styled_data = data.style.set_table_styles(styles)
            st.table(data)

if option == 'Username/Full name':
    input_text = st.sidebar.text_input("Enter username/full name")
    if st.sidebar.button("Submit"):
        with st.spinner('Getting data...'):
            data = get_username(input_text)
            # styled_data = data.style.set_table_styles(styles)
            st.table(data)

# print(get_top_10_tweets())

if st.sidebar.button("Top 10 Tweets"):
    data = get_top_10_tweets()
    print(data)
    # styled_data = data.style.set_table_styles(styles)
    st.table(data)

if st.sidebar.button("Top 10 Users"):
    data = get_top_10_users()
    # styled_data = data.style.set_table_styles(styles)
    st.table(data)

