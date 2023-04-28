import pandas as pd
import MySQLdb
from pymongo import MongoClient
from bson import json_util, Int64
from implementing_cache import Cache


class SearchFunction:    
    def __init__(self):
        self.tweet_cache = Cache(checkpoint_file='cache_checkpoint.pickle', checkpoint_interval=2)
        self.db = MySQLdb.connect(host="localhost",    # your host, usually localhost
                     user="root",         # your username
                     passwd="root",  # your password
                     db="twitter_db")
        self.conn = MongoClient()
    
    def get_sql_connect(self):
        self.cur = self.db.cursor()
        return self.cur
    
    def get_mongo_connect(self):
        db = self.conn.twitter_db
        self.collection = db.tweets_data
        return self.collection
    


    def get_hashtag(self, hashtag):
        if type(hashtag) != str:
            hashtag = str(hashtag)

        target_key = (__name__, 'get_hashtag', hashtag)

        # if target_key in cache return from cache
        if target_key in self.tweet_cache.cache.keys():
            return self.tweet_cache.get(target_key)
        else:
            try:
                query = {'entities.hashtags.text': {'$regex': f'.{hashtag}.',
                                                    '$options': 'i'}}
                

                df1 = pd.DataFrame(columns=['user_id', 'username'])
                df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

                keys_to_extract = ["user_id", "_id", "text", "popularity"]




                results = self.get_mongo_connect().find(query)
                documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                                for doc in results]
                

                for i in range(len(documents)):
                    df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                    documents[i]['text'], documents[i]['popularity']]


                results = []
                for i in range(len(documents)):
                    query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                    self.get_sql_connect().execute(query_find)
                    result = self.get_sql_connect().cur.fetchall()
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
                    self.tweet_cache.set(target_key, df3)
                    return df3

            except Exception as e:
                print(f"Error: {e}")






    def get_keyword(self, keyword):
        if type(keyword) != str:
            keyword = str(keyword)

        target_key = (__name__, 'get_keyword', keyword)

        if target_key in self.tweet_cache.cache.keys():
            return self.tweet_cache.get(target_key)
        else:
            try:
                query = {'text': {'$regex': f'.*{keyword}.*', '$options': 'i'}}

                df1 = pd.DataFrame(columns=['user_id', 'username'])
                df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

                keys_to_extract = ["user_id", "_id", "text", "popularity"]
                # documents = []
                results = self.get_mongo_connect().find(query)
                documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                                for doc in results]
                for i in range(len(documents)):
                    df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                    documents[i]['text'], documents[i]['popularity']]
                
                results = []
                for i in range(len(documents)):
                    query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                    self.get_sql_connect().execute(query_find)
                    result = self.get_sql_connect().fetchall()
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
                    self.tweet_cache.set(target_key, df3)
                    return df3

            except Exception as e:
                print(f"Error: {e}")





    def get_username(self, username):
        if type(username) != str:
            username = str(username)

        target_key = (__name__, 'get_username', username)

        if target_key in self.tweet_cache.cache.keys():
            return self.tweet_cache.get(target_key)
        else:
            try:
                query = f"SELECT user_id,username FROM user_data WHERE full_name LIKE \
                    '%{username}%' OR username LIKE '%{username}%'"

                self.get_sql_connect().execute(query)
                result_set = self.get_sql_connect().fetchall()

                documents = []

                df1 = pd.DataFrame(columns=['user_id', 'username'])
                df2 = pd.DataFrame(columns=['user_id', "tweet_id", 'tweet_text', 'popularity'])

                keys_to_extract = ["user_id", "_id", "text", "popularity"]

                for i in range(len(result_set)):
                    df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]
                    query_find = {'user_id': Int64(result_set[i][0])}
                    result_tweets = self.get_mongo_connect().find(query_find)
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
                    self.tweet_cache.set(target_key, df3)
                    return df3

            except Exception as e:
                print(f"Error: {e}")

    def get_top_10_users(self):
        target_key = (__name__, 'get_top_10_users')

        if target_key in self.tweet_cache.cache.keys():
            return self.tweet_cache.get(target_key)
        
        else:
            try:
                query = "SELECT user_id, username from user_data \
                    ORDER BY followers_count DESC, total_tweets DESC LIMIT 10"

                self.get_sql_connect().execute(query)
                result_set = self.get_sql_connect.fetchall()

                df1 = pd.DataFrame(columns=['user_id', 'username'])

                for i in range(len(result_set)):
                    df1.loc[len(df1)] = [result_set[i][0], result_set[i][1]]

                # add if not in cache
                self.tweet_cache.set(target_key, df1)
                return df1

            except Exception as e:
                print(f"Error: {e}")

    def get_top_10_tweets(self):
        target_key = (__name__, 'get_top_10_tweets')

        if target_key in self.tweet_cache.cache.keys():
            return self.tweet_cache.get(target_key)
        
        else:
            try:
                df1 = pd.DataFrame(columns=['user_id', 'username'])
                df2 = pd.DataFrame(columns=['user_id', 'tweet_id', 'tweet_text', 'popularity'])

                keys_to_extract = ["_id", "user_id", "text", "popularity"]
                results = self.get_mongo_connect().find().sort("popularity", -1).limit(10)
                documents = [json_util.loads(json_util.dumps({key: doc.get(key) for key in keys_to_extract}))
                                for doc in results]
                
                for i in range(len(documents)):
                    df2.loc[len(df2)] = [documents[i]['user_id'], documents[i]['_id'],
                                                    documents[i]['text'], documents[i]['popularity']]
                
                results = []
                for i in range(len(documents)):
                    query_find = f"select user_id,username from user_data where user_id = {documents[i]['user_id']};"
                    self.get_sql_connect().execute(query_find)
                    result = self.get_sql_connect().fetchall()
                    results.append(result)

                for i in range(len(results)):
                    if (len(results[i]) > 0):
                        df1.loc[len(df1)] = [results[i][0][0], results[i][0][1]]
                    else:
                        continue
                            
                df1.set_index('user_id', inplace=True)
                df2.set_index('user_id', inplace=True)

                df3 = df1.join(df2, on='user_id', how='inner')
                self.tweet_cache.set(target_key, df3)
                return self.df3
            
            except Exception as e:
                print(f"Error: {e}")


