# DBMS-Project


### Checklist:
 
> MongoDB

<li> Extract extended tweet data in retweeted status object. </li>
<li> Extract extended tweet data outside retweeted status object. </li>
<li> Extract entity data for retweeted data. </li>
<li> Extract entity data for tweet data. </li>
<li> UID with its corresponding tweet </li>
<li> UID with its name </li>

<br>

> SQL

<li> User Data </li>
<li> Count of Fav, tweets -> tweet stats </li>

Version 1
> Relational Datastore (MySQL):

Table: users
    user_id (PK)
    user_name
    user_screen_name
    user_location
    user_description
    user_followers_count
    user_friends_count
    user_created_at

Table: tweets
    tweet_id (PK)
    tweet_created_at
    tweet_text
    tweet_lang
    user_id (FK)

Table: retweets
     retweet_id (PK)
    tweet_id (FK)
    user_id (FK)

Non-Relational Datastore (MongoDB):

Database: tweets_db

Collection: tweets
    tweet_id (PK)
    tweet_created_at
    tweet_text
    tweet_lang
    user_id
    retweet_count
    favorite_count

Version 2
> Relational store (MySQL):

Table: tweets
    tweet_id (primary key)
    user_id (foreign key to users table)
    text
    created_at
    retweet_count
    favorite_count
    
Table: users
    user_id (primary key)
    name
    screen_name
    location
    description
    followers_count
    friends_count
    statuses_count

Non-relational store (MongoDB):

Collection: tweets
    tweet_id (primary key)
    user_id
    text
    created_at
    retweet_count
    favorite_count
    hashtags (array)
    mentions (array)

Collection: retweets
    retweet_id (primary key)
    tweet_id (foreign key to tweets collection)
    user_id




#### Report Sathi Information (bhoukat)

<ol> 
<li> cleaned the data </li>
<li> Understood meta data </li>
<li> Chose data that has to be used strategically </li>
<li> Configured MongoDB and SQL </li>
<li> Drew ER Diagram and developed the base for the application </li>
<li> Shotlisted the data to be inclued using the ER Diagram </li>


