show databases;
use trial;
-- create database trial;

-- create table test(name varchar(20));

-- insert into test values ("Atharva");

-- select * from test;

-- select * from trial;

-- alter table trial modify column created_at varchar(1000);

-- show tables;

-- rename table user to user_data;

-- drop table user_data;


-- CREATE TABLE tweet (created_at VARCHAR(100),
-- 	id BIGINT(20) NOT NULL,id_str VARCHAR(100),source VARCHAR(50),truncated BOOLEAN);

-- select * from tweet;
-- TRUNCATE TABLE tweet;

CREATE TABLE IF NOT EXISTS user_data(
	user_id INTEGER PRIMARY KEY NOT NULL,
    user_id_str VARCHAR(255),
    username VARCHAR(255),
    full_name VARCHAR(255),
    verfied BOOLEAN,
    bio TEXT,
    loaction VARCHAR(255),
    url VARCHAR(255),
    created_at TIMESTAMP,
    followers_count INTEGER,
    following_count INTEGER,
    likes_count INTEGER,
    total_tweets INTEGER,
    lang VARCHAR(10)
);



CREATE TABLE IF NOT EXISTS retweets(
	retweet_id INTEGER PRIMARY KEY,
    tweet_id VARCHAR(255),
    user_id INTEGER,
    created_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)
);



CREATE TABLE IF NOT EXISTS qouted_tweets(
	quoted_tweets_id INTEGER PRIMARY KEY,
    tweet_id VARCHAR(255),
    user_id INTEGER,
    created_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)
);












CREATE TABLE user (id BIGINT(20),
	id_str VARCHAR(100),
    name VARCHAR(50),
    username VARCHAR(50),
    location VARCHAR(50),
    url VARCHAR(50),
    description TEXT,
    translator_type VARCHAR(50),
    protected BOOLEAN,verified BOOLEAN,
    followers_count BIGINT(50),
    friends_count BIGINT(50),
    listed_count BIGINT(50),
    favourites_count BIGINT(50),
    no_tweets BIGINT(50),
    created_at VARCHAR(100)
    );
    

    
select * from user;

truncate table user;

drop database trial;


select * from user_data;
select count(*) from user_data;

describe user_data;

