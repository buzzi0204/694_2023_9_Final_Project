show databases;
create database trial;
use trial;

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
	user_id BIGINT PRIMARY KEY NOT NULL,
    user_id_str VARCHAR(255),
    username VARCHAR(255),
    full_name VARCHAR(255),
    verfied BOOLEAN,
    bio TEXT,
    loaction VARCHAR(255),
    url TEXT(255),
    created_at TIMESTAMP,
    followers_count INTEGER,
    following_count INTEGER,
    likes_count INTEGER,
    total_tweets INTEGER,
    lang VARCHAR(10)
);



CREATE TABLE IF NOT EXISTS retweets(
	retweet_id BIGINT PRIMARY KEY,
    tweet_id VARCHAR(255),
    user_id BIGINT,
    created_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)
);



CREATE TABLE IF NOT EXISTS quoted_tweets(
	quoted_tweets_id BIGINT PRIMARY KEY,
    tweet_id VARCHAR(255),
    user_id BIGINT,
    created_at TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_data(user_id)
);
    

    
-- select * from user;

-- truncate table user;

-- drop database trial;


-- select * from user_data;
-- select count(*) from user_data;

-- describe user_data;

show tables;

drop table user_data;
select * from user_data;
drop table retweets;
truncate table user_data;
truncate table retweets;
select count(*) from user_data;
describe retweets;
describe quoted_tweets;
select * from retweets;
select * from quoted_tweets;
truncate table quoted_tweets;
select count(*) from quoted_tweets;

show tables;

select * from user_data;
drop table user_data;

drop table retweets;
drop table quoted_tweets;


create table trial_id (id BIGINT(20));
alter table trial_id modify id BIGINT;
show tables;

describe trial_id;
insert into trial_id values(1249403767180668930);
select * from trial_id;
drop table trial_id;

describe quoted_tweets;
select * from user_data where user_id = 46769281;

select * from retweets r join user_data u on r.user_id = u.user_id where u.user_id = 1242817830946508801;
select q.quoted_tweets_id,q.tweet_id,u.user_id,u.username from quoted_tweets q join user_data u on q.user_id = u.user_id where u.user_id = 1242817830946508801;