show databases;
create database twitter_db;

use twitter_db;
show tables;

alter table user_data rename column loaction to location;
select * from retweets;
select * from quoted_tweets;
select * from user_data where user_id = 46769281;
select * from retweets r join user_data u on r.user_id = u.user_id where u.user_id = 14062180;
