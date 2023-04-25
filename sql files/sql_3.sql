show databases;
create database twitter_db;

use twitter_db;
show tables;

show indexes from user_data;
select * from user_data;
alter table user_data rename column loaction to location;
select * from retweet_data;
select * from quoted_tweet_data;
select * from user_data where user_id = 46769281;
select * from retweets r join user_data u on r.user_id = u.user_id where u.user_id = 14062180;

drop table retweets;
drop table quoted_tweets;
drop table user_data;



