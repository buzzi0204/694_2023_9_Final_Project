show databases;

use trial;
create database trial;

create table test(name varchar(20));

insert into test values ("Atharva");

select * from test;

select * from trial;

alter table trial modify column created_at varchar(1000);

show tables;



CREATE TABLE tweet (created_at VARCHAR(100),
	id BIGINT(20) NOT NULL,id_str VARCHAR(100),source VARCHAR(50),truncated BOOLEAN);

select * from tweet;
TRUNCATE TABLE tweet;






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


