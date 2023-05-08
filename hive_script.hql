
SET hive.exec.dynamic.partition.mode=nonstrict;
CREATE DATABASE IF NOT EXISTS Twitter_Project;
use Twitter_Project;

CREATE EXTERNAL TABLE IF NOT EXISTS twitter_landing_data (
    id STRING,
    text STRING,
    hashtags ARRAY<STRING>,
    hashtags_count INT,
    words ARRAY<STRING>,
    words_count INT,
    mentioned_clubs ARRAY<STRING>,
    mentioned_clubs_count INT,
    retweet_count DOUBLE,
    reply_count DOUBLE,
    like_count DOUBLE,
    quote_count DOUBLE,
    impression_count DOUBLE,
    user_id STRING,
    username STRING,
    user_description STRING,
    name STRING,
    user_verification STRING,
    user_creation_date TIMESTAMP,
    user_followers_count DOUBLE,
    user_following_count DOUBLE,
    user_tweet_count DOUBLE,
    created_at TIMESTAMP
)
PARTITIONED BY (year INT , month INT , day INT , hour INT )
STORED AS PARQUET
LOCATION '/home/itversity/twitter-landing-data_test';

MSCK REPAIR TABLE twitter_landing_data;


CREATE TABLE IF NOT EXISTS tweet_text (
    id STRING,
    text STRING,
    words ARRAY<STRING>,
    words_count INT,
    hashtags ARRAY<STRING>,
    hashtags_count INT,
    mentioned_clubs ARRAY<STRING>

) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/home/itversity/twitter-raw-data/tweet_text';

INSERT OVERWRITE TABLE tweet_text PARTITION(year, month, day, hour)
SELECT id, text,words,words_count,hashtags,hashtags_count,mentioned_clubs,year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_landing_data;

CREATE TABLE IF NOT EXISTS tweet_metrics (
    id STRING,
    retweet_count DOUBLE,
    reply_count DOUBLE,
    like_count DOUBLE,
    quote_count DOUBLE,
    impression_count DOUBLE
) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/home/itversity/twitter-raw-data/tweet_metrics';

INSERT OVERWRITE TABLE tweet_metrics PARTITION(year, month, day, hour)
SELECT id,retweet_count,reply_count,like_count, quote_count, impression_count,year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_landing_data;

CREATE TABLE IF NOT EXISTS user_info (
    user_id STRING,
    username STRING,
    user_description STRING,
    name STRING,
    user_verification STRING,
    user_creation_date TIMESTAMP
) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/home/itversity/twitter-raw-data/user_info';

INSERT OVERWRITE TABLE user_info PARTITION(year, month, day, hour)
SELECT user_id,username,user_description,name, user_verification, user_creation_date,year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_landing_data;

CREATE TABLE IF NOT EXISTS user_metrics (
    user_id STRING,
    user_followers_count DOUBLE,
    user_following_count DOUBLE,
    user_tweet_count DOUBLE
) PARTITIONED BY (year INT, month INT, day INT, hour INT)
STORED AS PARQUET
LOCATION '/home/itversity/twitter-raw-data/user_metrics';

INSERT OVERWRITE TABLE user_metrics PARTITION(year, month, day, hour)
SELECT user_id,user_followers_count,user_following_count,user_tweet_count ,year(created_at) as year, month(created_at) as month, day(created_at) as day, hour(created_at) as hour
FROM twitter_landing_data;





