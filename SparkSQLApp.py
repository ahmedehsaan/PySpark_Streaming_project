from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum

spark = SparkSession.builder.appName("twitter-processed-data").getOrCreate()

# Load dimensions from Hive tables
tweet_text = spark.table("twitter_project.tweet_text")
tweet_metrics = spark.table("twitter_project.tweet_metrics")
user_info = spark.table("twitter_project.user_info")
user_metrics = spark.table("twitter_project.user_metrics")

# Join dimensions to create the fact table
fact_table = tweet_text.join(tweet_metrics, "id").join(user_info, "user_id").join(user_metrics, "user_id")

# Aggregate the fact table to create multiple aggregations
aggregated_table = fact_table.groupBy("year", "month", "day", "hour") \
    .agg(
        count("id").alias("tweet_count"),
        sum("like_count").alias("total_like_count"),
        sum("retweet_count").alias("total_retweet_count"),
        sum("reply_count").alias("total_reply_count"),
        sum("words_count").alias("tweets_words_count"),
        sum("hashtags_count").alias("tweets_hashtags_count")
    )

# Save the aggregated table to Hive as a processed table
aggregated_table.write.mode("overwrite").saveAsTable("twitter_project.twitter_fact_table_processed")

aggregated_table.write.mode("overwrite").parquet("/home/itversity/twitter-processed-data/fact_table_processed")