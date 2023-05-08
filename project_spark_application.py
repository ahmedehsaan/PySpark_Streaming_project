from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, split, explode
from pyspark.sql.functions import from_json, col,collect_list
from pyspark.sql.functions import regexp_extract_all
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,DoubleType,ArrayType
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
import re
from pyspark.sql.functions import udf
from pyspark.sql.functions import trim
from pyspark.sql.functions import size
spark = SparkSession.builder.appName("StructuredJsonStreaming").getOrCreate()

# Define the schema for your JSON data
schema = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("retweet_count", DoubleType()),
    StructField("reply_count", DoubleType()),
    StructField("like_count", DoubleType()),
    StructField("quote_count", DoubleType()),
    StructField("impression_count", DoubleType()),
    StructField("created_at", TimestampType()),
    StructField("user_id", StringType()),
    StructField("username", StringType()),
    StructField("user_description", StringType()),
    StructField("name", StringType()),
    StructField("user_verification", StringType()),
    StructField("user_creation_date", TimestampType()),
    StructField("user_followers_count", DoubleType()),
    StructField("user_following_count", DoubleType()),
    StructField("user_tweet_count", DoubleType()),
    StructField("year", IntegerType()),
    StructField("month", IntegerType()),
    StructField("day", IntegerType()),
    StructField("hour", IntegerType()),
    StructField("hashtags", ArrayType(StringType()))
])

# Read the data from the socket
df = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 7777) \
    .load() \
    .select(from_json(col("value"), schema).alias("data")) \
    .selectExpr(
        "data.id",
        "data.text",
        "data.retweet_count",
        "data.reply_count",
        "data.like_count",
        "data.quote_count",
        "data.impression_count",
        "data.created_at",
        "data.user_id",
        "data.username",
        "data.user_description",
        "data.name",
        "data.user_verification",
        "data.user_creation_date",
        "data.user_followers_count",
        "data.user_following_count",
        "data.user_tweet_count",
        "year(data.created_at) as year",
        "month(data.created_at) as month",
        "day(data.created_at)  as day", 
        "hour(data.created_at) as hour"
    ) 
    




def extract_hashtags(text):
    return re.findall(r'#\w+', text)

# Register the UDF
extract_hashtags_udf = udf(extract_hashtags, ArrayType(StringType()))

# Apply the UDF to the 'text' column and create a new column called 'hashtags'
df = df.withColumn('hashtags', extract_hashtags_udf('text'))


# Define the list of words you want to filter in the text column
words = ["Arsenal", "Aston Villa", "Brentford", "Brighton", "Burnley", "Chelsea", "Crystal Palace", "Everton", "Leeds United", "Leicester City", "Liverpool", "Manchester City", "Manchester United", "Newcastle United", "Norwich City", "Southampton", "Tottenham", "Watford", "West Ham United", "Wolverhampton"]

# Define a UDF to extract words from text
def extract_clubs(text):
    return [word for word in words if word.lower() in text.lower()]
# Function to remove URLs from text
def remove_urls(text):
    return re.sub(r'http\S+', '', text)


remove_urls_udf = udf(remove_urls, StringType())
df = df.withColumn('text', remove_urls_udf('text'))
df = df.withColumn("text", trim(df["text"]))
def extract_words(text):
    return re.findall(r'\b\w+\b', text)

# Register the UDF
extract_words_udf = udf(extract_words, ArrayType(StringType()))

# Apply the UDF to the 'text' column and create a new column called 'words'
df = df.withColumn('words', extract_words_udf('text'))

# Register the UDF
extract_clubs_udf = udf(extract_clubs, ArrayType(StringType()))

# Apply the UDF to the 'text' column and create a new column called 'filtered_words'
df = df.withColumn('mentioned_clubs', extract_clubs_udf('text'))



df = df \
    .withColumn('hashtags_count', size('hashtags')) \
    .withColumn('words_count', size('words')) \
    .withColumn('mentioned_clubs_count', size('mentioned_clubs')) \
    .select(
        "id",
        "text",
        "hashtags",
        "hashtags_count",
        "words",
        "words_count",
        "mentioned_clubs",
        "mentioned_clubs_count",
        "retweet_count",
        "reply_count",
        "like_count",
        "quote_count",
        "impression_count",
        "user_id",
        "username",
        "user_description",
        "name",
        "user_verification",
        "user_creation_date",
        "user_followers_count",
        "user_following_count",
        "user_tweet_count",
        "created_at",
        "year",
        "month",
        "day",
        "hour"
    )




# Write the streaming data to HDFS in parquet format
query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("checkpointLocation", "/home/itversity/check")\
    .option("path","/home/itversity/twitter-landing-data_test")\
    .partitionBy("year", "month", "day", "hour") \
    .start()


query.awaitTermination()