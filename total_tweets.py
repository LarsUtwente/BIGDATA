from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, udf
from pyspark.sql.types import StringType
from datetime import datetime

# Function to parse and format the created_at field
def format_date(date_str):
    return datetime.strptime(date_str, '%a %b %d %H:%M:%S +0000 %Y').strftime('%Y-%m-%d')

format_date_udf = udf(format_date, StringType())

spark = SparkSession.builder.getOrCreate()

PATH = "../s2465795/project_data/twitter/2020/01/0*/*/*.json.bz2"
tweets_data = spark.read.json(PATH)

game_keywords = spark.read.json("../s2425181/games_keywords.json")

game_keywords_dict = dict(game_keywords.rdd.collect())

# Define a UDF to assign game names based on keywords
def assign_game_name(tweet_text):
    if tweet_text is not None:
        for game, keywords_list in game_keywords_dict.items():
            if any(keyword in tweet_text for keyword in keywords_list):
                return game
    return None

assign_game_name_udf = udf(assign_game_name, StringType())

# Assuming 'tweets_data' is the DataFrame with 'text' column
tweets_data = tweets_data.withColumn("game_name", assign_game_name_udf(lower(col("text"))))

# Filter out rows where 'game_name' is not null
tweets_data_filtered = tweets_data.filter(col("game_name").isNotNull())

# Assuming 'created_at' is the column containing the date information
tweets_data_filtered = tweets_data_filtered.withColumn("date", format_date_udf(col("created_at")))

# Group by 'date' and 'game_name', and count the tweets
grouped_data = tweets_data_filtered.groupBy("date", "game_name").count()

# Show the resulting DataFrame
grouped_data.show()

# Show the result
output_path = "tweets"
grouped_data.repartition(1).write.csv(output_path, mode="overwrite", header=True)
