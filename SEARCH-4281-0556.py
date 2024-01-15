# Huh, Jeongyeon (s2425181)
# Klunder, Lars (s3057356)

# real execution time: 0m 51.575s

from pyspark import SparkContext 
import json
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

spark = SparkSession.builder.getOrCreate()

PATH = "/data/doina/Twitter-Archive.org/2017-01/01/12/*.json.bz2"  # 1 hour of tweets

KEYWORDS = ["pubg", "csgo", "nigaraka", "rocket league"]

# Read JSON files
tweets = spark.read.json(PATH).select(col("text")).filter(col("text").isNotNull())

# Split the text into words and convert to lowercase
words = tweets.withColumn("word", explode(split(lower(col("text")), "\\s+")))

# Filter out the keywords and count occurrences
keyword_counts = words.filter(col("word").isin(KEYWORDS)) \
                      .groupBy("word") \
                      .count()

# Show the result
keyword_counts.show()

