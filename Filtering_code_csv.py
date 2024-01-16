#filtering code CSV

from pyspark import SparkContext 
import csv
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

spark = SparkSession.builder.getOrCreate()

PATH = "csv file path"  # 1 hour of tweets
GAMES = ["steam", "csgo", "lol"]

steam_reviews = spark.read.option(delimiter=",", header=True).csv(PATH)
counted_reviews = steam_reviews.filter(col("app_name") \
                .isin(GAMES)).groupBy("app_name").count()

counted_reviews.show()