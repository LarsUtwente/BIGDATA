from pyspark import SparkContext
import csv
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, udf, sum, when, count, year, dayofmonth
from pyspark.sql.types import StringType
from datetime import datetime


# Function to convert Unix timestamp to human-readable format
def convert_unix_to_datetime(unix_timestamp):
    if unix_timestamp is not None:
        return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d')
    else:
        return None
    
spark = SparkSession.builder.getOrCreate()

PATH = "../s2465795/project_data/steam_reviews.csv" 
GAMES = ["PLAYERUNKNOWN'S BATTLEGROUNDS", "Grand Theft Auto V", "Terraria", "DARK SOULS™ III", "Rocket League"]

steam_reviews = spark.read.csv(PATH, header=True)

convert_unix_udf = udf(convert_unix_to_datetime, StringType())

steam_reviews = steam_reviews.withColumn('time_created', convert_unix_udf(col('timestamp_created').cast('bigint')))

steam_reviews_2020 = steam_reviews.filter(year(col('time_created')) == 2020)

#steam_reviews_2020 = steam_reviews_2020.withColumn('day', dayofmonth(col('time_created')))

steam_reviews_2020 = steam_reviews_2020.select('app_name', 'review_id', 'time_created', 'recommended')

counted_reviews_2020 = steam_reviews_2020.filter(col("app_name").isin(GAMES)).groupBy("app_name", "time_created").agg(count("*").alias("count"))

counted_reviews_2020.show()

# Show the result
output_path = "steam_reviews"
counted_reviews_2020.repartition(1).write.csv(output_path, mode="overwrite", header=True)

# Group by 'app_name' and 'time_created', and calculate the sum of recommended and not recommended, and the total count
# counted_reviews_combi = steam_reviews_2021.filter(col("app_name").isin(GAMES)) \
#     .groupBy("app_name", "time_created") \
#     .agg(sum(when(col("recommended") == 'True', 1).otherwise(0)).alias('recommended_count'),
#          sum(when(col("recommended") == 'False', 1).otherwise(0)).alias('not_recommended_count'),
#          count('*').alias('total_count')) \
#     .select("app_name", "time_created", "recommended_count", "not_recommended_count", "total_count")

# counted_reviews_combi.show()
