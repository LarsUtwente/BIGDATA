#filtering code CSV

from pyspark import SparkContext 
import csv
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, udf
from pyspark.sql.types import StringType
from datetime import datetime


# Function to convert Unix timestamp to human-readable format
def convert_unix_to_datetime(unix_timestamp):
    return datetime.utcfromtimestamp(unix_timestamp).strftime('%Y-%m-%d')


spark = SparkSession.builder.getOrCreate()

PATH = "../s2465795/project_data/steam_reviews.csv" 
GAMES = ["PLAYERUNKNOWN'S BATTLEGROUNDS", 'Grand Theft Auto V', "Tom Clancy's Rainbow Six Siege", 'Terraria', "Garry's Mod", 'Rust', 'Rocket League', 'PAYDAY 2', 'Among Us', 'The Witcher 3: Wild Hunt', 'Dead by Daylight', 'ARK: Survival Evolved', 'Euro Truck Simulator 2', 'Stardew Valley', 'The Elder Scrolls V: Skyrim', 'Wallpaper Engine', 'Monster Hunter: World', 'Hollow Knight', 'The Forest', "Don't Starve Together", 'DARK SOULS™ III', 'Portal 2', 'Fallout 4', "Phasmophobia", "Dying Light", "Arma 3", "No Man's Sky", "Tomb Raider", "Sid Meier's Civilization V"]

steam_reviews = spark.read.csv(PATH, header=True)

convert_unix_udf = udf(convert_unix_to_datetime, StringType())

steam_reviews = steam_reviews.withColumn('time_created', convert_unix_udf(col('timestamp_created').cast('bigint')))
steam_reviews = steam_reviews.withColumn('time_updated', convert_unix_udf(col('timestamp_updated').cast('bigint')))

steam_reviews = steam_reviews.select('app_name', 'timestamp_created', 'time_created', 'timestamp_updated', 'time_updated', 'recommended')

steam_reviews.limit(3).show(truncate=False)

counted_reviews = steam_reviews.filter(col("app_name") \
                .isin(GAMES)).groupBy("app_name").count()

counted_reviews.show()


