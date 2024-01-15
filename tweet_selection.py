"""
DISTRIBUTED mode: Run me from a head node with:
    time spark-submit --conf spark.dynamicAllocation.maxExecutors=5 tweet_selection.py
             (increase maxExecutors or decrease the input data if you see problems at execution,
             but always try to stay under maxExecutors=10 !)

DASHBOARD at https://spark-nn.eemcs.utwente.nl:8088/cluster

Any files in output are saved in parts (one per partition! written out in parallel) in a folder in the 
user's HDFS home directory under /user/.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()

# Runtime with a constant maxExecutors=5:
#         for 1 hour of tweets: ~1m     (~60 MB compressed in 1-minute files of 1 MB), 
#         for 1 day  of tweets: ~2m     (~1.44 GB compressed in 1440 1-minute files),
#         for 2 days of tweets: ~3m     (~2.88 GB compressed in 2880 1-minute files)

PATH = "/data/doina/Twitter-Archive.org/2017-01/01/12/*.json.bz2"     # 1 hour of tweets
# PATH = "/data/doina/Twitter-Archive.org/2017-01/01/*/*.json.bz2"      # 1 day  of tweets
# PATH = "/data/doina/Twitter-Archive.org/2017-01/0[12]/*/*.json.bz2"   # 2 days of tweets

# a regexp, case-insensitive matching; this was a topic of interest on those dates (Jan 2017)
KEYWORDS = "(inauguration)|(whitehouse)|(washington)|(president)|(obama)|(trump)"

tweets = spark.read.json(PATH) \
    .select(col("text")) \
    .filter(col("text").isNotNull()) \
    .filter(col("text").rlike(KEYWORDS).alias("text")) \
    .write.mode("overwrite").text("tweet_selection")
