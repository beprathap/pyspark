# Databricks notebook source
# dbutils.fs.mkdirs("dbfs:/FileStore/data/youtube-dataset")
# dbfs:/FileStore/data/youtube-dataset/channel_stats_20250102_222959.json

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

spark = SparkSession.builder.appName('Channel Data').getOrCreate()

# Read the JSON file
df = spark.read.option("multiline", "true").json("dbfs:/FileStore/data/youtube-dataset/channel_stats_20250102_222959.json")

df = df.select(
    "id",
    "snippet.title",
    "snippet.description",
    "snippet.publishedAt",
    "snippet.country",
    "statistics.subscriberCount",
    "statistics.viewCount",
    "statistics.videoCount"
)

from delta import *

df.write.format("delta").mode("overwrite").saveAsTable("channel_data")


# COMMAND ----------

df = spark.read.format("delta").table("channel_data")
