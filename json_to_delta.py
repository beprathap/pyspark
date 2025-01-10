from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from delta import *
import os

# Initialize SparkSession with Delta Lake support
spark = SparkSession.builder.appName("YouTube Data Pipeline").getOrCreate()

# Define S3 paths
# s3_raw_path = "s3://your-bucket-name/youtube-dataset/"
# s3_processed_path = "s3://your-bucket-name/youtube-processed/"
s3_raw_path = "dbfs:/FileStore/data/youtube-dataset/"
s3_processed_path = "dbfs:/FileStore/data/youtube-dataset/output"

# ---- Process Channel Data ----
channel_df = spark.read.option("multiline", "true").json(f"{s3_raw_path}channel_stats_*.json")
channel_df = channel_df.select(
    "id",
    "snippet.title",
    "snippet.description",
    "snippet.publishedAt",
    "snippet.country",
    "statistics.subscriberCount",
    "statistics.viewCount",
    "statistics.videoCount"
)
channel_df.write.format("delta").mode("overwrite").save(f"{s3_processed_path}/channel_data")

# ---- Process Video Data ----
video_df = spark.read.option("multiline", "true").json(f"{s3_raw_path}video_details_*.json")
video_df = video_df.select(
    "id",
    "snippet.channelId",
    "snippet.title",
    "snippet.description",
    "snippet.publishedAt",
    "contentDetails.duration",
    "snippet.categoryId",
    "snippet.liveBroadcastContent",
    "statistics.viewCount",
    "statistics.likeCount",
    "statistics.commentCount"
)
video_df.write.format("delta").mode("overwrite").save(f"{s3_processed_path}/videos_data")

# ---- Process Comment Data ----
comment_df = spark.read.option("multiline", "true").json(f"{s3_raw_path}video_comments_*.json")
comment_df = comment_df.select(
    "comment_id",
    "video_id",
    "author_name",
    "text",
    "like_count",
    "published_at"
)
comment_df.write.format("delta").mode("overwrite").save(f"{s3_processed_path}/comments_data")

# ---- Process Video Categories ----
categories_df = spark.read.option("multiline", "true").json(f"{s3_raw_path}video_categories_*.json")

# Extract dynamic video IDs from schema
video_ids = categories_df.columns
flattened_data = []
for video_id in video_ids:
    flattened_data.append(
        categories_df.select(
            lit(video_id).alias("video_id"),
            col(f"`{video_id}`.category_id").alias("category_id"),
            col(f"`{video_id}`.title").alias("title")
        )
    )

final_categories_df = flattened_data[0]
for data in flattened_data[1:]:
    final_categories_df = final_categories_df.union(data)

final_categories_df.write.format("delta").mode("overwrite").save(f"{s3_processed_path}/categories_data")

# Stop Spark Session
# spark.stop()