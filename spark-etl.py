import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from delta import *

def transform_data(data_source, output_uri):
    # Initialize SparkSession with Delta Lake support - update the delta version to 3.3.x to support with Apache Spark 3.5.3
    spark = SparkSession.builder \
        .appName("YouTube Data Pipeline") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # Process Channel Data
    channel_df = spark.read.option("multiline", "true").json(f"{data_source}/channel_stats_*.json")
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
    channel_df.write.format("delta").mode("overwrite").save(f"{output_uri}/channel_data")

    # Process Video Data
    video_df = spark.read.option("multiline", "true").json(f"{data_source}/video_details_*.json")
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
    video_df.write.format("delta").mode("overwrite").save(f"{output_uri}/videos_data")

    # Process Comment Data
    comment_df = spark.read.option("multiline", "true").json(f"{data_source}/video_comments_*.json")
    comment_df = comment_df.select(
        "comment_id",
        "video_id",
        "author_name",
        "text",
        "like_count",
        "published_at"
    )
    comment_df.write.format("delta").mode("overwrite").save(f"{output_uri}/comments_data")

    # Process Video Categories
    categories_df = spark.read.option("multiline", "true").json(f"{data_source}/video_categories_*.json")
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
    final_categories_df.write.format("delta").mode("overwrite").save(f"{output_uri}/categories_data")

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_source', required=True, help="Path to the raw JSON data")
    parser.add_argument('--output_uri', required=True, help="S3 path for Delta tables")
    args = parser.parse_args()
    transform_data(args.data_source, args.output_uri)