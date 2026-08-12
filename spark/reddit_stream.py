from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


spark = (
    SparkSession.builder
    .appName("RedditStreamingPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


schema = StructType([
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_at", StringType(), True),
])


raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "reddit_messages")
    .option("startingOffsets", "latest")
    .load()
)


parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            schema
        ).alias("event")
    )
    .select("event.*")
)


query = (
    parsed_df
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)


query.awaitTermination()