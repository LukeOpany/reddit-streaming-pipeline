from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, length, when, lower, regexp_replace, trim, to_timestamp, hour
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

transformed_df = (
    parsed_df
    .withColumn("text_length", length(col("text")))
    .withColumn(
        "text_size",
        when(col("text_length") < 45, "short")
        .when(col("text_length") <= 55, "medium")
        .otherwise("long")
    )
    .withColumn(
        "clean_text",
        trim(
            regexp_replace(
                lower(col("text")),
                r"[^a-z0-9\s]",
                ""
            )
        )
    )
    .withColumn(
        "mentions_kafka",
        when(col("clean_text").contains("kafka"), 1).otherwise(0)
    )
    .withColumn(
        "mentions_spark",
        when(col("clean_text").contains("spark"), 1).otherwise(0)
    )
    .withColumn(
        "mentions_python",
        when(col("clean_text").contains("python"), 1).otherwise(0)
    )
    .withColumn(
        "event_timestamp",
        to_timestamp(col("created_at"))
    )
    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )
)


def write_to_postgres(batch_df, batch_id):
    row_count = batch_df.count()

    print(f"Batch {batch_id}: {row_count} rows")

    if row_count > 0:
        (
            batch_df.write
            .format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://localhost:5433/reddit_pipeline"
            )
            .option("dbtable", "reddit_events")
            .option("user", "reddit")
            .option("password", "reddit_password")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )


query = (
    transformed_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .start()
)


query.awaitTermination()