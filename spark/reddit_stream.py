import psycopg

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    hour,
    length,
    lower,
    regexp_replace,
    to_timestamp,
    trim,
    when,
    window,
)
from pyspark.sql.types import StructField, StringType, StructType


# --------------------------------------------------
# 1. START SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RedditStreamingPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. DEFINE THE STRUCTURE OF AN INCOMING EVENT
# --------------------------------------------------

schema = StructType([
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_at", StringType(), True),
])


# --------------------------------------------------
# 3. READ RAW EVENTS FROM KAFKA
# --------------------------------------------------

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "reddit_messages")
    .option("startingOffsets", "latest")
    .load()
)


# --------------------------------------------------
# 4. PARSE THE JSON EVENT
# --------------------------------------------------

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(
        from_json(
            col("json_value"),
            schema,
        ).alias("event")
    )
    .select("event.*")
)


# --------------------------------------------------
# 5. TRANSFORM / ENRICH EACH EVENT
# --------------------------------------------------

transformed_df = (
    parsed_df

    # How many characters are in the text?
    .withColumn(
        "text_length",
        length(col("text"))
    )

    # Categorize the text by size.
    .withColumn(
        "text_size",
        when(col("text_length") < 45, "short")
        .when(col("text_length") <= 55, "medium")
        .otherwise("long")
    )

    # Clean the text.
    .withColumn(
        "clean_text",
        trim(
            regexp_replace(
                lower(col("text")),
                r"[^a-z0-9\s]",
                "",
            )
        )
    )

    # Keyword features.
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

    # Convert the timestamp from text into a real timestamp.
    .withColumn(
        "event_timestamp",
        to_timestamp(col("created_at"))
    )

    # Extract the hour.
    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )
)


# --------------------------------------------------
# 6. CREATE THE 1-MINUTE STREAMING ANALYSIS
# --------------------------------------------------

windowed_df = (
    transformed_df
    .withWatermark(
        "event_timestamp",
        "2 minutes"
    )
    .groupBy(
        window(
            col("event_timestamp"),
            "1 minute",
        ),
        col("subreddit"),
    )
    .count()
)


# --------------------------------------------------
# 7. FLATTEN THE WINDOW RESULTS
# --------------------------------------------------

window_summary_df = (
    windowed_df
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("subreddit"),
        col("count").alias("event_count"),
    )
)


# --------------------------------------------------
# 8. WRITE INDIVIDUAL EVENTS TO POSTGRESQL
# --------------------------------------------------

def write_to_postgres(batch_df, batch_id):
    row_count = batch_df.count()

    print(f"Event batch {batch_id}: {row_count} rows")

    if row_count > 0:
        (
            batch_df.write
            .format("jdbc")
            .option(
                "url",
                "jdbc:postgresql://localhost:5433/reddit_pipeline",
            )
            .option("dbtable", "reddit_events")
            .option("user", "reddit")
            .option("password", "reddit_password")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )


# --------------------------------------------------
# 9. UPSERT WINDOW SUMMARIES INTO POSTGRESQL
# --------------------------------------------------

def write_window_summary_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()

    print(f"Window batch {batch_id}: {len(rows)} rows")

    if not rows:
        return

    with psycopg.connect(
        host="localhost",
        port=5433,
        dbname="reddit_pipeline",
        user="reddit",
        password="reddit_password",
    ) as connection:

        with connection.cursor() as cursor:

            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO event_window_summary (
                        window_start,
                        window_end,
                        subreddit,
                        event_count
                    )
                    VALUES (%s, %s, %s, %s)

                    ON CONFLICT (window_start, subreddit)

                    DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        event_count = EXCLUDED.event_count
                    """,
                    (
                        row["window_start"],
                        row["window_end"],
                        row["subreddit"],
                        row["event_count"],
                    ),
                )


# --------------------------------------------------
# 10. START THE INDIVIDUAL EVENT STREAM
# --------------------------------------------------

query = (
    transformed_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .start()
)


# --------------------------------------------------
# 11. START THE WINDOW SUMMARY STREAM
# --------------------------------------------------

window_query = (
    window_summary_df
    .writeStream
    .outputMode("update")
    .foreachBatch(write_window_summary_to_postgres)
    .start()
)


# --------------------------------------------------
# 12. KEEP BOTH STREAMS RUNNING
# --------------------------------------------------

spark.streams.awaitAnyTermination()