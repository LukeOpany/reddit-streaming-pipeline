import os

import psycopg
from dotenv import load_dotenv

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
# 1. LOAD ENVIRONMENT VARIABLES
# --------------------------------------------------

load_dotenv()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5433")


# --------------------------------------------------
# 2. START SPARK
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("RedditStreamingPipeline")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 3. DEFINE THE STRUCTURE OF AN INCOMING EVENT
# --------------------------------------------------

schema = StructType([
    StructField("id", StringType(), True),
    StructField("subreddit", StringType(), True),
    StructField("text", StringType(), True),
    StructField("created_at", StringType(), True),
])


# --------------------------------------------------
# 4. READ RAW EVENTS FROM KAFKA
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
# 5. PARSE THE JSON EVENT
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
# 6. TRANSFORM / ENRICH EACH EVENT
# --------------------------------------------------

transformed_df = (
    parsed_df

    .withColumn(
        "text_length",
        length(col("text"))
    )

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
                "",
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


# --------------------------------------------------
# 7. CREATE 1-MINUTE STREAMING ANALYSIS
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
# 8. FLATTEN WINDOW RESULTS
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
# 9. WRITE INDIVIDUAL EVENTS TO POSTGRESQL
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
                f"jdbc:postgresql://localhost:{POSTGRES_PORT}/{POSTGRES_DB}",
            )
            .option("dbtable", "reddit_events")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )


# --------------------------------------------------
# 10. UPSERT WINDOW SUMMARIES INTO POSTGRESQL
# --------------------------------------------------

def write_window_summary_to_postgres(batch_df, batch_id):
    rows = batch_df.collect()

    print(f"Window batch {batch_id}: {len(rows)} rows")

    if not rows:
        return

    with psycopg.connect(
        host="localhost",
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
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
# 11. START INDIVIDUAL EVENT STREAM
# --------------------------------------------------

query = (
    transformed_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .start()
)


# --------------------------------------------------
# 12. START WINDOW SUMMARY STREAM
# --------------------------------------------------

window_query = (
    window_summary_df
    .writeStream
    .outputMode("update")
    .foreachBatch(write_window_summary_to_postgres)
    .start()
)


# --------------------------------------------------
# 13. KEEP STREAMS RUNNING
# --------------------------------------------------

spark.streams.awaitAnyTermination()