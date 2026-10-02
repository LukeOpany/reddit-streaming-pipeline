"""Bounded Spark microbatches with persistent offsets and a transactional sink."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from dotenv import load_dotenv
from reliability.config import database_dsn
from reliability.processor import initialize, process_batch


def main():
    from pyspark.sql import SparkSession
    load_dotenv()
    dsn = database_dsn()
    initialize(dsn)
    spark = SparkSession.builder.appName('RedditReliableStreaming').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    source = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
              .option('subscribe', os.getenv('KAFKA_TOPIC', 'reddit_messages'))
              .option('startingOffsets', 'earliest')
              .option('maxOffsetsPerTrigger', 1000).load()
              .selectExpr('topic', 'partition', 'offset', 'CAST(value AS STRING) AS value'))

    def write_batch(frame, batch_id):
        rows = (row.asDict() for row in frame.orderBy('topic', 'partition', 'offset').toLocalIterator())
        print({'batch_id': batch_id, **process_batch(dsn, rows)}, flush=True)

    query = (source.writeStream.foreachBatch(write_batch)
             .option('checkpointLocation', os.getenv('SPARK_CHECKPOINT_DIR', 'checkpoints/reddit-v2'))
             .start())
    query.awaitTermination()


if __name__ == '__main__':
    main()
