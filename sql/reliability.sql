CREATE TABLE IF NOT EXISTS reddit_events (
    id TEXT PRIMARY KEY, subreddit TEXT NOT NULL, text TEXT NOT NULL,
    created_at TEXT NOT NULL, text_length INTEGER NOT NULL, text_size TEXT NOT NULL,
    clean_text TEXT NOT NULL, mentions_python INTEGER NOT NULL,
    mentions_spark INTEGER NOT NULL, mentions_kafka INTEGER NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL, event_hour INTEGER NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS reddit_events_window ON reddit_events(event_timestamp, subreddit);
CREATE TABLE IF NOT EXISTS event_window_summary (
    window_start TIMESTAMPTZ NOT NULL, window_end TIMESTAMPTZ NOT NULL,
    subreddit TEXT NOT NULL, event_count BIGINT NOT NULL,
    PRIMARY KEY(window_start,subreddit)
);
CREATE TABLE IF NOT EXISTS processed_offsets (
    topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id BIGINT NOT NULL,
    PRIMARY KEY(topic,partition_id,offset_id)
);
CREATE TABLE IF NOT EXISTS rejected_events (
    topic TEXT NOT NULL, partition_id INTEGER NOT NULL, offset_id BIGINT NOT NULL,
    raw_value TEXT NOT NULL, reason TEXT NOT NULL,
    PRIMARY KEY(topic,partition_id,offset_id)
);
CREATE TABLE IF NOT EXISTS pipeline_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1), high_water TIMESTAMPTZ
);
INSERT INTO pipeline_state VALUES (1,NULL) ON CONFLICT DO NOTHING;
