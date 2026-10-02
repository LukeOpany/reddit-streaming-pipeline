"""One transaction owns source offsets, accepted events, rejects, and summaries.

Portfolio-scale implementation: batches are capped by the Spark reader. A
single state-row lock serializes writers; it is not a distributed sink design.
"""
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

SCHEMA = Path(__file__).resolve().parents[1] / 'sql' / 'reliability.sql'


def initialize(dsn):
    with psycopg.connect(dsn) as conn:
        conn.execute(SCHEMA.read_text())


def parse_event(raw, now):
    try:
        obj = json.loads(raw)
        if not isinstance(obj, dict):
            raise ValueError('expected_object')
        for field in ('id', 'subreddit', 'text', 'created_at'):
            if not isinstance(obj.get(field), str) or not obj[field].strip():
                raise ValueError('missing_or_empty_' + field)
        stamp = datetime.fromisoformat(obj['created_at'].replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            raise ValueError('timestamp_requires_timezone')
        stamp = stamp.astimezone(timezone.utc)
        if stamp > now + timedelta(minutes=5):
            raise ValueError('future_timestamp')
        return obj, stamp
    except (ValueError, TypeError) as exc:
        raise ValueError(str(exc)) from exc


def process_batch(dsn, records, fail_before_commit=False, now=None):
    """records contain topic, partition, offset, value (raw JSON string).

    Lateness is compared against the prior committed high-water mark minus
    two minutes. Boundary timestamps are accepted. First payload wins per ID.
    Replays are identified by Kafka topic/partition/offset, not Spark batch ID.
    """
    now = now or datetime.now(timezone.utc)
    counts = dict(accepted=0, duplicate=0, rejected=0, replayed=0)
    affected = set()
    with psycopg.connect(dsn) as conn:
        previous = conn.execute(
            'SELECT high_water FROM pipeline_state WHERE singleton = 1 FOR UPDATE'
        ).fetchone()[0]
        maximum = previous
        for record in records:
            source_key = (record['topic'], record['partition'], record['offset'])
            claimed = conn.execute(
                'INSERT INTO processed_offsets VALUES (%s,%s,%s) '
                'ON CONFLICT DO NOTHING RETURNING offset_id', source_key
            ).fetchone()
            if claimed is None:
                counts['replayed'] += 1
                continue
            raw = record['value']
            try:
                event, stamp = parse_event(raw, now)
                # Existing IDs never affect watermark or counts, even if payload changed.
                if conn.execute('SELECT 1 FROM reddit_events WHERE id=%s', (event['id'],)).fetchone():
                    counts['duplicate'] += 1
                    continue
                if previous and stamp < previous - timedelta(minutes=2):
                    raise ValueError('too_late')
            except ValueError as exc:
                conn.execute('INSERT INTO rejected_events(topic,partition_id,offset_id,raw_value,reason) '
                             'VALUES (%s,%s,%s,%s,%s)', (*source_key, raw if isinstance(raw, str) else repr(raw), str(exc)))
                counts['rejected'] += 1
                continue
            clean = re.sub(r'[^a-z0-9\s]', '', event['text'].lower()).strip()
            length = len(event['text'])
            conn.execute('''INSERT INTO reddit_events
                (id,subreddit,text,created_at,text_length,text_size,clean_text,
                 mentions_python,mentions_spark,mentions_kafka,event_timestamp,event_hour,ingested_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                (event['id'], event['subreddit'], event['text'], event['created_at'], length,
                 'short' if length < 45 else 'medium' if length <= 55 else 'long', clean,
                 int('python' in clean), int('spark' in clean), int('kafka' in clean),
                 stamp, stamp.hour, now))
            affected.add((stamp.replace(second=0, microsecond=0), event['subreddit']))
            maximum = max(maximum, stamp) if maximum else stamp
            counts['accepted'] += 1
        for start, subreddit in affected:
            end = start + timedelta(minutes=1)
            conn.execute('''INSERT INTO event_window_summary
                SELECT %s,%s,%s,count(*) FROM reddit_events
                WHERE event_timestamp >= %s AND event_timestamp < %s AND subreddit=%s
                ON CONFLICT (window_start,subreddit) DO UPDATE SET event_count=EXCLUDED.event_count''',
                (start, end, subreddit, start, end, subreddit))
        conn.execute('UPDATE pipeline_state SET high_water=%s WHERE singleton=1', (maximum,))
        if fail_before_commit:
            raise RuntimeError('Injected failure before transaction commit')
    return counts
