"""Exercise the real PostgreSQL sink in fresh worker processes; no Kafka required."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg
from psycopg.conninfo import make_conninfo
from reliability.processor import initialize, process_batch


def event(identifier, stamp, offset):
    return dict(topic='demo', partition=0, offset=offset, value=json.dumps(dict(
        id=identifier, subreddit='dataengineering', text='Python Spark Kafka', created_at=stamp.isoformat())))


def run(dsn):
    # Isolated schema prevents the demo from modifying existing pipeline records.
    schema = 'demo_' + uuid.uuid4().hex
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(psycopg.sql.SQL('CREATE SCHEMA {}').format(psycopg.sql.Identifier(schema)))
    isolated = make_conninfo(dsn, options=f'-c search_path={schema}')
    initialize(isolated)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    first = [event('first', now, 0), event('first', now, 1),
             dict(topic='demo', partition=0, offset=2, value='not json')]
    started = time.perf_counter()

    def worker(rows, fail=False):
        env = dict(os.environ, DEMO_DSN=isolated)
        result = subprocess.run([sys.executable, '-m', 'reliability.demo', '--worker'] + (['--fail'] if fail else []),
                                input=json.dumps(rows), text=True, capture_output=True, env=env)
        if fail:
            assert result.returncode != 0, 'Injected failure did not fail'
            return
        if result.returncode: raise RuntimeError(result.stderr)
        return json.loads(result.stdout)

    try:
        worker(first, fail=True)
        with psycopg.connect(isolated) as conn:
            assert conn.execute('SELECT count(*) FROM processed_offsets').fetchone()[0] == 0
            assert conn.execute('SELECT count(*) FROM reddit_events').fetchone()[0] == 0
        first_result = worker(first)
        replay_result = worker(first)  # New process, identical Kafka offsets.
        late_result = worker([event('late', now - timedelta(minutes=3), 3),
                              event('boundary', now - timedelta(minutes=2), 4)])
        with psycopg.connect(isolated) as conn:
            events = conn.execute('SELECT count(*) FROM reddit_events').fetchone()[0]
            summaries = conn.execute('SELECT sum(event_count) FROM event_window_summary').fetchone()[0]
            rejects = dict(conn.execute('SELECT reason,count(*) FROM rejected_events GROUP BY reason').fetchall())
            latency = conn.execute('SELECT percentile_cont(0.95) WITHIN GROUP '
                                   '(ORDER BY extract(epoch from ingested_at-event_timestamp)) FROM reddit_events').fetchone()[0]
        assert first_result == dict(accepted=1,duplicate=1,rejected=1,replayed=0)
        assert replay_result['replayed'] == 3
        assert late_result['accepted'] == 1 and late_result['rejected'] == 1
        assert events == summaries == 2
        elapsed = time.perf_counter() - started
        return dict(scope='PostgreSQL sink only; fresh Python workers, not Kafka/Spark restart',
                    recorded_at=datetime.now(timezone.utc).isoformat(),
                    accepted_events=events, summary_total=int(summaries), rejection_reasons=rejects,
                    rollback_verified=True, process_replay_verified=True, duplicate_suppression_verified=True,
                    boundary_lateness_verified=True, scenario_seconds=round(elapsed,3),
                    scenario_input_records_per_second=round(5/elapsed,3),
                    event_age_at_write_p95_seconds=round(latency,3),
                    limitation='Tiny correctness scenario includes deliberate two-minute-old input and process startup. '
                               'Rate is not sustained throughput; event age is not Kafka end-to-end latency.')
    finally:
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(psycopg.sql.SQL('DROP SCHEMA {} CASCADE').format(psycopg.sql.Identifier(schema)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--fail', action='store_true')
    parser.add_argument('--output', default='reports/recovery.json')
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(process_batch(os.environ['DEMO_DSN'], json.load(sys.stdin), args.fail)))
    else:
        from dotenv import load_dotenv
        from reliability.config import database_dsn
        load_dotenv()
        result = run(database_dsn())
        target = Path(args.output); target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2)+'\n')
        print(json.dumps(result, indent=2))
