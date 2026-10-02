# Reliable Streaming Analytics

Kafka → Spark microbatches → transactional PostgreSQL event storage and minute summaries → Streamlit.

This portfolio project uses synthetic Reddit-like events. It demonstrates a bounded local streaming workflow and a reproducible PostgreSQL recovery scenario; it is not a deployed distributed service.

## What changed

The original detail append and independent summary writer could disagree after retries. A single transaction now owns source offsets, accepted events, rejected input, the event-time high-water mark, and affected minute summaries. Spark persists its progress in a checkpoint directory.

| Case | Behavior |
|---|---|
| Same Kafka topic/partition/offset again | Skip a previously committed record |
| Same event ID at a new offset | First payload wins; no duplicate event or summary count |
| Malformed JSON, missing fields, invalid/timezone-free date | Store raw input and reason in `rejected_events` |
| Timestamp over five minutes in the future | Reject to protect the event-time high-water mark |
| Timestamp older than prior committed maximum minus two minutes | Reject as `too_late`; exact boundary is accepted |
| Failure before transaction commit | Roll back all records, offset claims, and summary changes |
| Process restart after commit but before Spark checkpoint | Replayed offsets become no-ops |

The lateness policy is implemented in the transaction processor, **not Spark's native watermark operator**. All records in a microbatch use the previous committed maximum; the first batch has no historical lateness cutoff. This makes acceptance consistent across transaction retries. Detail and summary tables include the same accepted population.

## Architecture and files

- [Spark reader](spark/reddit_stream.py): caps each trigger at 1,000 Kafka offsets, starts at earliest on a new checkpoint, and invokes the transactional processor.
- [Processor](reliability/processor.py): validates events and writes everything atomically. A state-row lock serializes writers.
- [SQL schema](sql/reliability.sql): event-ID primary key, source-offset ledger, rejects, summaries, and processing state.
- [Recovery demo](reliability/demo.py): uses fresh Python processes against a temporary PostgreSQL schema, injects a transaction failure, then retries and replays data.
- [Dashboard](dashboard/app.py): shows event counts, subreddit activity, minute summaries, keyword mentions, and recent events.
- [Automated checks](.github/workflows/tests.yml): validation tests and the PostgreSQL recovery scenario.

## Run locally

Use Python 3.12, Docker, and the Java version required by the installed Spark release. Install project dependencies in a fresh environment:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Set local PostgreSQL credentials in .env.
docker compose up -d
```

**Existing installations:** use a fresh database and a new checkpoint directory for this schema. The old append-only table has no primary key or `ingested_at` column; `CREATE TABLE IF NOT EXISTS` is not a migration. Preserve existing records before switching. PostgreSQL initialization SQL runs automatically for a new Compose volume.

In separate terminals, from the repository root with the environment activated:

```bash
python producer/reddit_producer.py
```

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 spark/reddit_stream.py
```

```bash
streamlit run dashboard/app.py
```

Keep `checkpoints/reddit-v2` across Spark restarts. Override it with `SPARK_CHECKPOINT_DIR`. Database variables are read from `.env`; `DATABASE_URL` overrides individual variables for the processor/demo. Kafka can be configured with `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_TOPIC` in the reader.

## Reproduce recovery evidence

```bash
python -m reliability.demo --output reports/recovery.json
pip install pytest
python -m pytest -q
# To include the database test:
TEST_DATABASE_URL='postgresql://user:password@localhost:5433/database' python -m pytest -q
```

The demo creates and removes its own schema. It requires permission to create schemas and does not clear pipeline tables. It checks rollback, duplicate suppression, cross-process replay, malformed input, late input, and exact lateness-boundary acceptance. Generated reports distinguish scenario rate and event age from sustained throughput and true end-to-end latency.

## Recorded validation

[Recovery evidence](docs/recovery-evidence.json) records a local PostgreSQL 17 run: 7 automated checks passed, including the fresh-process recovery scenario. Docker/Kafka/Spark integration was not executed on the validation host.

## Limits and next measurements

This design prioritizes auditable correctness at portfolio scale. It processes a bounded batch on the driver and uses a serialized PostgreSQL writer. Offset and event history grow indefinitely; retention must preserve replay guarantees. Kafka storage remains the original local single-broker setup, so broker loss is outside the verified recovery scope. Do not reuse a topic name with reset offsets against an old ledger.

The database scenario does not prove Kafka/Spark restart behavior. A full deployment check should interrupt Spark between commit and checkpoint, restart with the same checkpoint, and reconcile unique event IDs and summary totals. Sustained throughput and end-to-end latency need a larger controlled run with the actual broker and Spark runtime.
