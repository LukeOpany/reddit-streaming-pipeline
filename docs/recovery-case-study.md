# Keeping a streaming dashboard accurate after a retry

Luke Opany · Data engineering portfolio

I built a local pipeline that turns synthetic Reddit activity into per-minute subreddit counts. In the first version, one writer stored the events and another updated the summaries. If processing stopped between those writes, a dashboard count could disagree with the rows behind it. I wanted to answer a question a dashboard user could reasonably ask: if the pipeline processes my event twice, will the chart count it twice?

## Where a retry can go wrong

Spark reads Kafka records in batches and saves its progress in a checkpoint. Suppose PostgreSQL commits a batch and Spark stops before saving that progress. On restart, the database can see records it has already processed. Appending those records again would inflate the dashboard totals.

I moved the event writes, source offsets, rejected inputs, event-time progress, and minute summaries into one PostgreSQL transaction. A failure before commit rolls them all back. After commit, the database has enough information to recognize a replay. [Transaction processor](../reliability/processor.py) · [Database schema](../sql/reliability.sql)

Two identifiers handle different ways an event can come back. The Kafka topic, partition, and offset identify a specific record the reader has seen. The event ID identifies the underlying activity. If the producer sends the same event at a new offset, the first accepted payload still contributes exactly one count.

## Putting recovery to the test

I used fresh Python processes against a temporary PostgreSQL schema so a retry had to rely on what the previous process had committed. [Recovery demo](../reliability/demo.py)

| Scenario | Recorded result |
|---|---|
| Fail before commit | The transaction rolled back, including its offset claims. A new process could retry the records. |
| Replay committed records | The replay added no events or summary counts. |
| Deliver an existing event ID at a new offset | The duplicate added no count. |
| Submit malformed or late input | One malformed record and one late record were rejected. An event exactly at the two-minute lateness boundary was accepted. |

The run ended with **two accepted events and a total of two across the minute summaries**. That reconciliation checks the number a dashboard user actually sees. The repository records seven passing automated checks, including this recovery scenario. [Recorded results](recovery-evidence.json) · [Reproduction steps](../README.md#reproduce-recovery-evidence)

## Trade-off and scope

A single serialized PostgreSQL writer keeps the recovery rules easy to audit and limits write concurrency. The recorded test covers the PostgreSQL sink. The next test would interrupt Spark between database commit and checkpoint, restart it against the same Kafka topic and checkpoint, and compare the summary totals with the unique accepted events.
