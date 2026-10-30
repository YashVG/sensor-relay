# How it works

The main problem is deciding when an edge client can forget a reading. A network error leaves two possibilities: the collector never received the batch, or it saved the batch and the response was lost. The client cannot distinguish them from the error alone.

## Follow one batch

1. `Outbox::enqueue` saves a batch and advances its sequence in one SQLite transaction. Each sensor keeps a persistent stream ID across restarts.
2. The client sends the oldest queued batch over HTTP. Failed attempts leave it on disk.
3. Axum passes the request to a bounded Tokio mailbox. A dedicated thread owns DuckDB and performs the synchronous database work.
4. One DuckDB transaction saves the readings, next expected sequence, and a retry receipt containing a hash of the parsed batch.
5. The collector returns its receipt after commit. The client checks it before deleting the local batch.

Repeating the same identity and content returns `duplicate`. Reusing an identity with different content, skipping a sequence, or overlapping a batch returns `409`. JSON whitespace and key order do not change the digest. Starting a new outbox creates a new stream; copying old readings into a new stream will count them again.

This is at-least-once delivery with idempotent storage for a retained identity. It depends on keeping the receipt and database. It does not provide replication or a distributed exactly-once guarantee.

## Why these choices

| Choice | What it makes easier to study | Cost |
| --- | --- | --- |
| SQLite outbox | Atomic queue updates and restart recovery | A second database dependency |
| One DuckDB owner thread | Clear ownership and ordered transactions | Queries can delay writes |
| Bounded mailbox | Visible overload instead of an endlessly growing work queue | Clients must retry `503` responses |
| Integer readings | Stable serialization and explicit units | Callers convert millidegrees/basis points for display |

The [DuckDB concurrency model](https://duckdb.org/docs/2.0/connect/concurrency) is useful background for the single-owner design. An append-only outbox file would be another interesting implementation, but would need its own recovery and compaction rules.

## Time and capacity

A snapshot takes the latest observation per sensor at or before server time, then excludes stale sensors from its averages. Ties use stream ID and sequence. Window queries use `[from, to)` and average every reading, so a faster-reporting sensor contributes more. These are different questions, especially when data arrives late. Clock synchronization is assumed.

Batches contain at most 256 readings. The mailbox defaults to 64 operations, with four extra admitted requests. Saturation returns `503`; accepted work can still commit after an HTTP timeout. The edge stops adding readings when its outbox limit is reached and preserves the pending data. These bounds do not cap database growth or all network memory.

## Current boundaries

The service defaults to localhost. Shared write/read tokens are enough for these experiments; public deployment would need TLS and per-device authorization. Receipts and readings are retained indefinitely. Metrics reset on restart; database counts persist. There are no schema migrations, so use a fresh database when switching between incompatible sample formats. The failure script tests process termination and lost responses, not physical power loss or disk failure.
