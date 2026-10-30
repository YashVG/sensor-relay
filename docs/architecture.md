# Architecture and guarantees

GridStream separates independently running edge clients from one HTTP ingest and analytics service. Tokio handles networking; a dedicated thread owns the DuckDB connection. Its bounded mailbox serializes database operations and keeps synchronous database work off the async executor.

## Ingest and replay

Each device has a persistent `stream_id` and increasing sequence. SQLite commits the serialized batch and next sequence in one local transaction, using WAL mode and `synchronous=FULL`. Restarting an edge reuses that identity. A deliberately new outbox starts a new stream; replaying historical samples under a new identity can count them again.

DuckDB receives at most 256 samples per batch. One transaction writes:

1. Samples keyed by `(device_id, stream_id, sequence)`.
2. The next expected sequence for that stream.
3. A receipt keyed by `(device_id, stream_id, first_seq)`, with a SHA-256 digest of the parsed batch's canonical serialization.

Only a committed transaction produces a successful receipt. An identical retry returns `duplicate`; a changed payload, overlap, or gap returns `409`. JSON whitespace and object-key order do not affect the digest. The edge removes its outbox entry only after a matching successful receipt.

A lost response can leave a committed batch pending at the edge. Retrying resolves that ambiguity. This provides at-least-once transport with idempotent persistence for a retained batch identity. Receipt retention is currently unbounded; deleting receipts or losing the database breaks that guarantee. No distributed exactly-once guarantee is claimed.

## Capacity and event time

The mailbox defaults to 64 operations; admission allows four additional requests. A full queue returns `503`. A handler timeout does not cancel an accepted storage operation, so clients retry the same payload. An edge outbox stops capture at its configured batch limit without discarding pending batches. Neither limit is a disk quota or a bound on all network connection memory.

Fleet queries take one latest observation per device at or before server time, then exclude stale devices from power and charge aggregates. Equal event times use stream ID and sequence as deterministic tie breakers. A device with only future samples is not yet counted as known. Window queries use `[from, to)` event-time bounds and sample-weighted means; they do not estimate energy or simultaneous fleet power. Clock synchronization is assumed.

## Simulated commands

An operator can issue a bounded setpoint only with telemetry from the last 30 seconds. Commands have increasing per-device generations and expire within five minutes. The edge records a generation once in SQLite; the server rejects acknowledgements for superseded or expired commands. An older command already in transit can still apply before the edge learns about a newer one. Expiry prevents new application; the stored audit record is retained. There is no physical actuator or automatic physical fail-safe.

## Deliberate limits

One DuckDB owner makes transaction and ordering behavior easy to inspect, but analytics can delay ingest and the node is a single point of failure. Sharding would require device routing, cross-shard queries, and a separate replication/recovery design. [DuckDB's concurrency model](https://duckdb.org/docs/2.0/connect/concurrency) informs this ownership choice.

Bearer tokens distinguish two trusted lab roles; all devices share one credential. TLS termination, per-device authorization, token rotation, retention, schema migrations, disk-failure recovery, and distributed deployment remain outside this implementation. The server binds to loopback by default. Metrics reset on process restart; durable counts come from DuckDB.
