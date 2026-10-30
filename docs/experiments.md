# Experiments

These are small ways to explore the behavior before adding more features. Each uses the existing code and keeps the failure visible.

## 1. Lose the response after saving data

Run `python3 scripts/scenario.py`, then read `DropAcknowledgement` in that script. The proxy waits for the collector's successful commit, closes the connection without returning the response, and leaves the batch in the client's outbox.

The important observation is the next replay: it reports one duplicate, clears the queue, and leaves only one saved copy. Compare the point where `Store::ingest` commits with the point where `Outbox::acknowledge` deletes a batch. Moving deletion before the response would create a data-loss window.

## 2. Change a retry

```sh
./scripts/cargo.sh test identical_retry_is_idempotent_and_changed_retry_conflicts
```

The test sends one batch twice, then changes a reading while keeping its identity. The first retry succeeds as a duplicate; the changed retry is rejected. This is why a sequence number alone is not enough to detect a caller accidentally reusing a batch ID.

## 3. Make the queue tiny

```sh
python3 scripts/benchmark.py --queue-capacity 1 --concurrency 64
```

Inspect `overloaded_responses`, p95 latency, and verified row counts. Rejected requests should increase, and retries should still deliver the expected readings. Then reduce concurrency to four. Compare tail latency as well as throughput; the load generator includes retry delays in the latency it reports.

## 4. Change batch size

```sh
python3 scripts/benchmark.py --batch-size 16
python3 scripts/benchmark.py --batch-size 128
```

A larger batch spreads transaction overhead across more readings. It also changes the total workload here, because the number of batches stays fixed. Keep total readings constant for a more controlled comparison, and avoid interpreting one short run as a capacity limit.

## 5. Arrive late

```sh
./scripts/cargo.sh test event_time_snapshot_ignores_late_older_and_not_yet_current_samples
```

An older reading can arrive after a newer one. The snapshot should still use the newer observation. A time-window query can include both. Explore how the answer changes when one sensor reports much more often than another.

## Possible next steps

- Replace one simulated sender with a real sensor client, while preserving stream IDs across restarts.
- Compare the SQLite outbox with an append-only file and document recovery behavior.
- Separate reads from the storage worker and measure whether queries still delay ingestion.

These are follow-up ideas, not implemented capabilities.
