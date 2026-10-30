# Benchmark notes

These runs explore how batching, queue capacity, and concurrent senders affect a small collector. They use Sensor Relay 0.2.0 with the temperature/battery sample format. Raw JSON files are linked below so the workload and timing scope can be checked.

## Setup

Measured October 30, 2026 on an Apple M3 Pro laptop (11 CPU cores, 18 GiB RAM, Darwin 27.2.0), using Rust 1.99.1 release builds and DuckDB 2.0.0. Client and server share the host and communicate over loopback HTTP. Each run starts with a fresh temporary database. There is no warm-up phase or dedicated benchmark machine.

The three baseline runs each use 64 simulated sensors, eight batches per sensor, 64 readings per batch, and 16 concurrent streams. The larger run uses 128 batches per sensor and 128 readings per batch. All four use a mailbox capacity of 64.

| Run | Unique readings | Readings/sec | p50 / p95 / p99 batch latency |
| --- | ---: | ---: | --- |
| [Baseline 1](evidence/baseline-1.json) | 32,768 | 31,361 | 29.70 / 33.87 / 37.14 ms |
| [Baseline 2](evidence/baseline-2.json) | 32,768 | 32,590 | 29.49 / 30.34 / 32.98 ms |
| [Baseline 3](evidence/baseline-3.json) | 32,768 | 28,885 | 29.93 / 47.05 / 49.74 ms |
| [Larger batch/run](evidence/million-samples.json) | 1,048,576 | 51,271 | 34.06 / 49.62 / 60.82 ms |

The larger run took 20.45 seconds, committed 8,192 batches, and returned 2,048 duplicate receipts for deliberate retries. Every run checks the stored reading and receipt counts against the generated workload. The larger batch amortizes transaction overhead, but this comparison also changes workload length, so it is not a controlled measurement of batch size alone.

Latency covers HTTP serialization, transport, queue wait, commit, and acknowledgement. It includes overload retry waits; separately sent duplicate probes are excluded. Throughput divides unique readings by total load-generation wall time, including duplicate probes. Server startup and final verification queries are outside the measured interval. These are short localhost experiments, not a deployment capacity estimate.

## Try changing the workload

```sh
./scripts/cargo.sh build --release --locked --bins
python3 scripts/benchmark.py
python3 scripts/benchmark.py --batches 128 --batch-size 128
python3 scripts/benchmark.py --queue-capacity 1 --concurrency 64
```

The [one-slot queue experiment](evidence/overload.json) returned 1,088 overload responses. After retries, all 32,768 expected readings and 512 receipts were present. Compare its latency distribution with the baseline: a bounded queue makes saturation visible, but rejected requests still spend time waiting and retrying.

## Checks behind the numbers

There are 16 passing Rust tests covering duplicate/gap handling, transaction rollback, persistent checkpoints, stale/future observations, value bounds, and HTTP authentication. Formatting and Clippy also pass.

The [process experiment](evidence/fault-scenario.json) passes seven checks. It disconnects a client after commit, kills and restarts the collector, replays the durable queue, and runs three edge processes concurrently. The final database contains exactly 125 readings. It tests process termination, not physical storage failure.

[CI](https://github.com/YashVG/sensor-relay/actions/workflows/ci.yml) runs the Rust checks and process experiment on Ubuntu. The performance values here come from the local release build.
