# Measured results

These are local synthetic measurements from October 30, 2026. The server and load generator ran on the same Apple M3 Pro host (11 CPU cores, 18 GiB RAM, macOS/Darwin 27.2.0), using Rust 1.99.1 release builds and DuckDB 2.0.0. Each run used a fresh temporary database, loopback HTTP, a queue capacity of 64, and 16 concurrent device streams. There was no warm-up phase or dedicated benchmark machine.

## Throughput and commit latency

| Workload | Unique samples | Samples/sec | p50 / p95 / p99 batch latency |
| --- | ---: | ---: | --- |
| [Baseline 1](evidence/baseline-1.json): 64 devices × 8 batches × 64 samples | 32,768 | 24,697 | 36.74 / 46.82 / 55.83 ms |
| [Baseline 2](evidence/baseline-2.json): same workload | 32,768 | 31,276 | 30.14 / 32.61 / 37.63 ms |
| [Baseline 3](evidence/baseline-3.json): same workload | 32,768 | 26,875 | 32.56 / 43.77 / 45.29 ms |
| [Larger run](evidence/million-samples.json): 64 devices × 128 batches × 128 samples | 1,048,576 | 56,308 | 32.11 / 44.59 / 64.86 ms |

The larger run took 18.62 seconds, committed 8,192 batches, and returned 2,048 duplicate receipts for intentional retries. The harness checked persisted row and receipt counts against expected totals and verified server counters. Larger batches amortize transaction overhead; this table does not isolate batch size from workload length.

Latency covers HTTP serialization, loopback transport, queue wait, DuckDB commit, and acknowledgement. It includes any `503` retry waits and excludes separately timed duplicate probes. Throughput divides unique committed samples by total load-generation wall time, including duplicate probes. Sample construction, server startup, and the final verification queries are outside the reported batch-latency measurement; server startup and final verification are also outside throughput timing. These are short local tests, not WAN, long-running soak, power-loss, or production capacity results.

Reproduce:

```sh
./scripts/cargo.sh build --release --locked --bins
python3 scripts/benchmark.py
python3 scripts/benchmark.py --batches 128 --batch-size 128
python3 scripts/benchmark.py --queue-capacity 1 --concurrency 64
```

The [overload run](evidence/overload.json) reduces the mailbox to one slot and raises concurrency to 64. All 32,768 expected samples and 512 batch receipts were present after retries. Its raw output includes rejected attempts and latency with retry waits; this run checks recovery under saturation.

## Reliability checks

`./scripts/cargo.sh test --locked` passed **19 Rust tests**, including simultaneous retries, gap/overlap rejection, rollback after an injected final-insert failure, persistent stream checkpoints, stale/future event handling, authentication boundaries, and command generations.

`python3 scripts/scenario.py` passed [nine process checks](evidence/fault-scenario.json). It retains offline batches, drops a real commit acknowledgement, kills the server with `SIGKILL`, verifies replay after restart, runs three independent edge processes, and checks simulated command expiry and analytics. Its final database contains exactly 125 samples. Process termination is tested; physical storage failure and loss of power are not.

[CI](https://github.com/YashVG/gridstream-rs/actions/workflows/ci.yml) runs formatting, Clippy, Rust tests, and the process scenario on Ubuntu. Performance values above are from the local release build, not CI.
