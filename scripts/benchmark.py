#!/usr/bin/env python3
"""Run loadgen against a fresh local database and verify committed row counts."""

import argparse
from datetime import datetime, timezone
import json
import platform
import subprocess
import tempfile

from lab import Lab, ROOT


def benchmark(directory, args):
    lab = Lab(directory, "release", args.queue_capacity)
    try:
        lab.start()
        result = subprocess.run([
            str(ROOT / "target/release/loadgen"), "--url", lab.url,
            "--devices", str(args.devices), "--batches", str(args.batches),
            "--batch-size", str(args.batch_size), "--concurrency", str(args.concurrency)],
            env=lab.env, text=True, capture_output=True, timeout=180)
        if result.returncode:
            raise AssertionError(result.stderr)
        report = json.loads(result.stdout)
        snapshot = lab.ok("GET", "/v1/snapshot")
        assert snapshot["committed_samples"] == report["unique_samples"]
        assert snapshot["committed_batches"] == report["committed_batches"]
        status, metrics = lab.request("GET", "/metrics")
        assert status == 200
        counters = {line.split()[0]: int(line.split()[1]) for line in metrics.splitlines() if not line.startswith("#")}
        assert counters["sensor_relay_committed_samples_total"] == report["unique_samples"]
        assert counters["sensor_relay_duplicate_batches_total"] == report["duplicate_receipts"]
        report.update({"measured_at_utc": datetime.now(timezone.utc).isoformat(),
                       "queue_capacity": args.queue_capacity, "duckdb_version": "2.0.0",
                       "rust_profile": "release", "os_release": platform.release(),
                       "verified_committed_samples": snapshot["committed_samples"],
                       "verified_committed_batches": snapshot["committed_batches"],
                       "server_counters": counters,
                       "environment": "single host, loopback HTTP, fresh temporary database, synthetic load; server and client share resources"})
        return report
    finally:
        lab.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devices", type=int, default=64)
    parser.add_argument("--batches", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=64)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--queue-capacity", type=int, default=64)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sensor-relay-bench-") as directory:
        print(json.dumps(benchmark(directory, args), indent=2))
