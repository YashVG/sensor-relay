#!/usr/bin/env python3
"""Exercise real HTTP, edge processes, durable storage, and a lost commit ACK."""

import argparse
from contextlib import closing
import http.client
import http.server
import json
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time

from lab import Lab


def pending(spool):
    with sqlite3.connect(spool) as conn:
        return conn.execute("SELECT count(*) FROM outbox").fetchone()[0]


def scenario(directory, profile):
    lab = Lab(directory, profile)
    checks = []
    spool = lab.directory / "edge.sqlite"
    proxy = None
    proxy_thread = None
    children = []
    try:
        lab.cli("stage", "--spool", spool, "--devices", 2, "--batches", 3, "--batch-size", 8)
        assert pending(spool) == 6
        lab.cli("replay", "--spool", spool, "--url", lab.url, "--attempts", 1, expect_success=False)
        assert pending(spool) == 6
        checks.append("offline replay preserves all 6 durable batches")
        lab.start()

        class DropAcknowledgement(http.server.BaseHTTPRequestHandler):
            # Forward through the real server, wait for its commit receipt, then
            # close the client connection without returning any HTTP response.
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                with closing(http.client.HTTPConnection("127.0.0.1", lab.port, timeout=10)) as upstream:
                    upstream.request("POST", self.path, body, {
                        "Authorization": self.headers["Authorization"],
                        "Content-Type": "application/json"})
                    response = upstream.getresponse()
                    receipt = json.loads(response.read())
                    assert response.status == 200 and receipt["status"] == "committed"
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()

            def log_message(self, *_):
                pass

        proxy = http.server.ThreadingHTTPServer(("127.0.0.1", 0), DropAcknowledgement)
        proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        proxy_thread.start()
        proxy_url = f"http://127.0.0.1:{proxy.server_address[1]}"
        lab.cli("replay", "--spool", spool, "--url", proxy_url, "--attempts", 1, expect_success=False)
        assert pending(spool) == 6
        assert lab.ok("GET", "/v1/snapshot")["committed_samples"] == 8
        checks.append("lost ACK leaves committed batch in the edge outbox")

        lab.stop(crash=True)
        lab.start()
        assert lab.ok("GET", "/v1/snapshot")["committed_samples"] == 8
        report = lab.cli("replay", "--spool", spool, "--url", lab.url)
        assert report["batches"] == 6 and report["samples"] == 48 and report["duplicates"] == 1
        assert pending(spool) == 0
        assert lab.ok("GET", "/v1/snapshot")["committed_samples"] == 48
        checks.append("SIGKILL recovery retains commit and deduplicates its retry")

        lab.cli("stage", "--spool", spool, "--devices", 2, "--batches", 1, "--batch-size", 8)
        lab.cli("replay", "--spool", spool, "--url", lab.url)
        assert lab.ok("GET", "/v1/snapshot")["committed_samples"] == 64
        checks.append("restarted edge resumes the same streams and sequences")

        for device in range(3):
            children.append(subprocess.Popen([
                str(lab.binary), "stream", "--id", f"parallel-{device}",
                "--spool", str(lab.directory / f"edge-{device}.sqlite"),
                "--url", lab.url, "--ticks", "2", "--interval-ms", "10"],
                env=lab.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        for child in children:
            _, error = child.communicate(timeout=40)
            assert child.returncode == 0, error
        assert lab.ok("GET", "/v1/snapshot")["committed_samples"] == 124
        checks.append("3 independent edge processes stream concurrently")

        before_stale = lab.ok("GET", "/v1/snapshot")
        now = int(time.time() * 1000)
        lab.ok("POST", "/v1/telemetry", {"device_id": "offline", "stream_id": "boot", "first_seq": 0,
                                         "samples": [{"observed_ms": now - 120_000, "temperature_mc": -10000, "battery_bps": 3000}]}, "writer")
        snapshot = lab.ok("GET", "/v1/snapshot")
        assert snapshot["committed_samples"] == 125 and snapshot["known_devices"] == 6
        assert snapshot["fresh_devices"] == 5 and snapshot["stale_devices"] == 1
        assert snapshot["mean_temperature_mc"] == before_stale["mean_temperature_mc"]
        windows = lab.ok("GET", f"/v1/windows?from_ms={now - 180_000}&to_ms={now + 1}&bucket_ms=1000")
        assert sum(window["samples"] for window in windows) == 125
        assert lab.request("GET", "/v1/snapshot", role="writer")[0] == 401
        checks.append("event-time analytics count 125 samples; stale sensor is excluded from the latest average")
        checks.append("write credential cannot access read-only queries")
        return {"passed": len(checks), "checks": checks, "final_committed_samples": 125,
                "duplicate_after_lost_ack": report["duplicates"],
                "scope": "localhost HTTP, separate OS processes, SIGKILL; no physical power-loss or hardware test"}
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
        if proxy:
            proxy.shutdown()
            proxy.server_close()
        if proxy_thread:
            proxy_thread.join(timeout=5)
        lab.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=["release", "debug"], default="release")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="sensor-relay-faults-") as directory:
        print(json.dumps(scenario(directory, args.profile), indent=2))
