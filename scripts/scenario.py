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
        assert lab.ok("GET", "/v1/fleet")["committed_samples"] == 8
        checks.append("lost ACK leaves committed batch in the edge outbox")

        lab.stop(crash=True)
        lab.start()
        assert lab.ok("GET", "/v1/fleet")["committed_samples"] == 8
        report = lab.cli("replay", "--spool", spool, "--url", lab.url)
        assert report["batches"] == 6 and report["samples"] == 48 and report["duplicates"] == 1
        assert pending(spool) == 0
        assert lab.ok("GET", "/v1/fleet")["committed_samples"] == 48
        checks.append("SIGKILL recovery retains commit and deduplicates its retry")

        lab.cli("stage", "--spool", spool, "--devices", 2, "--batches", 1, "--batch-size", 8)
        lab.cli("replay", "--spool", spool, "--url", lab.url)
        assert lab.ok("GET", "/v1/fleet")["committed_samples"] == 64
        checks.append("restarted edge resumes the same streams and sequences")

        for device in range(3):
            children.append(subprocess.Popen([
                str(lab.binary), "device", "--id", f"parallel-{device}",
                "--spool", str(lab.directory / f"edge-{device}.sqlite"),
                "--url", lab.url, "--ticks", "2", "--interval-ms", "10"],
                env=lab.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        for child in children:
            _, error = child.communicate(timeout=40)
            assert child.returncode == 0, error
        assert lab.ok("GET", "/v1/fleet")["committed_samples"] == 124
        checks.append("3 independent edge processes stream concurrently")

        expires = int(time.time() * 1000) + 20_000
        for name in ["old", "current"]:
            lab.ok("POST", "/v1/commands", {"command_id": name, "device_id": "battery-0000",
                                             "target_w": 1200, "expires_ms": expires})
        assert lab.request("POST", "/v1/devices/battery-0000/commands/old/ack", role="device")[0] == 409
        assert lab.cli("poll", "--spool", spool, "--url", lab.url)["new_simulated_applications"] == 1
        assert lab.cli("poll", "--spool", spool, "--url", lab.url)["new_simulated_applications"] == 0
        command = lab.ok("GET", "/v1/devices/battery-0000/command", role="device")
        assert command["acknowledged"] and command["generation"] == 2
        checks.append("superseded ACK rejected; command application survives edge restart")

        lab.ok("POST", "/v1/commands", {"command_id": "short-lived", "device_id": "battery-0000",
                                         "target_w": 0, "expires_ms": int(time.time() * 1000) + 300})
        deadline = time.monotonic() + 5
        while lab.ok("GET", "/v1/devices/battery-0000/command", role="device") is not None:
            assert time.monotonic() < deadline
            time.sleep(0.05)
        assert lab.cli("poll", "--spool", spool, "--url", lab.url)["new_simulated_applications"] == 0
        checks.append("expired command is not delivered or newly applied")

        now = int(time.time() * 1000)
        lab.ok("POST", "/v1/telemetry", {"device_id": "offline", "stream_id": "boot", "first_seq": 0,
                                         "samples": [{"observed_ms": now - 120_000, "power_w": 9999, "soc_bps": 3000}]}, "device")
        fleet = lab.ok("GET", "/v1/fleet")
        assert fleet["committed_samples"] == 125 and fleet["known_devices"] == 6
        assert fleet["fresh_devices"] == 5 and fleet["stale_devices"] == 1
        assert lab.request("POST", "/v1/commands", {"command_id": "unsafe-stale", "device_id": "offline",
                                                    "target_w": 1000, "expires_ms": now + 10_000})[0] == 409
        windows = lab.ok("GET", f"/v1/windows?from_ms={now - 180_000}&to_ms={now + 1}&bucket_ms=1000")
        assert sum(window["samples"] for window in windows) == 125
        assert lab.request("GET", "/v1/fleet", role="device")[0] == 401
        checks.append("event-time analytics count 125 samples; stale device cannot receive a new command")
        checks.append("device credential cannot read operator analytics")
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
    with tempfile.TemporaryDirectory(prefix="gridstream-faults-") as directory:
        print(json.dumps(scenario(directory, args.profile), indent=2))
