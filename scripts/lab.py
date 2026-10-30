"""Standard-library helpers for reproducible, isolated local experiments."""

import json
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class Lab:
    def __init__(self, directory, profile="release", queue_capacity=64):
        self.directory = Path(directory)
        self.binary = ROOT / "target" / profile / "sensor-relay"
        if not self.binary.is_file():
            raise RuntimeError(f"Build first: ./scripts/cargo.sh build --{profile} --bins"
                               if profile == "release" else "Build first: ./scripts/cargo.sh build --bins")
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.write_token = secrets.token_hex(24)
        self.read_token = secrets.token_hex(24)
        self.env = dict(os.environ, SENSOR_RELAY_WRITE_TOKEN=self.write_token,
                        SENSOR_RELAY_READ_TOKEN=self.read_token)
        self.queue_capacity = queue_capacity
        self.process = None
        self.log = open(self.directory / "server.log", "a", encoding="utf-8")

    def start(self):
        if self.process is not None:
            raise RuntimeError("Server already started")
        self.process = subprocess.Popen(
            [str(self.binary), "serve", "--bind", f"127.0.0.1:{self.port}",
             "--db", str(self.directory / "readings.duckdb"),
             "--queue-capacity", str(self.queue_capacity)],
            env=self.env, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError((self.directory / "server.log").read_text())
            try:
                if self.request("GET", "/readyz", role=None)[0] == 200:
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.03)
        raise RuntimeError("Server did not become ready")

    def stop(self, crash=False):
        if self.process is not None:
            self.process.send_signal(signal.SIGKILL if crash else signal.SIGINT)
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                raise RuntimeError("Server failed to drain during shutdown")
            finally:
                self.process = None

    def close(self):
        try:
            self.stop()
        finally:
            self.log.close()

    def cli(self, *args, expect_success=True):
        result = subprocess.run([str(self.binary), *map(str, args)], env=self.env,
                                text=True, capture_output=True, timeout=40)
        if expect_success and result.returncode:
            raise AssertionError(result.stderr)
        if not expect_success:
            assert result.returncode != 0, "Expected a failed operation"
            return result
        return json.loads(result.stdout)

    def request(self, method, path, payload=None, role="reader"):
        headers = {}
        if role:
            token = self.write_token if role == "writer" else self.read_token
            headers["Authorization"] = f"Bearer {token}"
        data = None
        if payload is not None:
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read().decode()
            return response.status, json.loads(body) if body.startswith(("{", "[", "null")) else body

    def ok(self, method, path, payload=None, role="reader"):
        status, body = self.request(method, path, payload, role)
        assert status == 200, (status, body)
        return body
