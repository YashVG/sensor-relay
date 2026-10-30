# API and interactive use

Build with `./scripts/cargo.sh build --release --locked --bins`. Run the following in one terminal:

```sh
export GRIDSTREAM_DEVICE_TOKEN="$(openssl rand -hex 24)"
export GRIDSTREAM_OPERATOR_TOKEN="$(openssl rand -hex 24)"
target/release/gridstream serve
```

In a second terminal, set the **same values** for those environment variables. Keep them out of committed files. Then stage and replay synthetic telemetry:

```sh
target/release/gridstream stage --devices 8 --batches 4 --batch-size 64
target/release/gridstream replay
curl -s -H "Authorization: Bearer $GRIDSTREAM_OPERATOR_TOKEN" \
  http://127.0.0.1:8080/v1/fleet
```

Staging works while the server is stopped. Restart the server and run `replay` to recover pending data. Default paths are `data/fleet.duckdb` and `data/edge.sqlite`. Each live edge needs its own spool:

```sh
target/release/gridstream device --id battery-live --spool data/live.sqlite --ticks 60
```

## Endpoints

All times are Unix milliseconds. Except `/readyz`, requests require `Authorization: Bearer <token>`.

| Method and path | Role | Result |
| --- | --- | --- |
| `POST /v1/telemetry` | Device | Commit or duplicate receipt |
| `GET /v1/fleet?freshness_ms=30000` | Operator | Durable counts and latest fresh fleet snapshot |
| `GET /v1/windows?from_ms=…&to_ms=…&bucket_ms=60000` | Operator | Sample-weighted event-time buckets |
| `POST /v1/commands` | Operator | Create command or return identical existing command |
| `GET /v1/devices/{device}/command` | Device | Latest unexpired command, or JSON `null` |
| `POST /v1/devices/{device}/commands/{id}/ack` | Device | Acknowledge current unexpired command |
| `GET /metrics` | Operator | Prometheus-format counters and queue depth |
| `GET /readyz` | None | Readiness through the storage mailbox |

Telemetry body:

```json
{
  "device_id": "battery-0000",
  "stream_id": "example-stream",
  "first_seq": 0,
  "samples": [
    {"observed_ms": 1000, "power_w": 1200, "soc_bps": 6500}
  ]
}
```

Positive watts mean export/discharge; negative means import/charge. Charge uses basis points (`6500` = 65%). The example timestamp is historical and will be stale; use current time for a fresh snapshot. IDs allow 1–64 ASCII letters, digits, `_`, and `-`. Batches contain 1–256 samples; request bodies are limited to 128 KiB. Each stream starts at zero and advances by batch length.

Receipts contain `status` (`committed` or `duplicate`), `next_seq`, and `samples`. Validation errors return `400` (or `422` for JSON type/shape errors); invalid credentials return `401`; identity/sequence conflicts return `409`. Retry `503`, `504`, other server failures, and ambiguous transport outcomes with the identical batch. `503` includes `Retry-After: 1`; the lab edge currently uses its own capped exponential backoff with jitter. Permanent failures keep the batch for inspection and stop replay.

## Issue a simulated command

After sending fresh telemetry for `battery-0000`, generate an expiry and issue a command:

```sh
expires_ms=$(python3 -c 'import time; print(int(time.time()*1000)+60000)')
curl -s -X POST http://127.0.0.1:8080/v1/commands \
  -H "Authorization: Bearer $GRIDSTREAM_OPERATOR_TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{\"command_id\":\"dispatch-1\",\"device_id\":\"battery-0000\",\"target_w\":1200,\"expires_ms\":$expires_ms}"
target/release/gridstream poll
```

Targets are limited to ±10,000 W. Use a new command ID for a new request. `poll` records the local simulated application and acknowledges it; repeating it reports zero new applications. A live `device` also polls after successful telemetry replay. See [command limitations](architecture.md#simulated-commands).
