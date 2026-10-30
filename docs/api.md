# Run a sensor and collector

First build with `./scripts/cargo.sh build --release --locked --bins`.

## Terminal 1: collector

```sh
export SENSOR_RELAY_WRITE_TOKEN="$(openssl rand -hex 24)"
export SENSOR_RELAY_READ_TOKEN="$(openssl rand -hex 24)"
target/release/sensor-relay serve
```

The collector listens on `127.0.0.1:8080` and saves readings to `data/readings.duckdb`.

## Terminal 2: readings

Set the **same values** for those two environment variables in this terminal. Keep them out of committed files. Then stage a few batches and send them:

```sh
target/release/sensor-relay stage --devices 2 --batches 3 --batch-size 8
target/release/sensor-relay replay
curl -s -H "Authorization: Bearer $SENSOR_RELAY_READ_TOKEN" \
  http://127.0.0.1:8080/v1/snapshot | python3 -m json.tool
```

With a fresh database, this saves 48 readings. Try stopping the collector before `stage`: staging still works, and `replay` retains pending data until the collector returns. The default outbox is `data/edge.sqlite`.

To generate readings continuously, give each simulated sensor its own outbox:

```sh
target/release/sensor-relay stream --id desk-sensor --spool data/desk.sqlite --ticks 60
```

`stream` generates ten readings per tick, with a default interval of one second. A simulated sample has a temperature in millidegrees Celsius (`23500` = 23.5 °C) and a battery level in basis points (`6500` = 65%).

## HTTP interface

All timestamps are Unix milliseconds. Requests use `Authorization: Bearer <token>` except `/readyz`.

| Method and path | Token | Response |
| --- | --- | --- |
| `POST /v1/telemetry` | Write | Commit or duplicate receipt |
| `GET /v1/snapshot?freshness_ms=30000` | Read | Counts and averages over the latest fresh sensor readings |
| `GET /v1/windows?from_ms=…&to_ms=…&bucket_ms=60000` | Read | Average temperature and minimum battery level per time bucket |
| `GET /metrics` | Read | Prometheus-format counters and queue depth |
| `GET /readyz` | None | Readiness through the storage mailbox |

Example batch shape:

```json
{
  "device_id": "desk-sensor",
  "stream_id": "example-stream",
  "first_seq": 0,
  "samples": [
    {"observed_ms": 1000, "temperature_mc": 23500, "battery_bps": 6500}
  ]
}
```

The example timestamp is historical; use current time for a fresh snapshot. IDs allow 1–64 ASCII letters, digits, `_`, and `-`. Every stream starts at sequence zero and advances by batch length. Batches contain 1–256 samples; bodies are limited to 128 KiB. Temperatures must fall within −40 to 125 °C, battery levels within 0–100%, and timestamps no more than 60 seconds ahead of server time.

A receipt contains `status` (`committed` or `duplicate`), `next_seq`, and `samples`. Conflicting identities or sequences return `409`; invalid values return `400`; malformed JSON shapes return `422`. Retry server failures, `503`, `504`, and ambiguous transport outcomes with the identical batch. `503` includes `Retry-After: 1`; the example edge uses its own capped exponential backoff and jitter. Permanent failures keep the batch for inspection and stop replay.
