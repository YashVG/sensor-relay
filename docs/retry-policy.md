# Replay retry policy

Replay waits for the longer of its local backoff and the server's `Retry-After` delay, then adds jitter. The pending SQLite batch keeps its identity and content until a valid commit or duplicate receipt arrives.

## Why this changed

The collector already returned `503` with `Retry-After: 1` when its queue filled. The edge ignored that header and could resend after 100–199 ms. Under sustained overload, those early retries could consume the attempt budget while the collector was still busy.

The edge now accepts both non-negative whole seconds and HTTP dates, as defined by [HTTP Semantics](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after). It applies the header to retryable responses (`429` and `5xx`). Transport failures, missing or malformed headers, and dates in the past use local backoff.

## Bounds and tradeoffs

- Local backoff starts at 100 ms, doubles per retry, and caps at 5 seconds. Jitter adds 0–99 ms.
- A valid server delay is a minimum, even when local backoff is shorter. Total sleep is bounded at 60 seconds.
- A server delay above 60 seconds stops replay with an error and leaves the batch queued. It does not shorten the requested delay and retry early. Run replay later after investigating the response.
- The attempt budget includes the initial request. Exhausting it leaves pending data on disk, with no final sleep.
- Permanent rejections such as `401` or `409` stop immediately. A `Retry-After` header does not make them retryable.

HTTP dates rely on the client's wall clock. This policy bounds each wait, not the total replay duration. It does not coordinate retry schedules across clients or improve the collector's storage throughput.

## Verify

```sh
./scripts/cargo.sh test --locked --test replay
./scripts/cargo.sh test --locked edge::tests
```

The HTTP tests run a loopback collector and exercise the actual replay client. They check the one-second delay for both `503` and `429`, unchanged retry payloads, attempt exhaustion followed by reopening the outbox, permanent rejections, and excessive delays. Unit tests cover date parsing, invalid headers, overflow, and backoff bounds without long waits.
