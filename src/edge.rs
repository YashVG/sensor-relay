use crate::model::*;
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    time::{Duration, SystemTime},
};
use uuid::Uuid;

/// SQLite stores the batches waiting to be sent.
pub struct Outbox {
    conn: Connection,
    max_pending: usize,
}

impl Outbox {
    pub fn open(path: impl AsRef<Path>, max_pending: usize) -> Result<Self> {
        ensure!(max_pending > 0, "max_pending must be positive");
        let conn = Connection::open(path)?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS devices (device_id TEXT PRIMARY KEY, stream_id TEXT NOT NULL, next_seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);")?;
        Ok(Self { conn, max_pending })
    }

    pub fn pending(&self) -> Result<usize> {
        Ok(self
            .conn
            .query_row("SELECT count(*) FROM outbox", [], |r| r.get(0))?)
    }

    pub fn enqueue(&mut self, device: &str, samples: Vec<Sample>) -> Result<Batch> {
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let pending: usize = tx.query_row("SELECT count(*) FROM outbox", [], |r| r.get(0))?;
        ensure!(
            pending < self.max_pending,
            "outbox full; capture paused without discarding pending batches"
        );
        let state: Option<(String, i64)> = tx
            .query_row(
                "SELECT stream_id,next_seq FROM devices WHERE device_id=?",
                params![device],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let (stream_id, first_seq) = state.unwrap_or_else(|| (Uuid::new_v4().to_string(), 0));
        let batch = Batch {
            device_id: device.into(),
            stream_id,
            first_seq,
            samples,
        };
        batch.validate(now_ms()).map_err(anyhow::Error::msg)?;
        tx.execute(
            "INSERT INTO outbox(payload) VALUES (?)",
            params![serde_json::to_string(&batch)?],
        )?;
        tx.execute("INSERT INTO devices VALUES (?, ?, ?) ON CONFLICT(device_id) DO UPDATE SET next_seq=excluded.next_seq",
            params![batch.device_id, batch.stream_id, batch.first_seq + batch.samples.len() as i64])?;
        tx.commit()?;
        Ok(batch)
    }

    pub fn next(&self) -> Result<Option<(i64, Batch)>> {
        let row: Option<(i64, String)> = self
            .conn
            .query_row(
                "SELECT id,payload FROM outbox ORDER BY id LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        row.map(|(id, text)| Ok((id, serde_json::from_str(&text)?)))
            .transpose()
    }

    pub fn acknowledge(&self, id: i64, batch: &Batch, receipt: &Receipt) -> Result<()> {
        ensure!(
            ["committed", "duplicate"].contains(&receipt.status.as_str())
                && receipt.next_seq == batch.first_seq + batch.samples.len() as i64
                && receipt.samples == batch.samples.len(),
            "invalid server receipt; outbox retained"
        );
        // Guard against deleting the wrong queued record, including caller bugs.
        self.conn.execute(
            "DELETE FROM outbox WHERE id=? AND payload=?",
            params![id, serde_json::to_string(batch)?],
        )?;
        Ok(())
    }
}

pub fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .redirect(reqwest::redirect::Policy::none())
        .build()?)
}

#[derive(Debug, Default, serde::Serialize)]
pub struct ReplayReport {
    pub batches: usize,
    pub samples: usize,
    pub duplicates: usize,
    pub retries: usize,
}

const MAX_RETRY_WAIT: Duration = Duration::from_secs(60);

fn retry_after_delay(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) {
        // An overflowing integer is still a valid request to wait too long,
        // not a malformed header that should trigger an early retry.
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|date| date.duration_since(now).unwrap_or_default())
}

fn retry_delay(attempt: usize, retry_after: Option<&str>, now: SystemTime) -> Result<Duration> {
    let server_delay = retry_after
        .and_then(|value| retry_after_delay(value, now))
        .unwrap_or_default();
    ensure!(
        server_delay <= MAX_RETRY_WAIT,
        "Retry-After exceeds 60 seconds; replay stopped and queued batch retained; retry later"
    );
    let backoff = Duration::from_millis((100_u64 * 2_u64.pow(attempt.min(6) as u32)).min(5_000));
    let jitter = Duration::from_millis((Uuid::new_v4().as_u128() % 100) as u64);
    Ok((backoff.max(server_delay) + jitter).min(MAX_RETRY_WAIT))
}

pub async fn replay(
    outbox: &Outbox,
    base: &str,
    token: &str,
    attempts: usize,
) -> Result<ReplayReport> {
    ensure!(attempts > 0, "attempts must be positive");
    let http = client()?;
    let mut report = ReplayReport::default();
    while let Some((id, batch)) = outbox.next()? {
        let mut accepted = false;
        for attempt in 0..attempts {
            let result = http
                .post(format!("{}/v1/telemetry", base.trim_end_matches('/')))
                .bearer_auth(token)
                .json(&batch)
                .send()
                .await;
            match result {
                Ok(response) if response.status().is_success() => {
                    let receipt: Receipt = response
                        .json()
                        .await
                        .context("invalid ACK; queued batch retained")?;
                    outbox.acknowledge(id, &batch, &receipt)?;
                    report.batches += 1;
                    report.samples += batch.samples.len();
                    report.duplicates += usize::from(receipt.status == "duplicate");
                    accepted = true;
                    break;
                }
                Ok(response)
                    if !response.status().is_server_error()
                        && response.status().as_u16() != 429 =>
                {
                    bail!(
                        "server rejected batch with {}; outbox retained: {}",
                        response.status(),
                        response.text().await?
                    );
                }
                retryable => {
                    if attempt + 1 < attempts {
                        let retry_after = retryable
                            .as_ref()
                            .ok()
                            .and_then(|response| {
                                response.headers().get(reqwest::header::RETRY_AFTER)
                            })
                            .and_then(|value| value.to_str().ok());
                        let delay = retry_delay(attempt, retry_after, SystemTime::now())?;
                        // Release the HTTP response before waiting for another attempt.
                        drop(retryable);
                        report.retries += 1;
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
        ensure!(
            accepted,
            "retry budget exhausted; unacknowledged batches remain in the outbox"
        );
    }
    Ok(report)
}

/// Reproducible readings from simulated temperature sensors.
pub fn synthetic_samples(count: usize, end_ms: i64, seed: u64) -> Vec<Sample> {
    (0..count)
        .map(|i| Sample {
            observed_ms: end_ms - (count - i - 1) as i64 * 100,
            temperature_mc: 18_000
                + ((seed.wrapping_mul(37).wrapping_add(i as u64 * 137)) % 12_000) as i64,
            battery_bps: 6000 + (seed % 4000) as u16,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::UNIX_EPOCH;

    #[test]
    fn retry_after_accepts_seconds_and_dates() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        assert_eq!(retry_after_delay(" 2 ", now), Some(Duration::from_secs(2)));
        assert_eq!(retry_after_delay("0", now), Some(Duration::ZERO));
        let future = httpdate::fmt_http_date(now + Duration::from_secs(2));
        let past = httpdate::fmt_http_date(now - Duration::from_secs(2));
        assert_eq!(
            retry_after_delay(&future, now),
            Some(Duration::from_secs(2))
        );
        assert_eq!(retry_after_delay(&past, now), Some(Duration::ZERO));
        let delay = retry_delay(0, Some(&future), now).unwrap();
        assert!((Duration::from_secs(2)..Duration::from_millis(2_100)).contains(&delay));
    }

    #[test]
    fn invalid_or_missing_retry_after_uses_local_backoff() {
        for value in [
            None,
            Some(""),
            Some("-1"),
            Some("+1"),
            Some("1.5"),
            Some("1, 2"),
            Some("tomorrow"),
        ] {
            let delay = retry_delay(0, value, UNIX_EPOCH).unwrap();
            assert!((Duration::from_millis(100)..Duration::from_millis(200)).contains(&delay));
        }
    }

    #[test]
    fn server_delay_never_shortens_backoff_and_waits_are_bounded() {
        for attempt in [6, usize::MAX] {
            let delay = retry_delay(attempt, Some("1"), UNIX_EPOCH).unwrap();
            assert!((Duration::from_secs(5)..Duration::from_millis(5_100)).contains(&delay));
        }
        assert_eq!(
            retry_delay(0, Some("60"), UNIX_EPOCH).unwrap(),
            MAX_RETRY_WAIT
        );
    }

    #[test]
    fn excessive_server_delays_stop_instead_of_retrying_early() {
        for value in [
            "61",
            "86400",
            "18446744073709551616",
            "9999999999999999999999999999999999999999",
        ] {
            assert!(retry_delay(0, Some(value), UNIX_EPOCH).is_err());
        }
        let future = httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_secs(61));
        assert!(retry_delay(0, Some(&future), UNIX_EPOCH).is_err());
    }
}
