use crate::model::*;
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{path::Path, time::Duration};
use uuid::Uuid;

/// SQLite is a local transport outbox, not the fleet analytics database.
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
            CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS applied_commands (device_id TEXT PRIMARY KEY, generation INTEGER NOT NULL, payload TEXT NOT NULL);")?;
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

    pub fn devices(&self) -> Result<Vec<String>> {
        Ok(self
            .conn
            .prepare("SELECT device_id FROM devices ORDER BY device_id")?
            .query_map([], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?)
    }

    /// Records a simulated setpoint only. No physical device is controlled.
    pub fn apply_command(&mut self, command: &Command, now: i64) -> Result<bool> {
        ensure!(
            command.expires_ms > now
                && command.generation > 0
                && (-10_000..=10_000).contains(&command.target_w),
            "expired or invalid command"
        );
        let tx = self
            .conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let existing: Option<(i64, String)> = tx
            .query_row(
                "SELECT generation,payload FROM applied_commands WHERE device_id=?",
                params![command.device_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((generation, text)) = existing {
            if generation > command.generation {
                return Ok(false);
            }
            if generation == command.generation {
                let old: Command = serde_json::from_str(&text)?;
                ensure!(
                    old.command_id == command.command_id
                        && old.target_w == command.target_w
                        && old.expires_ms == command.expires_ms,
                    "same generation has conflicting command content"
                );
                return Ok(false);
            }
        }
        tx.execute("INSERT INTO applied_commands VALUES (?, ?, ?) ON CONFLICT(device_id) DO UPDATE SET generation=excluded.generation,payload=excluded.payload",
            params![command.device_id, command.generation, serde_json::to_string(command)?])?;
        tx.commit()?;
        Ok(true)
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
                _ => {
                    if attempt + 1 < attempts {
                        report.retries += 1;
                        let delay = (100_u64 * 2_u64.pow(attempt.min(6) as u32)).min(5_000)
                            + (Uuid::new_v4().as_u128() % 100) as u64;
                        tokio::time::sleep(Duration::from_millis(delay)).await;
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

pub async fn poll_commands(outbox: &mut Outbox, base: &str, token: &str) -> Result<usize> {
    let http = client()?;
    let base = base.trim_end_matches('/');
    let mut applied = 0;
    for device in outbox.devices()? {
        let command: Option<Command> = http
            .get(format!("{base}/v1/devices/{device}/command"))
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if let Some(command) = command {
            ensure!(
                command.device_id == device,
                "command addressed to wrong device"
            );
            applied += usize::from(outbox.apply_command(&command, now_ms())?);
            http.post(format!(
                "{base}/v1/devices/{device}/commands/{}/ack",
                command.command_id
            ))
            .bearer_auth(token)
            .send()
            .await?
            .error_for_status()?;
        }
    }
    Ok(applied)
}

/// Reproducible synthetic samples; no hardware or Tesla data.
pub fn synthetic_samples(count: usize, end_ms: i64, seed: u64) -> Vec<Sample> {
    (0..count)
        .map(|i| Sample {
            observed_ms: end_ms - (count - i - 1) as i64 * 100,
            power_w: ((seed.wrapping_mul(37).wrapping_add(i as u64 * 137)) % 10_000) as i64 - 5000,
            soc_bps: 4000 + (seed % 2000) as u16,
        })
        .collect()
}
