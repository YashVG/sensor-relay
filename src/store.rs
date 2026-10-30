use crate::model::*;
use duckdb::{Connection, OptionalExt, params};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Conflict(String),
    #[error("database operation failed: {0}")]
    Db(#[from] duckdb::Error),
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// One connection, owned exclusively by the storage actor.
pub struct Store {
    conn: Connection,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::from_connection(Connection::open(path)?)
    }

    pub fn memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.execute_batch("SET memory_limit='256MB'; SET threads=2;")?;
        conn.execute_batch(include_str!("schema.sql"))?;
        Ok(Self { conn })
    }

    /// Samples, stream offset, and retry receipt commit together before ACK.
    pub fn ingest(&mut self, batch: &Batch, now: i64) -> Result<Receipt> {
        batch.validate(now).map_err(StoreError::Invalid)?;
        let encoded = serde_json::to_vec(batch).expect("Batch has only JSON-safe scalar types");
        let digest = hex::encode(Sha256::digest(encoded));
        let tx = self.conn.transaction()?;
        let previous: Option<(String, i64)> = tx.query_row(
            "SELECT digest, sample_count FROM receipts WHERE device_id=? AND stream_id=? AND first_seq=?",
            params![batch.device_id, batch.stream_id, batch.first_seq],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        if let Some((old_digest, count)) = previous {
            if old_digest != digest {
                return Err(StoreError::Conflict(
                    "retry changed an already committed batch".into(),
                ));
            }
            return Ok(Receipt {
                status: "duplicate".into(),
                next_seq: batch.first_seq + count,
                samples: count as usize,
            });
        }
        let expected: i64 = tx
            .query_row(
                "SELECT next_seq FROM streams WHERE device_id=? AND stream_id=?",
                params![batch.device_id, batch.stream_id],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if batch.first_seq != expected {
            return Err(StoreError::Conflict(format!(
                "expected first_seq {expected}, got {}",
                batch.first_seq
            )));
        }
        {
            let mut appender = tx.appender("telemetry")?;
            for (index, sample) in batch.samples.iter().enumerate() {
                appender.append_row(params![
                    batch.device_id,
                    batch.stream_id,
                    batch.first_seq + index as i64,
                    sample.observed_ms,
                    sample.power_w,
                    sample.soc_bps as i32
                ])?;
            }
            appender.flush()?;
        }
        let next_seq = batch.first_seq + batch.samples.len() as i64;
        tx.execute("INSERT INTO streams VALUES (?, ?, ?) ON CONFLICT (device_id,stream_id) DO UPDATE SET next_seq=excluded.next_seq",
            params![batch.device_id, batch.stream_id, next_seq])?;
        tx.execute(
            "INSERT INTO receipts VALUES (?, ?, ?, ?, ?)",
            params![
                batch.device_id,
                batch.stream_id,
                batch.first_seq,
                digest,
                batch.samples.len() as i64
            ],
        )?;
        tx.commit()?;
        Ok(Receipt {
            status: "committed".into(),
            next_seq,
            samples: batch.samples.len(),
        })
    }

    pub fn fleet(&self, now: i64, freshness_ms: i64) -> Result<Fleet> {
        if !(1..=86_400_000).contains(&freshness_ms) {
            return Err(StoreError::Invalid(
                "freshness_ms must be 1..86400000".into(),
            ));
        }
        // A late arrival cannot replace a newer event-time observation. Future
        // samples are stored, but do not enter the as-of snapshot prematurely.
        let mut stats = self.conn.query_row(
            "WITH latest AS (
                SELECT *, row_number() OVER (PARTITION BY device_id ORDER BY observed_ms DESC, stream_id DESC, seq DESC) AS rank
                FROM telemetry WHERE observed_ms <= ?
             ) SELECT count(*), count(*) FILTER (WHERE observed_ms >= ?),
                coalesce(sum(power_w) FILTER (WHERE observed_ms >= ?), 0)::BIGINT,
                avg(soc_bps) FILTER (WHERE observed_ms >= ?) FROM latest WHERE rank=1",
            params![now, now-freshness_ms, now-freshness_ms, now-freshness_ms],
            |row| Ok(Fleet { committed_samples: 0, committed_batches: 0, known_devices: row.get(0)?,
                fresh_devices: row.get(1)?, stale_devices: 0, net_power_w: row.get(2)?,
                average_soc_bps: row.get(3)?, as_of_ms: now, freshness_ms }),
        )?;
        stats.committed_samples =
            self.conn
                .query_row("SELECT count(*) FROM telemetry", [], |r| r.get(0))?;
        stats.committed_batches =
            self.conn
                .query_row("SELECT count(*) FROM receipts", [], |r| r.get(0))?;
        stats.stale_devices = stats.known_devices - stats.fresh_devices;
        Ok(stats)
    }

    pub fn windows(&self, from: i64, to: i64, bucket_ms: i64) -> Result<Vec<Window>> {
        if from < 0
            || to <= from
            || to - from > 86_400_000
            || !(1000..=3_600_000).contains(&bucket_ms)
        {
            return Err(StoreError::Invalid(
                "require 0 <= from < to, range <= 24h, bucket_ms 1000..3600000".into(),
            ));
        }
        let mut stmt = self.conn.prepare(
            "SELECT (observed_ms // ?) * ? AS bucket_ms, count(*), count(DISTINCT device_id),
                avg(power_w), min(soc_bps) FROM telemetry
             WHERE observed_ms >= ? AND observed_ms < ? GROUP BY 1 ORDER BY 1",
        )?;
        let rows = stmt.query_map(params![bucket_ms, bucket_ms, from, to], |row| {
            Ok(Window {
                bucket_ms: row.get(0)?,
                samples: row.get(1)?,
                devices: row.get(2)?,
                mean_power_w: row.get(3)?,
                min_soc_bps: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<_, _>>()?)
    }

    pub fn issue_command(&mut self, req: &CommandRequest, now: i64) -> Result<Command> {
        if !valid_id(&req.command_id)
            || !valid_id(&req.device_id)
            || !(-10_000..=10_000).contains(&req.target_w)
        {
            return Err(StoreError::Invalid(
                "invalid command/device ID or target outside +/-10000W".into(),
            ));
        }
        if let Some(old) = self.command_by_id(&req.command_id)? {
            if old.device_id != req.device_id
                || old.target_w != req.target_w
                || old.expires_ms != req.expires_ms
            {
                return Err(StoreError::Conflict(
                    "command ID reused with different payload".into(),
                ));
            }
            return Ok(old);
        }
        if req.expires_ms <= now || req.expires_ms > now.saturating_add(300_000) {
            return Err(StoreError::Invalid(
                "new command must expire within the next five minutes".into(),
            ));
        }
        let last: Option<i64> = self.conn.query_row(
            "SELECT max(observed_ms) FROM telemetry WHERE device_id=? AND observed_ms<=?",
            params![req.device_id, now],
            |r| r.get(0),
        )?;
        if last.is_none_or(|t| now - t > 30_000) {
            return Err(StoreError::Conflict(
                "cannot issue command to a device without fresh telemetry".into(),
            ));
        }
        let generation: i64 = self.conn.query_row(
            "SELECT coalesce(max(generation), 0)+1 FROM commands WHERE device_id=?",
            params![req.device_id],
            |r| r.get(0),
        )?;
        self.conn.execute(
            "INSERT INTO commands VALUES (?, ?, ?, ?, ?, false)",
            params![
                req.command_id,
                req.device_id,
                req.target_w,
                req.expires_ms,
                generation
            ],
        )?;
        Ok(Command {
            command_id: req.command_id.clone(),
            device_id: req.device_id.clone(),
            target_w: req.target_w,
            expires_ms: req.expires_ms,
            generation,
            acknowledged: false,
        })
    }

    fn command_by_id(&self, id: &str) -> Result<Option<Command>> {
        Ok(self
            .conn
            .query_row(
                "SELECT * FROM commands WHERE command_id=?",
                params![id],
                read_command,
            )
            .optional()?)
    }

    pub fn current_command(&self, device: &str, now: i64) -> Result<Option<Command>> {
        let command = self
            .conn
            .query_row(
                "SELECT * FROM commands WHERE device_id=? ORDER BY generation DESC LIMIT 1",
                params![device],
                read_command,
            )
            .optional()?;
        Ok(command.filter(|c| c.expires_ms > now))
    }

    pub fn ack_command(&mut self, device: &str, id: &str, now: i64) -> Result<Command> {
        let mut command = self
            .current_command(device, now)?
            .filter(|c| c.command_id == id)
            .ok_or_else(|| {
                StoreError::Conflict("command expired, superseded, or unknown".into())
            })?;
        self.conn.execute(
            "UPDATE commands SET acknowledged=true WHERE command_id=?",
            params![id],
        )?;
        command.acknowledged = true;
        Ok(command)
    }
}

fn read_command(row: &duckdb::Row<'_>) -> duckdb::Result<Command> {
    Ok(Command {
        command_id: row.get(0)?,
        device_id: row.get(1)?,
        target_w: row.get(2)?,
        expires_ms: row.get(3)?,
        generation: row.get(4)?,
        acknowledged: row.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_after_appending_samples_rolls_back_data_and_checkpoint() {
        let mut store = Store::memory().unwrap();
        // Inject a storage failure at the final insert, after samples and the
        // checkpoint have already been written within the transaction.
        store
            .conn
            .execute_batch(
                "DROP TABLE receipts;
            CREATE TABLE receipts (
                device_id VARCHAR, stream_id VARCHAR, first_seq BIGINT,
                digest VARCHAR, sample_count BIGINT CHECK(sample_count = 1),
                PRIMARY KEY(device_id, stream_id, first_seq)
            );",
            )
            .unwrap();
        let sample = Sample {
            observed_ms: 1000,
            power_w: 10,
            soc_bps: 5000,
        };
        let mut batch = Batch {
            device_id: "a".into(),
            stream_id: "boot".into(),
            first_seq: 0,
            samples: vec![sample.clone(), sample],
        };
        assert!(matches!(store.ingest(&batch, 1000), Err(StoreError::Db(_))));
        assert_eq!(store.fleet(1000, 100).unwrap().committed_samples, 0);
        let streams: i64 = store
            .conn
            .query_row("SELECT count(*) FROM streams", [], |r| r.get(0))
            .unwrap();
        assert_eq!(streams, 0);
        batch.samples.pop();
        assert_eq!(store.ingest(&batch, 1000).unwrap().next_seq, 1);
    }
}
