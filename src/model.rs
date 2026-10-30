use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_BATCH: usize = 256;
pub const MAX_POWER_W: i64 = 1_000_000;

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    pub observed_ms: i64,
    /// Positive = export/discharge, negative = import/charge. Synthetic watts.
    pub power_w: i64,
    /// Battery state of charge, in basis points: 0..=10000.
    pub soc_bps: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub device_id: String,
    pub stream_id: String,
    pub first_seq: i64,
    pub samples: Vec<Sample>,
}

impl Batch {
    pub fn validate(&self, now: i64) -> Result<(), String> {
        if !valid_id(&self.device_id) || !valid_id(&self.stream_id) {
            return Err(
                "device_id and stream_id must be 1..64 ASCII letters, digits, '_' or '-'".into(),
            );
        }
        if self.samples.is_empty() || self.samples.len() > MAX_BATCH {
            return Err(format!("batch must contain 1..={MAX_BATCH} samples"));
        }
        if self.first_seq < 0
            || self
                .first_seq
                .checked_add(self.samples.len() as i64)
                .is_none()
        {
            return Err("sequence out of range".into());
        }
        for sample in &self.samples {
            if sample.observed_ms < 0 || sample.observed_ms > now.saturating_add(60_000) {
                return Err("timestamp is negative or more than 60 seconds in the future".into());
            }
            if !(-MAX_POWER_W..=MAX_POWER_W).contains(&sample.power_w) || sample.soc_bps > 10_000 {
                return Err("sample outside the simulator's power or charge bounds".into());
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Receipt {
    pub status: String,
    pub next_seq: i64,
    pub samples: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Fleet {
    pub committed_samples: i64,
    pub committed_batches: i64,
    pub known_devices: i64,
    pub fresh_devices: i64,
    pub stale_devices: i64,
    pub net_power_w: i64,
    pub average_soc_bps: Option<f64>,
    pub as_of_ms: i64,
    pub freshness_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Window {
    pub bucket_ms: i64,
    pub samples: i64,
    pub devices: i64,
    /// Sample-weighted mean, NOT time-integrated energy or simultaneous fleet power.
    pub mean_power_w: f64,
    pub min_soc_bps: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandRequest {
    pub command_id: String,
    pub device_id: String,
    pub target_w: i64,
    pub expires_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Command {
    pub command_id: String,
    pub device_id: String,
    pub target_w: i64,
    pub expires_ms: i64,
    pub generation: i64,
    pub acknowledged: bool,
}
