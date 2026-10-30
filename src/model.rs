use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MAX_BATCH: usize = 256;
pub const MIN_TEMPERATURE_MC: i64 = -40_000;
pub const MAX_TEMPERATURE_MC: i64 = 125_000;

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
    /// Temperature in thousandths of a degree Celsius: 23500 means 23.5 C.
    pub temperature_mc: i64,
    /// Battery state of charge, in basis points: 0..=10000.
    pub battery_bps: u16,
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
            if !(MIN_TEMPERATURE_MC..=MAX_TEMPERATURE_MC).contains(&sample.temperature_mc)
                || sample.battery_bps > 10_000
            {
                return Err(
                    "temperature must be -40..125 C and battery must be 0..100 percent".into(),
                );
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
pub struct Snapshot {
    pub committed_samples: i64,
    pub committed_batches: i64,
    pub known_devices: i64,
    pub fresh_devices: i64,
    pub stale_devices: i64,
    pub mean_temperature_mc: Option<f64>,
    pub average_battery_bps: Option<f64>,
    pub as_of_ms: i64,
    pub freshness_ms: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Window {
    pub bucket_ms: i64,
    pub samples: i64,
    pub devices: i64,
    /// Mean over all readings in this window; faster sensors contribute more.
    pub mean_temperature_mc: f64,
    pub min_battery_bps: u16,
}
