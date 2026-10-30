use sensor_relay::{
    edge::{Outbox, synthetic_samples},
    model::*,
    store::{Store, StoreError},
};

fn batch(device: &str, seq: i64) -> Batch {
    Batch {
        device_id: device.into(),
        stream_id: "boot-1".into(),
        first_seq: seq,
        samples: vec![Sample {
            observed_ms: 1000,
            temperature_mc: 25_000,
            battery_bps: 5000,
        }],
    }
}

#[test]
fn identical_retry_is_idempotent_and_changed_retry_conflicts() {
    let mut store = Store::memory().unwrap();
    let mut b = batch("a", 0);
    assert_eq!(store.ingest(&b, 1000).unwrap().status, "committed");
    assert_eq!(store.ingest(&b, 1000).unwrap().status, "duplicate");
    b.samples[0].temperature_mc = 999;
    assert!(matches!(
        store.ingest(&b, 1000),
        Err(StoreError::Conflict(_))
    ));
    let snapshot = store.snapshot(1000, 1000).unwrap();
    assert_eq!(snapshot.committed_samples, 1);
    assert_eq!(snapshot.mean_temperature_mc, Some(25_000.0));
}

#[test]
fn sequence_gap_and_overlap_do_not_advance_checkpoint() {
    let mut store = Store::memory().unwrap();
    assert!(store.ingest(&batch("a", 1), 1000).is_err());
    let mut b = batch("a", 0);
    b.samples.push(b.samples[0].clone());
    store.ingest(&b, 1000).unwrap();
    assert!(store.ingest(&batch("a", 1), 1000).is_err());
    assert!(store.ingest(&batch("a", 3), 1000).is_err());
    assert_eq!(store.ingest(&batch("a", 2), 1000).unwrap().next_seq, 3);
}

#[test]
fn invalid_batch_is_atomic_and_corrected_batch_can_commit() {
    let mut store = Store::memory().unwrap();
    let mut b = batch("a", 0);
    b.samples.push(Sample {
        observed_ms: 1000,
        temperature_mc: 10,
        battery_bps: 10001,
    });
    assert!(store.ingest(&b, 1000).is_err());
    assert_eq!(store.snapshot(1000, 100).unwrap().committed_samples, 0);
    b.samples[1].battery_bps = 10000;
    assert_eq!(store.ingest(&b, 1000).unwrap().next_seq, 2);
}

#[test]
fn checkpoint_and_retry_receipt_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("readings.duckdb");
    {
        let mut store = Store::open(&path).unwrap();
        store.ingest(&batch("a", 0), 1000).unwrap();
    }
    let mut store = Store::open(&path).unwrap();
    assert_eq!(
        store.ingest(&batch("a", 0), 1000).unwrap().status,
        "duplicate"
    );
    assert_eq!(store.ingest(&batch("a", 1), 1000).unwrap().next_seq, 2);
}

#[test]
fn stream_reset_is_explicit_and_does_not_collide_with_previous_boot() {
    let mut store = Store::memory().unwrap();
    let b = batch("a", 0);
    store.ingest(&b, 1000).unwrap();
    let mut next_boot = b.clone();
    next_boot.stream_id = "boot-2".into();
    store.ingest(&next_boot, 1000).unwrap();
    let snapshot = store.snapshot(1000, 100).unwrap();
    assert_eq!(snapshot.committed_samples, 2);
    assert_eq!(snapshot.known_devices, 1);
    assert_eq!(snapshot.mean_temperature_mc, Some(25_000.0));
}

#[test]
fn event_time_snapshot_ignores_late_older_and_not_yet_current_samples() {
    let mut store = Store::memory().unwrap();
    store.ingest(&batch("a", 0), 1000).unwrap();
    let mut late = batch("a", 1);
    late.samples[0].observed_ms = 500;
    late.samples[0].temperature_mc = 36_000;
    store.ingest(&late, 1000).unwrap();
    let mut future = batch("a", 2);
    future.samples[0].observed_ms = 1100;
    future.samples[0].temperature_mc = 27_000;
    store.ingest(&future, 1000).unwrap();
    assert_eq!(
        store.snapshot(1000, 100).unwrap().mean_temperature_mc,
        Some(25_000.0)
    );
    assert_eq!(
        store.snapshot(1100, 100).unwrap().mean_temperature_mc,
        Some(27_000.0)
    );
    let stale = store.snapshot(2000, 100).unwrap();
    assert_eq!(stale.stale_devices, 1);
    assert_eq!(stale.mean_temperature_mc, None);
    assert!(stale.average_battery_bps.is_none());
}

#[test]
fn snapshot_uses_one_latest_sample_per_device_not_every_sample() {
    let mut store = Store::memory().unwrap();
    store.ingest(&batch("a", 0), 1000).unwrap();
    store.ingest(&batch("a", 1), 1000).unwrap();
    let mut b = batch("b", 0);
    b.samples[0].temperature_mc = 16_000;
    store.ingest(&b, 1000).unwrap();
    let snapshot = store.snapshot(1000, 100).unwrap();
    assert_eq!(snapshot.committed_samples, 3);
    assert_eq!(snapshot.fresh_devices, 2);
    assert_eq!(snapshot.mean_temperature_mc, Some(20_500.0));
}

#[test]
fn duckdb_windows_use_half_open_event_time_bounds() {
    let mut store = Store::memory().unwrap();
    let mut b = batch("a", 0);
    b.samples = vec![
        Sample {
            observed_ms: 999,
            temperature_mc: 100,
            battery_bps: 5000,
        },
        Sample {
            observed_ms: 1000,
            temperature_mc: 300,
            battery_bps: 4000,
        },
        Sample {
            observed_ms: 2000,
            temperature_mc: 500,
            battery_bps: 3000,
        },
    ];
    store.ingest(&b, 2000).unwrap();
    let windows = store.windows(0, 2000, 1000).unwrap();
    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].bucket_ms, 0);
    assert_eq!(windows[1].mean_temperature_mc, 300.0);
    assert!(store.windows(0, 86_400_001, 1000).is_err());
    assert!(store.windows(0, 2000, 0).is_err());
}

#[test]
fn validation_bounds_cover_overflow_future_payload_size_and_ids() {
    let mut b = batch("a", i64::MAX);
    assert!(b.validate(1000).is_err());
    b.first_seq = -1;
    assert!(b.validate(1000).is_err());
    b.first_seq = 0;
    b.samples[0].temperature_mc = i64::MIN;
    assert!(b.validate(1000).is_err());
    b.samples[0].temperature_mc = MAX_TEMPERATURE_MC + 1;
    assert!(b.validate(1000).is_err());
    b.samples[0].temperature_mc = MIN_TEMPERATURE_MC - 1;
    assert!(b.validate(1000).is_err());
    b.samples[0].temperature_mc = MIN_TEMPERATURE_MC;
    assert!(b.validate(1000).is_ok());
    b.samples[0].temperature_mc = MAX_TEMPERATURE_MC;
    assert!(b.validate(1000).is_ok());
    b.samples[0].temperature_mc = 10;
    b.samples[0].observed_ms = 61001;
    assert!(b.validate(1000).is_err());
    b.samples = vec![];
    assert!(b.validate(1000).is_err());
    b.samples = synthetic_samples(MAX_BATCH + 1, 1_000_000, 0);
    assert!(b.validate(1_000_000).is_err());
    assert!(!valid_id("../device"));
    assert!(!valid_id(&"a".repeat(65)));
    assert!(valid_id("sensor-0001"));
}

#[test]
fn outbox_restarts_with_stable_identity_and_only_removes_valid_receipts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("edge.sqlite");
    let original;
    {
        let mut outbox = Outbox::open(&path, 1).unwrap();
        original = outbox.enqueue("a", batch("a", 0).samples).unwrap();
        assert!(outbox.enqueue("a", vec![]).is_err());
    }
    let mut outbox = Outbox::open(&path, 1).unwrap();
    let (id, retry) = outbox.next().unwrap().unwrap();
    assert_eq!(retry.stream_id, original.stream_id);
    let mut receipt = Receipt {
        status: "committed".into(),
        next_seq: 999,
        samples: 1,
    };
    assert!(outbox.acknowledge(id, &retry, &receipt).is_err());
    assert_eq!(outbox.pending().unwrap(), 1);
    receipt.next_seq = 1;
    outbox.acknowledge(id, &retry, &receipt).unwrap();
    assert_eq!(outbox.pending().unwrap(), 0);
    let second = outbox.enqueue("a", batch("a", 0).samples).unwrap();
    assert_eq!(second.first_seq, 1);
    assert_eq!(second.stream_id, original.stream_id);
}

#[test]
fn outbox_backpressure_preserves_all_pending_data() {
    let dir = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::open(dir.path().join("edge.sqlite"), 1).unwrap();
    outbox.enqueue("a", batch("a", 0).samples).unwrap();
    assert!(outbox.enqueue("a", batch("a", 0).samples).is_err());
    assert_eq!(outbox.pending().unwrap(), 1);
    assert_eq!(outbox.next().unwrap().unwrap().1.first_seq, 0);
}
