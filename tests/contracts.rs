use gridstream::{
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
            power_w: 250,
            soc_bps: 5000,
        }],
    }
}

#[test]
fn identical_retry_is_idempotent_and_changed_retry_conflicts() {
    let mut store = Store::memory().unwrap();
    let mut b = batch("a", 0);
    assert_eq!(store.ingest(&b, 1000).unwrap().status, "committed");
    assert_eq!(store.ingest(&b, 1000).unwrap().status, "duplicate");
    b.samples[0].power_w = 999;
    assert!(matches!(
        store.ingest(&b, 1000),
        Err(StoreError::Conflict(_))
    ));
    let fleet = store.fleet(1000, 1000).unwrap();
    assert_eq!(fleet.committed_samples, 1);
    assert_eq!(fleet.net_power_w, 250);
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
        power_w: 10,
        soc_bps: 10001,
    });
    assert!(store.ingest(&b, 1000).is_err());
    assert_eq!(store.fleet(1000, 100).unwrap().committed_samples, 0);
    b.samples[1].soc_bps = 10000;
    assert_eq!(store.ingest(&b, 1000).unwrap().next_seq, 2);
}

#[test]
fn checkpoint_and_retry_receipt_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fleet.duckdb");
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
    let fleet = store.fleet(1000, 100).unwrap();
    assert_eq!(fleet.committed_samples, 2);
    assert_eq!(fleet.known_devices, 1);
    assert_eq!(fleet.net_power_w, 250);
}

#[test]
fn event_time_snapshot_ignores_late_older_and_not_yet_current_samples() {
    let mut store = Store::memory().unwrap();
    store.ingest(&batch("a", 0), 1000).unwrap();
    let mut late = batch("a", 1);
    late.samples[0].observed_ms = 500;
    late.samples[0].power_w = 900;
    store.ingest(&late, 1000).unwrap();
    let mut future = batch("a", 2);
    future.samples[0].observed_ms = 1100;
    future.samples[0].power_w = 700;
    store.ingest(&future, 1000).unwrap();
    assert_eq!(store.fleet(1000, 100).unwrap().net_power_w, 250);
    assert_eq!(store.fleet(1100, 100).unwrap().net_power_w, 700);
    let stale = store.fleet(2000, 100).unwrap();
    assert_eq!(stale.stale_devices, 1);
    assert_eq!(stale.net_power_w, 0);
    assert!(stale.average_soc_bps.is_none());
}

#[test]
fn fleet_uses_one_latest_sample_per_device_not_every_sample() {
    let mut store = Store::memory().unwrap();
    store.ingest(&batch("a", 0), 1000).unwrap();
    store.ingest(&batch("a", 1), 1000).unwrap();
    let mut b = batch("b", 0);
    b.samples[0].power_w = -100;
    store.ingest(&b, 1000).unwrap();
    let fleet = store.fleet(1000, 100).unwrap();
    assert_eq!(fleet.committed_samples, 3);
    assert_eq!(fleet.fresh_devices, 2);
    assert_eq!(fleet.net_power_w, 150);
}

#[test]
fn duckdb_windows_use_half_open_event_time_bounds() {
    let mut store = Store::memory().unwrap();
    let mut b = batch("a", 0);
    b.samples = vec![
        Sample {
            observed_ms: 999,
            power_w: 100,
            soc_bps: 5000,
        },
        Sample {
            observed_ms: 1000,
            power_w: 300,
            soc_bps: 4000,
        },
        Sample {
            observed_ms: 2000,
            power_w: 500,
            soc_bps: 3000,
        },
    ];
    store.ingest(&b, 2000).unwrap();
    let windows = store.windows(0, 2000, 1000).unwrap();
    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].bucket_ms, 0);
    assert_eq!(windows[1].mean_power_w, 300.0);
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
    b.samples[0].power_w = i64::MIN;
    assert!(b.validate(1000).is_err());
    b.samples[0].power_w = 10;
    b.samples[0].observed_ms = 61001;
    assert!(b.validate(1000).is_err());
    b.samples = vec![];
    assert!(b.validate(1000).is_err());
    b.samples = synthetic_samples(MAX_BATCH + 1, 1_000_000, 0);
    assert!(b.validate(1_000_000).is_err());
    assert!(!valid_id("../device"));
    assert!(!valid_id(&"a".repeat(65)));
    assert!(valid_id("battery-0001"));
}

fn command(id: &str, expiration: i64) -> CommandRequest {
    CommandRequest {
        command_id: id.into(),
        device_id: "a".into(),
        target_w: 1000,
        expires_ms: expiration,
    }
}

#[test]
fn commands_require_fresh_device_expire_and_fence_superseded_acknowledgements() {
    let mut store = Store::memory().unwrap();
    assert!(store.issue_command(&command("c1", 5000), 1000).is_err());
    store.ingest(&batch("a", 0), 1000).unwrap();
    let first = store.issue_command(&command("c1", 5000), 1000).unwrap();
    assert_eq!(first.generation, 1);
    assert_eq!(
        store
            .issue_command(&command("c1", 5000), 1000)
            .unwrap()
            .generation,
        1
    );
    assert!(store.issue_command(&command("c1", 6000), 1000).is_err());
    assert_eq!(
        store
            .issue_command(&command("c2", 5000), 1000)
            .unwrap()
            .generation,
        2
    );
    assert!(store.ack_command("a", "c1", 1000).is_err());
    assert!(store.ack_command("a", "c2", 1000).unwrap().acknowledged);
    assert!(store.ack_command("a", "c2", 1000).unwrap().acknowledged);
    assert!(store.current_command("a", 5000).unwrap().is_none());
    assert!(store.ack_command("a", "c2", 5000).is_err());
    assert!(store.issue_command(&command("c3", 40_000), 32_000).is_err());
}

#[test]
fn command_limits_and_generation_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fleet.duckdb");
    {
        let mut s = Store::open(&path).unwrap();
        s.ingest(&batch("a", 0), 1000).unwrap();
        s.issue_command(&command("c1", 5000), 1000).unwrap();
    }
    let mut s = Store::open(&path).unwrap();
    assert_eq!(
        s.issue_command(&command("c2", 5000), 1000)
            .unwrap()
            .generation,
        2
    );
    let mut bad = command("bad", 5000);
    bad.target_w = 10001;
    assert!(s.issue_command(&bad, 1000).is_err());
    bad.target_w = 0;
    bad.expires_ms = 1000;
    assert!(s.issue_command(&bad, 1000).is_err());
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

#[test]
fn simulated_command_application_is_persistent_and_rejects_expiry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("edge.sqlite");
    let command = Command {
        command_id: "c1".into(),
        device_id: "a".into(),
        target_w: 1000,
        expires_ms: 5000,
        generation: 1,
        acknowledged: false,
    };
    {
        let mut outbox = Outbox::open(&path, 2).unwrap();
        assert!(outbox.apply_command(&command, 1000).unwrap());
    }
    let mut outbox = Outbox::open(&path, 2).unwrap();
    assert!(!outbox.apply_command(&command, 1000).unwrap());
    let mut next = command.clone();
    next.generation = 2;
    next.command_id = "c2".into();
    assert!(outbox.apply_command(&next, 1000).unwrap());
    assert!(!outbox.apply_command(&command, 1000).unwrap());
    assert!(outbox.apply_command(&next, 5000).is_err());
}
