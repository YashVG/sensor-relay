use anyhow::{Result, bail, ensure};
use clap::Parser;
use gridstream::{
    edge::{client, synthetic_samples},
    model::*,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use uuid::Uuid;

#[derive(Clone, Parser)]
#[command(about = "Measure HTTP-to-DuckDB commit latency with synthetic devices; output JSON")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    url: String,
    #[arg(long, env = "GRIDSTREAM_DEVICE_TOKEN", hide_env_values = true)]
    token: String,
    #[arg(long, default_value_t = 64)]
    devices: usize,
    #[arg(long, default_value_t = 8)]
    batches: usize,
    #[arg(long, default_value_t = 64)]
    batch_size: usize,
    #[arg(long, default_value_t = 16)]
    concurrency: usize,
    #[arg(long, default_value_t = 4)]
    replay_every: usize,
}

#[derive(Default)]
struct Counters {
    attempts: usize,
    overloaded: usize,
    duplicates: usize,
}
async fn send(
    http: &reqwest::Client,
    args: &Args,
    batch: &Batch,
    counts: &mut Counters,
) -> Result<Receipt> {
    for attempt in 0..100 {
        counts.attempts += 1;
        let response = http
            .post(format!("{}/v1/telemetry", args.url.trim_end_matches('/')))
            .bearer_auth(&args.token)
            .json(batch)
            .send()
            .await?;
        if response.status().is_success() {
            return Ok(response.json().await?);
        }
        if response.status().as_u16() != 503 {
            bail!(
                "load request failed: {} {}",
                response.status(),
                response.text().await?
            );
        }
        counts.overloaded += 1;
        tokio::time::sleep(Duration::from_millis(20 + (attempt as u64).min(20) * 10)).await;
    }
    bail!("load request exhausted retry budget")
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    ensure!(
        (1..=10_000).contains(&args.devices)
            && (1..=MAX_BATCH).contains(&args.batch_size)
            && (1..=1000).contains(&args.batches)
            && (1..=1024).contains(&args.concurrency),
        "invalid workload limits"
    );
    let run_id = Uuid::new_v4().to_string();
    let semaphore = Arc::new(Semaphore::new(args.concurrency));
    let http = client()?;
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for device in 0..args.devices {
        let args = args.clone();
        let http = http.clone();
        let semaphore = semaphore.clone();
        let stream_id = run_id.clone();
        tasks.spawn(async move {
            let _permit = semaphore.acquire().await?;
            let mut counts = Counters::default();
            let mut latencies = Vec::new();
            for round in 0..args.batches {
                let batch = Batch {
                    device_id: format!("load-{device:05}"),
                    stream_id: stream_id.clone(),
                    first_seq: (round * args.batch_size) as i64,
                    samples: synthetic_samples(
                        args.batch_size,
                        now_ms(),
                        (device * args.batches + round) as u64,
                    ),
                };
                let began = Instant::now();
                let receipt = send(&http, &args, &batch, &mut counts).await?;
                latencies.push(began.elapsed().as_secs_f64() * 1000.0);
                ensure!(
                    receipt.status == "committed"
                        && receipt.next_seq == batch.first_seq + args.batch_size as i64,
                    "unexpected commit receipt"
                );
                if args.replay_every > 0 && round % args.replay_every == 0 {
                    let duplicate = send(&http, &args, &batch, &mut counts).await?;
                    ensure!(
                        duplicate.status == "duplicate",
                        "retry was not deduplicated"
                    );
                    counts.duplicates += 1;
                }
            }
            Ok::<_, anyhow::Error>((counts, latencies))
        });
    }
    let mut counts = Counters::default();
    let mut latencies = Vec::new();
    while let Some(task) = tasks.join_next().await {
        let (c, l) = task??;
        counts.attempts += c.attempts;
        counts.overloaded += c.overloaded;
        counts.duplicates += c.duplicates;
        latencies.extend(l);
    }
    let elapsed = started.elapsed().as_secs_f64();
    latencies.sort_by(f64::total_cmp);
    let percentile =
        |p: f64| latencies[((latencies.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    let samples = args.devices * args.batches * args.batch_size;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "run_id":run_id,"devices":args.devices,"batches_per_device":args.batches,"batch_size":args.batch_size,
            "concurrency":args.concurrency,"unique_samples":samples,"committed_batches":latencies.len(),"http_attempts":counts.attempts,
            "duplicate_receipts":counts.duplicates,"overloaded_responses":counts.overloaded,
            "elapsed_seconds":elapsed,"unique_samples_per_second":samples as f64/elapsed,
            "batch_commit_latency_ms":{"p50":percentile(0.50),"p95":percentile(0.95),"p99":percentile(0.99)},
            "latency_scope":"HTTP serialization, network, queue, DuckDB commit, ACK; includes 503 retry waits; excludes explicit duplicate probes",
            "throughput_scope":"unique committed samples / wall time, including duplicate probes",
            "os":std::env::consts::OS,"architecture":std::env::consts::ARCH
        }))?
    );
    Ok(())
}
