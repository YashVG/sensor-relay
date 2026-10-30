use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use sensor_relay::{
    edge::{self, Outbox},
    model::*,
    server,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(version, about = "Experiments with reliable sensor streams in Rust")]
struct Cli {
    #[command(subcommand)]
    command: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Start the single-writer telemetry and analytics service.
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: SocketAddr,
        #[arg(long, default_value = "data/readings.duckdb")]
        db: PathBuf,
        #[arg(long, default_value_t = 64)]
        queue_capacity: usize,
        #[arg(long, env = "SENSOR_RELAY_WRITE_TOKEN", hide_env_values = true)]
        write_token: String,
        #[arg(long, env = "SENSOR_RELAY_READ_TOKEN", hide_env_values = true)]
        read_token: String,
    },
    /// Stage synthetic telemetry durably, without needing a server.
    Stage {
        #[arg(long, default_value = "data/edge.sqlite")]
        spool: PathBuf,
        #[arg(long, default_value_t = 8)]
        devices: usize,
        #[arg(long, default_value_t = 4)]
        batches: usize,
        #[arg(long, default_value_t = 64)]
        batch_size: usize,
        #[arg(long, default_value_t = 1024)]
        max_pending: usize,
    },
    /// Send pending batches in order, retrying ambiguous outcomes.
    Replay {
        #[arg(long, default_value = "data/edge.sqlite")]
        spool: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        url: String,
        #[arg(long, env = "SENSOR_RELAY_WRITE_TOKEN", hide_env_values = true)]
        write_token: String,
        #[arg(long, default_value_t = 10)]
        attempts: usize,
    },
    /// Stream one simulated sensor; Ctrl-C leaves unsent data on disk.
    Stream {
        #[arg(long)]
        id: String,
        #[arg(long)]
        spool: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        url: String,
        #[arg(long, env = "SENSOR_RELAY_WRITE_TOKEN", hide_env_values = true)]
        write_token: String,
        #[arg(long, default_value_t = 30)]
        ticks: usize,
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
    },
}

fn parent(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    match Cli::parse().command {
        Action::Serve {
            bind,
            db,
            queue_capacity,
            write_token,
            read_token,
        } => {
            parent(&db)?;
            let server::Server { router, worker } =
                server::server(db, write_token, read_token, queue_capacity)?;
            let listener = tokio::net::TcpListener::bind(bind).await?;
            eprintln!("Sensor Relay listening on {}", listener.local_addr()?);
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
            tokio::task::spawn_blocking(move || worker.join())
                .await?
                .map_err(|_| anyhow::anyhow!("storage worker panicked"))?;
        }
        Action::Stage {
            spool,
            devices,
            batches,
            batch_size,
            max_pending,
        } => {
            ensure!(
                (1..=10_000).contains(&devices)
                    && (1..=MAX_BATCH).contains(&batch_size)
                    && batches > 0,
                "invalid generation limits"
            );
            parent(&spool)?;
            let mut outbox = Outbox::open(spool, max_pending)?;
            for round in 0..batches {
                for device in 0..devices {
                    outbox.enqueue(
                        &format!("sensor-{device:04}"),
                        edge::synthetic_samples(
                            batch_size,
                            now_ms(),
                            (round * devices + device) as u64,
                        ),
                    )?;
                }
            }
            println!(
                "{}",
                serde_json::json!({"pending_batches": outbox.pending()?})
            );
        }
        Action::Replay {
            spool,
            url,
            write_token,
            attempts,
        } => {
            let outbox = Outbox::open(spool, 1024)?;
            println!(
                "{}",
                serde_json::to_string(&edge::replay(&outbox, &url, &write_token, attempts).await?)?
            );
        }
        Action::Stream {
            id,
            spool,
            url,
            write_token,
            ticks,
            interval_ms,
        } => {
            ensure!(
                valid_id(&id) && interval_ms > 0 && ticks > 0,
                "invalid device options"
            );
            parent(&spool)?;
            let mut outbox = Outbox::open(spool, 1024)?;
            for tick in 0..ticks {
                outbox.enqueue(&id, edge::synthetic_samples(10, now_ms(), tick as u64))?;
                match edge::replay(&outbox, &url, &write_token, 1).await {
                    Ok(report) => {
                        println!("{}", serde_json::to_string(&report)?);
                    }
                    Err(error) => eprintln!("{error}"),
                }
                tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            }
            ensure!(
                outbox.pending()? == 0,
                "device stopped with pending data; use replay after recovery"
            );
        }
    }
    Ok(())
}
