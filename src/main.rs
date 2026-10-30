use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use gridstream::{
    edge::{self, Outbox},
    model::*,
    server,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};

#[derive(Parser)]
#[command(version, about = "Durable energy telemetry and DuckDB analytics lab")]
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
        #[arg(long, default_value = "data/fleet.duckdb")]
        db: PathBuf,
        #[arg(long, default_value_t = 64)]
        queue_capacity: usize,
        #[arg(long, env = "GRIDSTREAM_DEVICE_TOKEN", hide_env_values = true)]
        device_token: String,
        #[arg(long, env = "GRIDSTREAM_OPERATOR_TOKEN", hide_env_values = true)]
        operator_token: String,
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
        #[arg(long, env = "GRIDSTREAM_DEVICE_TOKEN", hide_env_values = true)]
        device_token: String,
        #[arg(long, default_value_t = 10)]
        attempts: usize,
    },
    /// Stream one synthetic device; Ctrl-C leaves unsent data on disk.
    Device {
        #[arg(long)]
        id: String,
        #[arg(long)]
        spool: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        url: String,
        #[arg(long, env = "GRIDSTREAM_DEVICE_TOKEN", hide_env_values = true)]
        device_token: String,
        #[arg(long, default_value_t = 30)]
        ticks: usize,
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
    },
    /// Apply current commands to a local simulated setpoint and ACK them.
    Poll {
        #[arg(long, default_value = "data/edge.sqlite")]
        spool: PathBuf,
        #[arg(long, default_value = "http://127.0.0.1:8080")]
        url: String,
        #[arg(long, env = "GRIDSTREAM_DEVICE_TOKEN", hide_env_values = true)]
        device_token: String,
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
            device_token,
            operator_token,
        } => {
            parent(&db)?;
            let server::Server { router, worker } =
                server::server(db, device_token, operator_token, queue_capacity)?;
            let listener = tokio::net::TcpListener::bind(bind).await?;
            eprintln!("GridStream listening on {}", listener.local_addr()?);
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
                        &format!("battery-{device:04}"),
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
            device_token,
            attempts,
        } => {
            let outbox = Outbox::open(spool, 1024)?;
            println!(
                "{}",
                serde_json::to_string(
                    &edge::replay(&outbox, &url, &device_token, attempts).await?
                )?
            );
        }
        Action::Device {
            id,
            spool,
            url,
            device_token,
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
                match edge::replay(&outbox, &url, &device_token, 1).await {
                    Ok(report) => {
                        println!("{}", serde_json::to_string(&report)?);
                        if let Err(error) =
                            edge::poll_commands(&mut outbox, &url, &device_token).await
                        {
                            eprintln!("command poll: {error}");
                        }
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
        Action::Poll {
            spool,
            url,
            device_token,
        } => {
            let mut outbox = Outbox::open(spool, 1024)?;
            println!(
                "{}",
                serde_json::json!({"new_simulated_applications": edge::poll_commands(&mut outbox, &url, &device_token).await?})
            );
        }
    }
    Ok(())
}
