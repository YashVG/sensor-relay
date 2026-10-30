use crate::{
    model::*,
    store::{Store, StoreError},
};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    path::Path as FilePath,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc, oneshot};

type WorkResult = Result<Value, StoreError>;
enum Operation {
    Ingest(Batch),
    Snapshot(i64),
    Windows(i64, i64, i64),
    Health,
}
struct Work {
    operation: Operation,
    reply: oneshot::Sender<WorkResult>,
}

#[derive(Default)]
struct Metrics {
    committed: AtomicU64,
    duplicates: AtomicU64,
    overloaded: AtomicU64,
    errors: AtomicU64,
}

#[derive(Clone)]
struct AppState {
    queue: mpsc::Sender<Work>,
    write_token: Arc<str>,
    read_token: Arc<str>,
    metrics: Arc<Metrics>,
    admission: Arc<Semaphore>,
}

pub struct Server {
    pub router: Router,
    pub worker: thread::JoinHandle<()>,
}

pub fn server(
    db: impl AsRef<FilePath>,
    write_token: String,
    read_token: String,
    queue_capacity: usize,
) -> anyhow::Result<Server> {
    anyhow::ensure!(
        write_token.len() >= 16 && read_token.len() >= 16,
        "tokens must have at least 16 characters"
    );
    anyhow::ensure!(
        write_token != read_token,
        "write and read tokens must differ"
    );
    anyhow::ensure!(
        (1..=4096).contains(&queue_capacity),
        "queue capacity must be 1..4096"
    );
    let mut store = Store::open(db)?;
    let (queue, mut receiver) = mpsc::channel::<Work>(queue_capacity);
    let metrics = Arc::new(Metrics::default());
    let worker_metrics = metrics.clone();
    let worker = thread::Builder::new()
        .name("duckdb-owner".into())
        .spawn(move || {
            while let Some(work) = receiver.blocking_recv() {
                let now = now_ms();
                let result = match work.operation {
                    Operation::Ingest(batch) => store.ingest(&batch, now).map(|receipt| {
                        if receipt.status == "committed" {
                            worker_metrics
                                .committed
                                .fetch_add(receipt.samples as u64, Ordering::Relaxed);
                        } else {
                            worker_metrics.duplicates.fetch_add(1, Ordering::Relaxed);
                        }
                        json!(receipt)
                    }),
                    Operation::Snapshot(freshness) => {
                        store.snapshot(now, freshness).map(|v| json!(v))
                    }
                    Operation::Windows(from, to, bucket) => {
                        store.windows(from, to, bucket).map(|v| json!(v))
                    }
                    Operation::Health => Ok(json!({"ready":true})),
                };
                if result.is_err() {
                    worker_metrics.errors.fetch_add(1, Ordering::Relaxed);
                }
                // A caller can time out after the commit. Dropping its reply is safe:
                // the next identical request finds the transaction's retry receipt.
                let _ = work.reply.send(result);
            }
        })?;
    let state = AppState {
        queue,
        write_token: write_token.into(),
        read_token: read_token.into(),
        metrics,
        admission: Arc::new(Semaphore::new(queue_capacity + 4)),
    };
    let router = Router::new()
        .route("/v1/telemetry", post(ingest))
        .route("/v1/snapshot", get(snapshot))
        .route("/v1/windows", get(windows))
        .route("/metrics", get(metrics_handler))
        .route("/readyz", get(ready))
        .layer(DefaultBodyLimit::max(128 * 1024))
        .layer(middleware::from_fn_with_state(state.clone(), admission))
        .with_state(state);
    Ok(Server { router, worker })
}

struct ApiError(StatusCode, String);
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.0, Json(json!({"error":self.1}))).into_response();
        if self.0 == StatusCode::SERVICE_UNAVAILABLE {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
        }
        response
    }
}

fn overloaded() -> ApiError {
    ApiError(
        StatusCode::SERVICE_UNAVAILABLE,
        "capacity exhausted; retry the identical batch".into(),
    )
}

async fn admission(State(state): State<AppState>, request: Request, next: Next) -> Response {
    let Ok(_permit) = state.admission.clone().try_acquire_owned() else {
        state.metrics.overloaded.fetch_add(1, Ordering::Relaxed);
        return overloaded().into_response();
    };
    next.run(request).await
}

fn authorize(headers: &HeaderMap, token: &str) -> Result<(), ApiError> {
    let expected = format!("Bearer {token}");
    if headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        == Some(expected.as_str())
    {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid bearer token".into(),
        ))
    }
}

async fn submit(state: &AppState, operation: Operation) -> Result<Json<Value>, ApiError> {
    let (reply, receive) = oneshot::channel();
    state
        .queue
        .try_send(Work { operation, reply })
        .map_err(|_| {
            state.metrics.overloaded.fetch_add(1, Ordering::Relaxed);
            overloaded()
        })?;
    match tokio::time::timeout(Duration::from_secs(10), receive).await {
        Ok(Ok(Ok(value))) => Ok(Json(value)),
        Ok(Ok(Err(StoreError::Invalid(message)))) => {
            Err(ApiError(StatusCode::BAD_REQUEST, message))
        }
        Ok(Ok(Err(StoreError::Conflict(message)))) => Err(ApiError(StatusCode::CONFLICT, message)),
        Ok(Ok(Err(error))) => {
            tracing::error!(%error, "storage operation failed");
            Err(ApiError(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation failed; retry identical request".into(),
            ))
        }
        Ok(Err(_)) => Err(overloaded()),
        Err(_) => Err(ApiError(
            StatusCode::GATEWAY_TIMEOUT,
            "commit outcome unknown; retry identical request".into(),
        )),
    }
}

async fn ingest(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.write_token)?;
    batch
        .validate(now_ms())
        .map_err(|e| ApiError(StatusCode::BAD_REQUEST, e))?;
    submit(&state, Operation::Ingest(batch)).await
}

#[derive(Deserialize)]
struct SnapshotQuery {
    #[serde(default = "default_freshness")]
    freshness_ms: i64,
}
fn default_freshness() -> i64 {
    30_000
}
async fn snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SnapshotQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.read_token)?;
    submit(&state, Operation::Snapshot(query.freshness_ms)).await
}
#[derive(Deserialize)]
struct WindowQuery {
    from_ms: i64,
    to_ms: i64,
    #[serde(default = "default_bucket")]
    bucket_ms: i64,
}
fn default_bucket() -> i64 {
    60_000
}
async fn windows(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<WindowQuery>,
) -> Result<Json<Value>, ApiError> {
    authorize(&headers, &state.read_token)?;
    submit(
        &state,
        Operation::Windows(query.from_ms, query.to_ms, query.bucket_ms),
    )
    .await
}
async fn ready(State(state): State<AppState>) -> Result<Json<Value>, ApiError> {
    submit(&state, Operation::Health).await
}
async fn metrics_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    authorize(&headers, &state.read_token)?;
    let m = &state.metrics;
    Ok((
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        format!(
            "# TYPE sensor_relay_committed_samples_total counter\nsensor_relay_committed_samples_total {}\n# TYPE sensor_relay_duplicate_batches_total counter\nsensor_relay_duplicate_batches_total {}\n# TYPE sensor_relay_overloaded_total counter\nsensor_relay_overloaded_total {}\n# TYPE sensor_relay_storage_errors_total counter\nsensor_relay_storage_errors_total {}\n# TYPE sensor_relay_queue_depth gauge\nsensor_relay_queue_depth {}\n",
            m.committed.load(Ordering::Relaxed),
            m.duplicates.load(Ordering::Relaxed),
            m.overloaded.load(Ordering::Relaxed),
            m.errors.load(Ordering::Relaxed),
            state.queue.max_capacity() - state.queue.capacity()
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn full_queue_returns_retryable_503() {
        let (queue, _receiver) = mpsc::channel(1);
        let (reply, _receive) = oneshot::channel();
        queue
            .try_send(Work {
                operation: Operation::Health,
                reply,
            })
            .unwrap_or_else(|_| panic!("empty queue"));
        let state = AppState {
            queue,
            write_token: "device".into(),
            read_token: "reader".into(),
            metrics: Arc::default(),
            admission: Arc::new(Semaphore::new(1)),
        };
        let error = submit(&state, Operation::Health).await.err().unwrap();
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "1");
    }
}
