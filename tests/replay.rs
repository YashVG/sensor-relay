use axum::{
    Json, Router,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use sensor_relay::{
    edge::{Outbox, replay, synthetic_samples},
    model::now_ms,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type Reply = (StatusCode, Option<&'static str>);

#[derive(Default)]
struct MockState {
    replies: VecDeque<Reply>,
    requests: Vec<(Instant, Value)>,
}

struct Collector {
    url: String,
    state: Arc<Mutex<MockState>>,
    task: tokio::task::JoinHandle<()>,
}

impl Collector {
    async fn start(replies: impl IntoIterator<Item = Reply>) -> Self {
        let state = Arc::new(Mutex::new(MockState {
            replies: replies.into_iter().collect(),
            ..Default::default()
        }));
        let router = Router::new()
            .route("/v1/telemetry", post(respond))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, state, task }
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn respond(State(state): State<Arc<Mutex<MockState>>>, Json(batch): Json<Value>) -> Response {
    let mut state = state.lock().unwrap();
    state.requests.push((Instant::now(), batch.clone()));
    let (status, retry_after) = state.replies.pop_front().unwrap_or((StatusCode::OK, None));
    let samples = batch["samples"].as_array().unwrap().len();
    let body = if status.is_success() {
        json!({"status":"committed", "samples":samples,
            "next_seq":batch["first_seq"].as_i64().unwrap() + samples as i64})
    } else {
        json!({"error":"test rejection"})
    };
    let mut response = (status, Json(body)).into_response();
    if let Some(value) = retry_after {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, value.parse().unwrap());
    }
    response
}

#[tokio::test]
async fn replay_respects_retry_after_and_resends_identical_batch() {
    for status in [
        StatusCode::SERVICE_UNAVAILABLE,
        StatusCode::TOO_MANY_REQUESTS,
    ] {
        let collector = Collector::start([(status, Some("1"))]).await;
        let dir = tempfile::tempdir().unwrap();
        let mut outbox = Outbox::open(dir.path().join("edge.sqlite"), 2).unwrap();
        outbox
            .enqueue("sensor", synthetic_samples(2, now_ms(), 0))
            .unwrap();
        let report = tokio::time::timeout(
            Duration::from_secs(10),
            replay(&outbox, &collector.url, "test-token", 2),
        )
        .await
        .unwrap()
        .unwrap();
        let state = collector.state.lock().unwrap();
        assert_eq!(state.requests.len(), 2);
        assert!(
            state.requests[1].0.duration_since(state.requests[0].0) >= Duration::from_secs(1),
            "retried {status} before Retry-After elapsed"
        );
        assert_eq!(state.requests[0].1, state.requests[1].1);
        assert_eq!((report.batches, report.samples, report.retries), (1, 2, 1));
        assert_eq!(outbox.pending().unwrap(), 0);
    }
}

#[tokio::test]
async fn exhausted_retry_budget_retains_batch_across_reopen() {
    let collector = Collector::start([(StatusCode::SERVICE_UNAVAILABLE, Some("0")); 2]).await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("edge.sqlite");
    let mut outbox = Outbox::open(&path, 2).unwrap();
    let original = outbox
        .enqueue("sensor", synthetic_samples(2, now_ms(), 0))
        .unwrap();
    let error = replay(&outbox, &collector.url, "test-token", 2)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("retry budget exhausted"));
    assert_eq!(collector.state.lock().unwrap().requests.len(), 2);
    drop(outbox);
    let outbox = Outbox::open(&path, 2).unwrap();
    assert_eq!(
        serde_json::to_value(outbox.next().unwrap().unwrap().1).unwrap(),
        serde_json::to_value(original).unwrap()
    );
    assert_eq!(outbox.pending().unwrap(), 1);
    let report = replay(&outbox, &collector.url, "test-token", 1)
        .await
        .unwrap();
    assert_eq!((report.batches, report.retries), (1, 0));
    assert_eq!(outbox.pending().unwrap(), 0);
}

#[tokio::test]
async fn permanent_rejections_do_not_retry_or_discard_the_batch() {
    for status in [StatusCode::UNAUTHORIZED, StatusCode::CONFLICT] {
        let collector = Collector::start([(status, Some("1"))]).await;
        let dir = tempfile::tempdir().unwrap();
        let mut outbox = Outbox::open(dir.path().join("edge.sqlite"), 1).unwrap();
        outbox
            .enqueue("sensor", synthetic_samples(1, now_ms(), 0))
            .unwrap();
        let error = replay(&outbox, &collector.url, "test-token", 3)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(&status.to_string()));
        assert_eq!(collector.state.lock().unwrap().requests.len(), 1);
        assert_eq!(outbox.pending().unwrap(), 1);
    }
}

#[tokio::test]
async fn excessive_retry_after_stops_replay_with_batch_retained() {
    let collector = Collector::start([(StatusCode::SERVICE_UNAVAILABLE, Some("86400"))]).await;
    let dir = tempfile::tempdir().unwrap();
    let mut outbox = Outbox::open(dir.path().join("edge.sqlite"), 1).unwrap();
    outbox
        .enqueue("sensor", synthetic_samples(1, now_ms(), 0))
        .unwrap();
    let error = tokio::time::timeout(
        Duration::from_secs(5),
        replay(&outbox, &collector.url, "test-token", 2),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("Retry-After exceeds 60 seconds"));
    assert_eq!(collector.state.lock().unwrap().requests.len(), 1);
    assert_eq!(outbox.pending().unwrap(), 1);
}
