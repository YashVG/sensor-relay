use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use sensor_relay::{model::*, server::server};
use serde_json::{Value, json};
use tower::ServiceExt;

const WRITE: &str = "test-device-secret-12345";
const READ: &str = "test-reader-secret-12345";
fn request(method: &str, path: &str, token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}
fn batch() -> Value {
    json!({"device_id":"battery","stream_id":"boot","first_seq":0,
    "samples":[{"observed_ms":now_ms(),"temperature_mc":22000,"battery_bps":5000}]})
}

#[tokio::test]
async fn http_auth_commit_retry_query_and_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let service = server(dir.path().join("db"), WRITE.into(), READ.into(), 16).unwrap();
    let app = service.router;
    let b = batch();
    let response = app
        .clone()
        .oneshot(request("POST", "/v1/telemetry", READ, b.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    for status in ["committed", "duplicate"] {
        let response = app
            .clone()
            .oneshot(request("POST", "/v1/telemetry", WRITE, b.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let receipt: Receipt =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(receipt.status, status);
    }
    let mut changed = b;
    changed["samples"][0]["temperature_mc"] = json!(77);
    assert_eq!(
        app.clone()
            .oneshot(request("POST", "/v1/telemetry", WRITE, changed))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let response = app
        .clone()
        .oneshot(request("GET", "/v1/snapshot", READ, json!(null)))
        .await
        .unwrap();
    let snapshot: Snapshot =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(snapshot.committed_samples, 1);
    assert_eq!(snapshot.mean_temperature_mc, Some(22000.0));
    assert_eq!(
        app.clone()
            .oneshot(request("GET", "/v1/snapshot", WRITE, json!(null)))
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    drop(app);
    service.worker.join().unwrap();
}

#[tokio::test]
async fn simultaneous_retries_commit_only_one_batch() {
    let dir = tempfile::tempdir().unwrap();
    let service = server(dir.path().join("db"), WRITE.into(), READ.into(), 32).unwrap();
    let app = service.router;
    let b = batch();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let app = app.clone();
        let b = b.clone();
        tasks.spawn(async move {
            let response = app
                .oneshot(request("POST", "/v1/telemetry", WRITE, b))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            serde_json::from_slice::<Receipt>(&to_bytes(response.into_body(), 4096).await.unwrap())
                .unwrap()
                .status
        });
    }
    let mut commits = 0;
    while let Some(task) = tasks.join_next().await {
        commits += usize::from(task.unwrap() == "committed");
    }
    assert_eq!(commits, 1);
    drop(app);
    service.worker.join().unwrap();
}

#[tokio::test]
async fn body_limit_and_bad_samples_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let service = server(dir.path().join("db"), WRITE.into(), READ.into(), 4).unwrap();
    let app = service.router;
    let mut b = batch();
    b["samples"][0]["battery_bps"] = json!(10001);
    assert_eq!(
        app.clone()
            .oneshot(request("POST", "/v1/telemetry", WRITE, b))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        app.clone()
            .oneshot(request(
                "POST",
                "/v1/telemetry",
                WRITE,
                json!({"padding":"a".repeat(140_000)})
            ))
            .await
            .unwrap()
            .status(),
        StatusCode::PAYLOAD_TOO_LARGE
    );
    drop(app);
    service.worker.join().unwrap();
}
