use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gridstream::{model::*, server::server};
use serde_json::{Value, json};
use tower::ServiceExt;

const DEVICE: &str = "test-device-secret-12345";
const OPERATOR: &str = "test-operator-secret-12345";
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
    "samples":[{"observed_ms":now_ms(),"power_w":1200,"soc_bps":5000}]})
}

#[tokio::test]
async fn http_auth_commit_retry_query_and_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let service = server(dir.path().join("db"), DEVICE.into(), OPERATOR.into(), 16).unwrap();
    let app = service.router;
    let b = batch();
    let response = app
        .clone()
        .oneshot(request("POST", "/v1/telemetry", OPERATOR, b.clone()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    for status in ["committed", "duplicate"] {
        let response = app
            .clone()
            .oneshot(request("POST", "/v1/telemetry", DEVICE, b.clone()))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let receipt: Receipt =
            serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
        assert_eq!(receipt.status, status);
    }
    let mut changed = b;
    changed["samples"][0]["power_w"] = json!(77);
    assert_eq!(
        app.clone()
            .oneshot(request("POST", "/v1/telemetry", DEVICE, changed))
            .await
            .unwrap()
            .status(),
        StatusCode::CONFLICT
    );
    let response = app
        .clone()
        .oneshot(request("GET", "/v1/fleet", OPERATOR, json!(null)))
        .await
        .unwrap();
    let fleet: Fleet =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap();
    assert_eq!(fleet.committed_samples, 1);
    assert_eq!(fleet.net_power_w, 1200);
    assert_eq!(
        app.clone()
            .oneshot(request("GET", "/v1/fleet", DEVICE, json!(null)))
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
    let service = server(dir.path().join("db"), DEVICE.into(), OPERATOR.into(), 32).unwrap();
    let app = service.router;
    let b = batch();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let app = app.clone();
        let b = b.clone();
        tasks.spawn(async move {
            let response = app
                .oneshot(request("POST", "/v1/telemetry", DEVICE, b))
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
    let service = server(dir.path().join("db"), DEVICE.into(), OPERATOR.into(), 4).unwrap();
    let app = service.router;
    let mut b = batch();
    b["samples"][0]["soc_bps"] = json!(10001);
    assert_eq!(
        app.clone()
            .oneshot(request("POST", "/v1/telemetry", DEVICE, b))
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
                DEVICE,
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
