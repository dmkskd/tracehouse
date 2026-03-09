//! Integration tests for the CORS proxy.
//!
//! Mirrors the TypeScript proxy tests in packages/proxy/src/__tests__/proxy.integration.test.ts.
//!
//! Tests that don't need ClickHouse run unconditionally (echo server tests).
//! Tests that need ClickHouse check CH_TEST_URL or default to localhost:8123.

use std::future::IntoFuture;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::http::{HeaderMap, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::any;
use axum::Router;
use tokio::net::TcpListener;

/// Start the proxy on an ephemeral port, return base URL.
async fn start_proxy() -> String {
    let app = tracehouse::build_router();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(axum::serve(listener, app).into_future());
    format!("http://127.0.0.1:{port}")
}

/// Captured request data from the echo server.
#[derive(Clone, Default)]
struct CapturedRequest {
    headers: HeaderMap,
    url: String,
}

/// Start an echo server that records incoming headers + URL.
/// Returns (base_url, captured_data).
async fn start_echo_server() -> (String, Arc<Mutex<CapturedRequest>>) {
    let captured = Arc::new(Mutex::new(CapturedRequest::default()));
    let captured_clone = captured.clone();

    let app = Router::new().fallback(any(
        move |req: Request<Body>| {
            let captured = captured_clone.clone();
            async move {
                let mut data = captured.lock().unwrap();
                data.headers = req.headers().clone();
                data.url = req.uri().to_string();
                (StatusCode::OK, "OK").into_response()
            }
        },
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(axum::serve(listener, app).into_future());
    (format!("http://127.0.0.1:{port}"), captured)
}

fn echo_headers(echo_port: u16) -> Vec<(&'static str, String)> {
    vec![
        ("x-ch-host", "127.0.0.1".into()),
        ("x-ch-port", echo_port.to_string()),
        ("x-ch-user", "alice".into()),
        ("x-ch-password", "super-secret-123".into()),
        ("x-ch-database", "mydb".into()),
        ("x-ch-secure", "false".into()),
    ]
}

fn extract_echo_port(echo_url: &str) -> u16 {
    echo_url.rsplit(':').next().unwrap().parse().unwrap()
}

// =========================================================================
// Health checks
// =========================================================================

#[tokio::test]
async fn ping_returns_ok() {
    let base = start_proxy().await;
    let resp = reqwest::get(format!("{base}/proxy/ping")).await.unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
async fn health_returns_ok() {
    let base = start_proxy().await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    assert!(resp.status().is_success());
    let body: serde_json::Value = resp.json::<serde_json::Value>().await.unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["service"], "tracehouse");
}

// =========================================================================
// CORS
// =========================================================================

#[tokio::test]
async fn cors_headers_on_proxy_responses() {
    let base = start_proxy().await;
    let resp = reqwest::get(format!("{base}/proxy/ping")).await.unwrap();
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
}

#[tokio::test]
async fn cors_preflight_options() {
    let base = start_proxy().await;
    let client = reqwest::Client::new();
    let resp = client
        .request(reqwest::Method::OPTIONS, format!("{base}/proxy/query"))
        .header("Origin", "http://localhost:5173")
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "x-ch-host,x-ch-port,x-ch-user,x-ch-password,x-ch-database,x-ch-secure",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
    assert!(resp
        .headers()
        .get("access-control-max-age")
        .unwrap()
        .to_str()
        .unwrap()
        .parse::<u32>()
        .unwrap()
        > 0);
}

// =========================================================================
// Security: missing x-ch-host
// =========================================================================

#[tokio::test]
async fn missing_host_returns_error() {
    let base = start_proxy().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/proxy/query"))
        .header("x-ch-port", "8123")
        .header("x-ch-secure", "false")
        .body("SELECT 1")
        .send()
        .await
        .unwrap();
    assert!(!resp.status().is_success());
}

// =========================================================================
// Security: header leak verification (echo server)
// =========================================================================

#[tokio::test]
async fn password_only_in_clickhouse_key_header() {
    let base = start_proxy().await;
    let (echo_url, captured) = start_echo_server().await;
    let echo_port = extract_echo_port(&echo_url);

    let client = reqwest::Client::new();
    let mut req = client.post(format!("{base}/proxy/query")).body("SELECT 1");
    for (k, v) in echo_headers(echo_port) {
        req = req.header(k, v);
    }
    let _ = req.send().await.unwrap();

    let data = captured.lock().unwrap();
    assert_eq!(
        data.headers.get("x-clickhouse-key").unwrap(),
        "super-secret-123"
    );
    assert!(data.headers.get("x-ch-password").is_none());
}

#[tokio::test]
async fn no_x_ch_headers_forwarded_to_backend() {
    let base = start_proxy().await;
    let (echo_url, captured) = start_echo_server().await;
    let echo_port = extract_echo_port(&echo_url);

    let client = reqwest::Client::new();
    let mut req = client.post(format!("{base}/proxy/query")).body("SELECT 1");
    for (k, v) in echo_headers(echo_port) {
        req = req.header(k, v);
    }
    let _ = req.send().await.unwrap();

    let data = captured.lock().unwrap();
    let leaked: Vec<_> = data
        .headers
        .keys()
        .filter(|h| h.as_str().starts_with("x-ch-"))
        .collect();
    assert!(
        leaked.is_empty(),
        "x-ch-* headers leaked to backend: {:?}",
        leaked
    );
}

#[tokio::test]
async fn translates_credentials_to_clickhouse_headers() {
    let base = start_proxy().await;
    let (echo_url, captured) = start_echo_server().await;
    let echo_port = extract_echo_port(&echo_url);

    let client = reqwest::Client::new();
    let mut req = client.post(format!("{base}/proxy/query")).body("SELECT 1");
    for (k, v) in echo_headers(echo_port) {
        req = req.header(k, v);
    }
    let _ = req.send().await.unwrap();

    let data = captured.lock().unwrap();
    assert_eq!(data.headers.get("x-clickhouse-user").unwrap(), "alice");
    assert_eq!(
        data.headers.get("x-clickhouse-key").unwrap(),
        "super-secret-123"
    );
    assert_eq!(data.headers.get("x-clickhouse-database").unwrap(), "mydb");
}

#[tokio::test]
async fn password_not_in_forwarded_url() {
    let base = start_proxy().await;
    let (echo_url, captured) = start_echo_server().await;
    let echo_port = extract_echo_port(&echo_url);

    let client = reqwest::Client::new();
    let mut req = client
        .post(format!("{base}/proxy/query?format=JSONEachRow"))
        .body("SELECT 1");
    for (k, v) in echo_headers(echo_port) {
        req = req.header(k, v);
    }
    let _ = req.send().await.unwrap();

    let data = captured.lock().unwrap();
    assert!(
        !data.url.contains("super-secret-123"),
        "password leaked into URL: {}",
        data.url
    );
    assert!(
        !data.url.contains("password"),
        "password param in URL: {}",
        data.url
    );
    assert!(
        data.url.contains("default_format=JSONEachRow"),
        "format not rewritten: {}",
        data.url
    );
}

#[tokio::test]
async fn overwrites_injected_clickhouse_auth_headers() {
    let base = start_proxy().await;
    let (echo_url, captured) = start_echo_server().await;
    let echo_port = extract_echo_port(&echo_url);

    let client = reqwest::Client::new();
    let mut req = client.post(format!("{base}/proxy/query")).body("SELECT 1");
    for (k, v) in echo_headers(echo_port) {
        req = req.header(k, v);
    }
    // Inject headers a malicious client might send
    req = req
        .header("X-ClickHouse-User", "malicious-admin")
        .header("X-ClickHouse-Key", "hacked")
        .header("X-ClickHouse-Database", "system");
    let _ = req.send().await.unwrap();

    let data = captured.lock().unwrap();
    assert_eq!(data.headers.get("x-clickhouse-user").unwrap(), "alice");
    assert_eq!(
        data.headers.get("x-clickhouse-key").unwrap(),
        "super-secret-123"
    );
    assert_eq!(data.headers.get("x-clickhouse-database").unwrap(), "mydb");
}

// =========================================================================
// Query forwarding (requires ClickHouse — skipped if unavailable)
// =========================================================================

/// Get ClickHouse connection details from CH_TEST_URL or default to localhost:8123.
fn ch_connection() -> (String, u16, String, String) {
    if let Ok(url_str) = std::env::var("CH_TEST_URL") {
        let url = url::Url::parse(&url_str).expect("invalid CH_TEST_URL");
        let host = url.host_str().unwrap_or("localhost").to_string();
        let port = url.port().unwrap_or(8123);
        let user = if url.username().is_empty() {
            "default".into()
        } else {
            url.username().to_string()
        };
        let password = url.password().unwrap_or("").to_string();
        (host, port, user, password)
    } else {
        ("localhost".into(), 8123, "default".into(), String::new())
    }
}

fn ch_headers() -> Vec<(&'static str, String)> {
    let (host, port, user, password) = ch_connection();
    vec![
        ("x-ch-host", host),
        ("x-ch-port", port.to_string()),
        ("x-ch-user", user),
        ("x-ch-password", password),
        ("x-ch-database", "default".into()),
        ("x-ch-secure", "false".into()),
    ]
}

async fn ch_available() -> bool {
    let (host, port, _, _) = ch_connection();
    reqwest::get(format!("http://{host}:{port}/ping"))
        .await
        .map(|r| r.status().is_success())
        .unwrap_or(false)
}

async fn proxy_post(base: &str, path: &str, body: &str) -> (StatusCode, String) {
    let client = reqwest::Client::new();
    let mut req = client.post(format!("{base}{path}")).body(body.to_string());
    for (k, v) in ch_headers() {
        req = req.header(k, v);
    }
    let resp = req.send().await.unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    (StatusCode::from_u16(status.as_u16()).unwrap(), text)
}

#[tokio::test]
async fn forwards_query_with_format_in_body() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let (status, text) = proxy_post(&base, "/proxy/query", "SELECT 1 as n FORMAT JSONEachRow").await;
    assert_eq!(status, 200, "body: {text}");
    let row: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    assert_eq!(row["n"], 1);
}

#[tokio::test]
async fn forwards_format_query_param() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let (status, text) =
        proxy_post(&base, "/proxy/query?format=JSONEachRow", "SELECT 42 as answer").await;
    assert_eq!(status, 200, "body: {text}");
    let row: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    assert_eq!(row["answer"], 42);
}

#[tokio::test]
async fn forwards_version_query() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let (status, text) = proxy_post(
        &base,
        "/proxy/query?format=JSONEachRow",
        "SELECT version() as version, timezone() as timezone, hostName() as display_name",
    )
    .await;
    assert_eq!(status, 200, "body: {text}");
    let row: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    assert!(row["version"].is_string());
    assert!(row["timezone"].is_string());
    assert!(row["display_name"].is_string());
}

#[tokio::test]
async fn forwards_command() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let (status, _text) = proxy_post(&base, "/proxy/command", "SELECT 1").await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn returns_error_for_invalid_sql() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let (status, _text) = proxy_post(&base, "/proxy/query", "THIS IS NOT VALID SQL").await;
    assert!(!status.is_success());
}

#[tokio::test]
async fn returns_error_for_invalid_credentials() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/proxy/query"))
        .header("x-ch-host", "localhost")
        .header("x-ch-port", "8123")
        .header("x-ch-user", "nonexistent_user_12345")
        .header("x-ch-password", "wrong")
        .header("x-ch-database", "default")
        .header("x-ch-secure", "false")
        .body("SELECT 1")
        .send()
        .await
        .unwrap();
    assert!(!resp.status().is_success());
}

#[tokio::test]
async fn auth_translates_user() {
    if !ch_available().await {
        eprintln!("SKIP: ClickHouse not available");
        return;
    }
    let base = start_proxy().await;
    let (status, text) = proxy_post(
        &base,
        "/proxy/query",
        "SELECT currentUser() as user FORMAT JSONEachRow",
    )
    .await;
    assert_eq!(status, 200, "body: {text}");
    let row: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    let (_, _, expected_user, _) = ch_connection();
    assert_eq!(row["user"], expected_user);
}
